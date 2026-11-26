package alert

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/phillong97254/news-market-pipeline/internal/domain"
	"github.com/phillong97254/news-market-pipeline/internal/gateway/clickhouse"
	"github.com/phillong97254/news-market-pipeline/internal/gateway/telegram"
)

var now = time.Date(2026, 11, 26, 8, 0, 30, 0, time.UTC)

type fakeTG struct {
	sent []string
	err  error
}

func (f *fakeTG) Send(_ context.Context, in telegram.SendInput) error {
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, in.Text)
	return nil
}

type fakeCH struct {
	changes []clickhouse.PriceChange
	news    []clickhouse.NewsSummary
	err     error
	newsIn  clickhouse.RecentNewsInput
}

func (f *fakeCH) PriceChanges(context.Context, clickhouse.PriceChangesInput) ([]clickhouse.PriceChange, error) {
	return f.changes, f.err
}

func (f *fakeCH) RecentNews(_ context.Context, in clickhouse.RecentNewsInput) ([]clickhouse.NewsSummary, error) {
	f.newsIn = in
	return f.news, f.err
}

type fakeStore struct{ done map[string]bool }

func (s *fakeStore) Done(_ context.Context, id string) (bool, error) { return s.done[id], nil }
func (s *fakeStore) MarkDone(_ context.Context, id string) error     { s.done[id] = true; return nil }

func newSvc(tg *fakeTG, ch *fakeCH, cfg Config) (*impl, *fakeStore) {
	st := &fakeStore{done: map[string]bool{}}
	s := New(slog.New(slog.NewTextHandler(io.Discard, nil)), tg, ch, st, cfg).(*impl)
	s.now = func() time.Time { return now }
	return s, st
}

func newsJSON(t *testing.T, mut func(n *domain.EnrichedNews)) []byte {
	t.Helper()
	n := domain.EnrichedNews{
		NewsItem: domain.NewsItem{ID: "n1", Feed: "bbc-world", URL: "https://x/a?b=1&c=2", Title: "Thuế <mới>",
			PublishedAt: now.Add(-20 * time.Minute)},
		NewsAnalysis: domain.NewsAnalysis{TitleEN: "New tariffs <50%>", Category: "trade", Relevant: true,
			Sentiment: -0.6, Impact: 3, Assets: []string{"US_STOCKS", "USD"}, ProcessedAt: now.Add(-time.Minute)},
	}
	if mut != nil {
		mut(&n)
	}
	b, err := json.Marshal(n)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestHandleNews(t *testing.T) {
	tests := []struct {
		name     string
		mut      func(n *domain.EnrichedNews)
		tgErr    error
		chErr    error
		wantSent bool
		wantErr  error
		wantDone bool
	}{
		{name: "impact 3 thì gửi", wantSent: true, wantDone: true},
		{name: "impact 2 bỏ qua", mut: func(n *domain.EnrichedNews) { n.Impact = 2 }},
		{name: "không liên quan bỏ qua", mut: func(n *domain.EnrichedNews) { n.Relevant = false }},
		{name: "tin cũ bỏ qua", mut: func(n *domain.EnrichedNews) { n.PublishedAt = now.Add(-7 * time.Hour) }},
		{name: "ClickHouse lỗi vẫn gửi", chErr: errors.New("down"), wantSent: true, wantDone: true},
		{name: "Telegram tạm lỗi thì thử lại", tgErr: telegram.ErrUnavailable, wantErr: telegram.ErrUnavailable},
		{name: "Telegram từ chối thì bỏ", tgErr: telegram.ErrRejected, wantErr: ErrPermanent},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tg := &fakeTG{err: tt.tgErr}
			ch := &fakeCH{err: tt.chErr, changes: []clickhouse.PriceChange{{Symbol: "BTC-USD", FromPrice: 60000, ToPrice: 60600}}}
			s, st := newSvc(tg, ch, Config{})
			err := s.HandleNews(context.Background(), newsJSON(t, tt.mut))
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if (len(tg.sent) == 1) != tt.wantSent || st.done["n1"] != tt.wantDone {
				t.Fatalf("sent = %d, done = %v", len(tg.sent), st.done["n1"])
			}
		})
	}
}

func TestHandleNews_DedupeAndMessage(t *testing.T) {
	tg := &fakeTG{}
	s, _ := newSvc(tg, &fakeCH{changes: []clickhouse.PriceChange{{Symbol: "BTC-USD", FromPrice: 60000, ToPrice: 60600}}},
		Config{Location: time.FixedZone("ICT", 7*3600)})
	raw := newsJSON(t, nil)
	for range 2 {
		if err := s.HandleNews(context.Background(), raw); err != nil {
			t.Fatal(err)
		}
	}
	if len(tg.sent) != 1 {
		t.Fatalf("sent %d times, want 1", len(tg.sent))
	}
	msg := tg.sent[0]
	for _, want := range []string{"🔴", "New tariffs &lt;50%&gt;", "<i>Thuế &lt;mới&gt;</i>", "BTC-USD 60,600.00 (1h +1.00%)",
		"14:40 26/11", `href="https://x/a?b=1&amp;c=2"`} {
		if !strings.Contains(msg, want) {
			t.Errorf("message missing %q:\n%s", want, msg)
		}
	}
}

func TestHandleNews_BadJSON(t *testing.T) {
	s, _ := newSvc(&fakeTG{}, &fakeCH{}, Config{})
	if err := s.HandleNews(context.Background(), []byte("{")); !errors.Is(err, ErrPermanent) {
		t.Fatalf("err = %v", err)
	}
}

func TestHourlyLimit(t *testing.T) {
	tg := &fakeTG{}
	s, _ := newSvc(tg, &fakeCH{}, Config{MaxPerHour: 2})
	for _, id := range []string{"a", "b", "c"} {
		if err := s.HandleNews(context.Background(), newsJSON(t, func(n *domain.EnrichedNews) { n.ID = id })); err != nil {
			t.Fatal(err)
		}
	}
	if len(tg.sent) != 2 {
		t.Fatalf("sent = %d, want 2", len(tg.sent))
	}
}

func change(sym string, from, to float64, age time.Duration) clickhouse.PriceChange {
	end := now.Truncate(time.Minute).Add(-time.Minute - age)
	return clickhouse.PriceChange{Symbol: sym, FromPrice: from, ToPrice: to, FromTime: end.Add(-15 * time.Minute), ToTime: end}
}

func TestCheckPrices(t *testing.T) {
	tg := &fakeTG{}
	ch := &fakeCH{
		changes: []clickhouse.PriceChange{
			change("BTC-USD", 60000, 61200, 0),         // +2%: báo
			change("ETH-USD", 1700, 1710, 0),           // +0.6%: không
			change("SOL-USD", 100, 90, 10*time.Minute), // -10% nhưng dữ liệu cũ: không
		},
		news: []clickhouse.NewsSummary{{Title: "Fed & rates", Impact: 3, Category: "monetary_policy", URL: "https://x/1", PublishedAt: now}},
	}
	s, _ := newSvc(tg, ch, Config{})
	if err := s.CheckPrices(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(tg.sent) != 1 || !strings.Contains(tg.sent[0], "BTC-USD +2.00% trong 15 phút") ||
		!strings.Contains(tg.sent[0], "Fed &amp; rates") || !strings.Contains(tg.sent[0], "60,000.00 → 61,200.00") {
		t.Fatalf("sent = %q", tg.sent)
	}
	if ch.newsIn.MinImpact != 1 || !ch.newsIn.From.Equal(ch.changes[0].ToTime.Add(-time.Hour)) {
		t.Fatalf("news query = %+v", ch.newsIn)
	}

	// Lần sau trong thời gian cooldown: không báo lại.
	if err := s.CheckPrices(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(tg.sent) != 1 {
		t.Fatalf("cooldown ignored: sent = %d", len(tg.sent))
	}
}

func TestCheckPrices_NoNews(t *testing.T) {
	tg := &fakeTG{}
	s, _ := newSvc(tg, &fakeCH{changes: []clickhouse.PriceChange{change("BTC-USD", 60000, 58800, 0)}}, Config{})
	if err := s.CheckPrices(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(tg.sent) != 1 || !strings.Contains(tg.sent[0], "📉") || !strings.Contains(tg.sent[0], "Không có tin liên quan") {
		t.Fatalf("sent = %q", tg.sent)
	}
}

func TestFormatPrice2(t *testing.T) {
	for in, want := range map[float64]string{0: "0.00", 999.5: "999.50", 1234.567: "1,234.57", 60123.4: "60,123.40", 1234567: "1,234,567.00"} {
		if got := formatPrice2(in); got != want {
			t.Errorf("formatPrice2(%v) = %q, want %q", in, got, want)
		}
	}
}
