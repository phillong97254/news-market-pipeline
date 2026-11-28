package alert

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/phillong97254/news-market-pipeline/internal/gateway/clickhouse"
)

func healthy() clickhouse.Health {
	return clickhouse.Health{LastTick: now.Add(-10 * time.Second), LastNews: now.Add(-5 * time.Minute), LastEnriched: now.Add(-time.Minute), Backlog: 3}
}

func TestCheckHealth_RaiseRemindResolve(t *testing.T) {
	tg := &fakeTG{}
	ch := &fakeCH{health: healthy()}
	s, _ := newSvc(tg, ch, Config{MaxPerHour: 1})
	ctx := context.Background()

	if err := s.CheckHealth(ctx); err != nil || len(tg.sent) != 0 {
		t.Fatalf("healthy: err=%v sent=%q", err, tg.sent)
	}

	// Giá tắc 10 phút và nlp-worker kẹt: báo 2 sự cố, bỏ qua hạn mức MaxPerHour.
	ch.health.LastTick = now.Add(-10 * time.Minute)
	ch.health.LastEnriched = now.Add(-2 * time.Hour)
	if err := s.CheckHealth(ctx); err != nil {
		t.Fatal(err)
	}
	if len(tg.sent) != 2 || !strings.Contains(strings.Join(tg.sent, "\n"), "Không có giá mới trong 10m0s") ||
		!strings.Contains(strings.Join(tg.sent, "\n"), "còn 3 tin chờ") {
		t.Fatalf("sent = %q", tg.sent)
	}

	// Phút sau vẫn lỗi: không báo lại.
	if err := s.CheckHealth(ctx); err != nil || len(tg.sent) != 2 {
		t.Fatalf("repeat: sent = %d", len(tg.sent))
	}

	// 6 giờ sau vẫn lỗi: nhắc lại.
	s.now = func() time.Time { return now.Add(6 * time.Hour) }
	ch.health = healthy()
	ch.health.LastTick = now.Add(6*time.Hour - 10*time.Minute)
	ch.health.LastNews = now.Add(6 * time.Hour)
	ch.health.LastEnriched = now.Add(6 * time.Hour)
	if err := s.CheckHealth(ctx); err != nil {
		t.Fatal(err)
	}
	if len(tg.sent) != 4 || !strings.Contains(tg.sent[2]+tg.sent[3], "Sự cố vẫn còn") || !strings.Contains(tg.sent[2]+tg.sent[3], "Đã ổn lại</b>: gán nhãn tin") {
		t.Fatalf("sent = %q", tg.sent[2:])
	}
}

func TestCheckHealth_NewsQuietIsNotAnIssue(t *testing.T) {
	tg := &fakeTG{}
	ch := &fakeCH{health: healthy()}
	s, _ := newSvc(tg, ch, Config{})
	ctx := context.Background()

	// Đêm vắng tin 1 giờ: không báo.
	ch.health.LastNews = now.Add(-time.Hour)
	if err := s.CheckHealth(ctx); err != nil || len(tg.sent) != 0 {
		t.Fatalf("1h quiet: err=%v sent=%q", err, tg.sent)
	}
	// Quá 90 phút: báo.
	ch.health.LastNews = now.Add(-100 * time.Minute)
	if err := s.CheckHealth(ctx); err != nil || len(tg.sent) != 1 || !strings.Contains(tg.sent[0], "Không có tin mới trong 1h40m0s") {
		t.Fatalf("100m quiet: err=%v sent=%q", err, tg.sent)
	}
}

func TestCheckHealth_ClickHouseDown(t *testing.T) {
	tg := &fakeTG{}
	ch := &fakeCH{healthErr: errors.New("connection refused")}
	s, _ := newSvc(tg, ch, Config{})
	for k := 0; k < clickhouseFails; k++ {
		if err := s.CheckHealth(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if len(tg.sent) != 1 || !strings.Contains(tg.sent[0], "ClickHouse không trả lời 5 phút") {
		t.Fatalf("sent = %q", tg.sent)
	}
	ch.healthErr, ch.health = nil, healthy()
	if err := s.CheckHealth(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(tg.sent) != 2 || !strings.Contains(tg.sent[1], "Đã ổn lại</b>: clickhouse") {
		t.Fatalf("sent = %q", tg.sent)
	}
}

func TestMaybeSendDigest(t *testing.T) {
	ict := time.FixedZone("ICT", 7*3600)
	tg := &fakeTG{}
	ch := &fakeCH{
		ranges: []clickhouse.PriceRange{{Symbol: "BTC-USD", Open: 60000, High: 62000, Low: 59000, Close: 61200}},
		stats:  clickhouse.NewsStats{Total: 300, Relevant: 120, AvgSentiment: -0.15, ByCategory: []clickhouse.CategoryCount{{Category: "trade", Count: 50}}},
		news:   []clickhouse.NewsSummary{{Title: "Tariffs", Impact: 3, Category: "trade", URL: "https://x/1"}},
		social: []clickhouse.SocialTopic{{Topic: "tariff", Posts: 2500, Baseline: 1000}, {Topic: "fed", Posts: 0}},
	}
	s, st := newSvc(tg, ch, Config{Location: ict})
	ctx := context.Background()

	s.now = func() time.Time { return time.Date(2026, 11, 26, 20, 59, 0, 0, ict) }
	if err := s.MaybeSendDigest(ctx); err != nil || len(tg.sent) != 0 {
		t.Fatalf("before 21h: err=%v sent=%d", err, len(tg.sent))
	}
	s.now = func() time.Time { return time.Date(2026, 11, 26, 21, 0, 30, 0, ict) }
	for range 2 {
		if err := s.MaybeSendDigest(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if len(tg.sent) != 1 || !st.done["digest:2026-11-26"] {
		t.Fatalf("sent = %d, done = %v", len(tg.sent), st.done)
	}
	msg := tg.sent[0]
	for _, want := range []string{"đến 21:00 26/11", "BTC-USD 61,200.00 (+2.00%, thấp 59,000.00, cao 62,000.00)",
		"300 tin, 120 liên quan, sentiment TB -0.15", "trade 50", "[3] <a href=\"https://x/1\">Tariffs</a>", "tariff 2,500 (x2.5)"} {
		if !strings.Contains(msg, want) {
			t.Errorf("digest missing %q:\n%s", want, msg)
		}
	}
	if strings.Contains(msg, "fed 0") {
		t.Errorf("zero topics should be hidden:\n%s", msg)
	}

	// Khởi động lại (RAM trống) nhưng Redis nhớ: không gửi lại.
	s2 := New(s.logger, tg, ch, st, Config{Location: ict}).(*impl)
	s2.now = s.now
	if err := s2.MaybeSendDigest(ctx); err != nil || len(tg.sent) != 1 {
		t.Fatalf("after restart: sent = %d", len(tg.sent))
	}
}

func TestMaybeSendDigest_PartialFailure(t *testing.T) {
	tg := &fakeTG{}
	s, _ := newSvc(tg, &fakeCH{err: errors.New("down")}, Config{})
	s.now = func() time.Time { return time.Date(2026, 11, 26, 21, 5, 0, 0, time.UTC) }
	if err := s.MaybeSendDigest(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(tg.sent) != 1 || !strings.Contains(tg.sent[0], "Tóm tắt 24 giờ") {
		t.Fatalf("sent = %q", tg.sent)
	}
}
