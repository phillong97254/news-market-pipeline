package social

import (
	"context"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/phillong97254/news-market-pipeline/internal/domain"
	"github.com/phillong97254/news-market-pipeline/internal/gateway/jetstream"
)

func TestMatch(t *testing.T) {
	tests := []struct {
		text string
		want []string
	}{
		{"New TARIFFS on China, trade war heats up", []string{"tariff"}},
		{"Powell says rate cuts are coming; inflation sticky", []string{"fed", "inflation"}},
		{"ethics and bethlehem", nil},
		{"I sold my ETH and BTC", []string{"bitcoin", "ethereum"}},
		{"Giá vàng hôm nay tăng mạnh, lãi suất giữ nguyên", []string{"fed", "gold"}},
		{"S&P 500 at record", []string{"stocks"}},
		{"thuế quanh năm", nil}, // "thuế quan" phải là nguyên cụm
	}
	for _, tt := range tests {
		if got := match(DefaultTopics, tt.text); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("match(%q) = %v, want %v", tt.text, got, tt.want)
		}
	}
}

type fakePub struct {
	mu   sync.Mutex
	recs []domain.SocialMention
}

func (f *fakePub) Publish(_ context.Context, _, _ string, v any, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recs = append(f.recs, v.(domain.SocialMention))
	return nil
}
func (f *fakePub) PublishSync(ctx context.Context, topic, key string, v any, ts time.Time) error {
	return f.Publish(ctx, topic, key, v, ts)
}
func (f *fakePub) Close(context.Context) {}

type fakeSource struct {
	calls   []int64
	batches [][]jetstream.Post
}

func (f *fakeSource) Stream(ctx context.Context, in jetstream.StreamInput, on func(jetstream.Post) error) error {
	f.calls = append(f.calls, in.Cursor)
	if len(f.batches) == 0 {
		<-ctx.Done()
		return ctx.Err()
	}
	b := f.batches[0]
	f.batches = f.batches[1:]
	for _, p := range b {
		if err := on(p); err != nil {
			return err
		}
	}
	return io.ErrUnexpectedEOF
}

func us(min, sec int) int64 {
	return time.Date(2026, 11, 26, 8, min, sec, 0, time.UTC).UnixMicro()
}

func TestRun_CountsPerMinuteAndResumes(t *testing.T) {
	src := &fakeSource{batches: [][]jetstream.Post{
		{{TimeUS: us(0, 1), Text: "tariffs!"}, {TimeUS: us(0, 30), Text: "hello"}},
		{{TimeUS: us(0, 59), Text: "bitcoin and tariff"}, {TimeUS: us(1, 2), Text: "fomc today"}},
	}}
	pub := &fakePub{}
	svc := New(slog.New(slog.NewTextHandler(io.Discard, nil)), src, pub, "social.mentions", nil).(*impl)
	svc.sleep = func(time.Duration) <-chan time.Time { c := make(chan time.Time, 1); c <- time.Time{}; return c }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- svc.Run(ctx) }()
	for len(src.calls) < 3 {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	// Kết nối lại đọc tiếp sau bài cuối.
	if src.calls[0] != 0 || src.calls[1] != us(0, 30)+1 || src.calls[2] != us(1, 2)+1 {
		t.Fatalf("cursors = %v", src.calls)
	}
	got := map[string]int{}
	for _, r := range pub.recs {
		got[r.Minute.Format("15:04")+" "+r.Topic] = r.Posts
	}
	want := map[string]int{
		"08:00 _all": 3, "08:00 tariff": 2, "08:00 bitcoin": 1, "08:00 fed": 0,
		"08:01 _all": 1, "08:01 fed": 1, "08:01 tariff": 0, // phút cuối ghi khi dừng
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %d, want %d (all: %v)", k, got[k], v, got)
		}
	}
	if len(pub.recs) != 2*(len(DefaultTopics)+1) {
		t.Fatalf("records = %d", len(pub.recs))
	}
}
