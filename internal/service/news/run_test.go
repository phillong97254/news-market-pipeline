package news

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/phillong97254/news-market-pipeline/internal/domain"
)

type fakePub struct {
	mu   sync.Mutex
	keys []string
	fail map[string]bool
}

func (f *fakePub) Publish(_ context.Context, topic, key string, _ any, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if topic != "news.raw" {
		return errors.New("wrong topic")
	}
	if f.fail[key] {
		return errors.New("encode failed")
	}
	f.keys = append(f.keys, key)
	return nil
}
func (f *fakePub) PublishSync(ctx context.Context, topic, key string, v any, ts time.Time) error {
	return f.Publish(ctx, topic, key, v, ts)
}
func (f *fakePub) Close(context.Context) {}

var now = time.Date(2026, 11, 26, 6, 0, 0, 0, time.UTC)

func item(id string, age time.Duration) domain.NewsItem {
	return domain.NewsItem{ID: id, Title: id, PublishedAt: now.Add(-age), IngestTime: now}
}

func newTestService(pub *fakePub) *impl {
	return &impl{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)), pub: pub, topic: "news.raw",
		seen: newSeenSet(), now: func() time.Time { return now },
	}
}

func fixed(items ...domain.NewsItem) Poller {
	return Poller{Name: "p", Every: time.Hour, Fetch: func(context.Context) ([]domain.NewsItem, error) { return items, nil }}
}

func TestPollOnce(t *testing.T) {
	t.Run("gửi bài mới, bỏ bài quá cũ", func(t *testing.T) {
		pub := &fakePub{}
		svc := newTestService(pub)
		if n, _ := svc.pollOnce(context.Background(), fixed(item("a", time.Hour), item("old", 72*time.Hour))); n != 1 {
			t.Fatalf("sent = %d, want 1", n)
		}
		if len(pub.keys) != 1 || pub.keys[0] != "a" {
			t.Fatalf("keys = %v", pub.keys)
		}
	})

	t.Run("không gửi lại bài đã gửi, kể cả từ poller khác", func(t *testing.T) {
		pub := &fakePub{}
		svc := newTestService(pub)
		svc.pollOnce(context.Background(), fixed(item("a", 0), item("b", 0)))
		if n, _ := svc.pollOnce(context.Background(), fixed(item("b", 0), item("c", 0))); n != 1 {
			t.Fatalf("second poll sent %d, want 1", n)
		}
		if want := []string{"a", "b", "c"}; len(pub.keys) != 3 || pub.keys[2] != want[2] {
			t.Fatalf("keys = %v, want %v", pub.keys, want)
		}
	})

	t.Run("trùng ID trong cùng một lần lấy chỉ gửi 1 lần", func(t *testing.T) {
		pub := &fakePub{}
		svc := newTestService(pub)
		svc.pollOnce(context.Background(), fixed(item("a", 0), item("a", 0)))
		if len(pub.keys) != 1 {
			t.Fatalf("keys = %v", pub.keys)
		}
	})

	t.Run("lỗi fetch không gửi gì", func(t *testing.T) {
		pub := &fakePub{}
		svc := newTestService(pub)
		p := Poller{Name: "x", Fetch: func(context.Context) ([]domain.NewsItem, error) { return nil, errors.New("503") }}
		if n, _ := svc.pollOnce(context.Background(), p); n != 0 || len(pub.keys) != 0 {
			t.Fatalf("sent %d", n)
		}
	})

	t.Run("gửi lỗi thì chưa đánh dấu đã gửi, lần sau thử lại", func(t *testing.T) {
		pub := &fakePub{fail: map[string]bool{"a": true}}
		svc := newTestService(pub)
		svc.pollOnce(context.Background(), fixed(item("a", 0)))
		pub.fail = nil
		if n, _ := svc.pollOnce(context.Background(), fixed(item("a", 0))); n != 1 {
			t.Fatalf("retry sent %d, want 1", n)
		}
	})
}

func TestNextWait(t *testing.T) {
	p := Poller{Every: 15 * time.Minute, Retry: time.Minute}
	for fails, want := range []time.Duration{15 * time.Minute, time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 15 * time.Minute, 15 * time.Minute} {
		if got := nextWait(p, fails); got != want {
			t.Errorf("fails=%d: got %v, want %v", fails, got, want)
		}
	}
	if got := nextWait(Poller{Every: time.Hour}, 3); got != time.Hour {
		t.Errorf("no retry: got %v", got)
	}
	if got := nextWait(p, 1000); got != 15*time.Minute {
		t.Errorf("many fails: got %v", got)
	}
}

func TestRun_RetriesAfterFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := make(chan struct{}, 10)
	n := 0
	p := Poller{Name: "p", Every: time.Hour, Retry: 10 * time.Millisecond, Fetch: func(context.Context) ([]domain.NewsItem, error) {
		n++
		calls <- struct{}{}
		if n == 1 {
			return nil, errors.New("429")
		}
		return nil, nil
	}}
	svc := New(slog.New(slog.NewTextHandler(io.Discard, nil)), &fakePub{}, "news.raw", []Poller{p})
	go func() { _ = svc.Run(ctx) }()
	for k := 0; k < 2; k++ {
		select {
		case <-calls:
		case <-time.After(2 * time.Second):
			t.Fatalf("call %d did not happen: failed poll was not retried early", k+1)
		}
	}
}

func TestSeenSet_Prune(t *testing.T) {
	s := newSeenSet()
	s.add("old", now.Add(-100*time.Hour))
	s.add("new", now)
	if n := s.prune(now.Add(-seenTTL)); n != 1 || s.has("old") || !s.has("new") {
		t.Fatalf("pruned %d, has old=%v new=%v", n, s.has("old"), s.has("new"))
	}
}

func TestRun_StopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := make(chan struct{}, 10)
	p := Poller{Name: "p", Every: 10 * time.Millisecond, Fetch: func(context.Context) ([]domain.NewsItem, error) {
		calls <- struct{}{}
		return nil, nil
	}}
	svc := New(slog.New(slog.NewTextHandler(io.Discard, nil)), &fakePub{}, "news.raw", []Poller{p, p})

	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	<-calls
	<-calls // cả 2 poller đã chạy lần đầu ngay khi khởi động
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
}
