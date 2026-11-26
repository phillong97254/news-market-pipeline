package ingest

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

type fakeSource struct {
	mu    sync.Mutex
	calls int
	// mỗi lần Stream: gửi các symbol trong batches[calls] rồi trả err
	batches [][]string
	cancel  context.CancelFunc // huỷ ctx sau lần gọi cuối
}

func (f *fakeSource) Stream(ctx context.Context, _ []string, onTick func(domain.Tick) error) error {
	f.mu.Lock()
	idx := f.calls
	f.calls++
	f.mu.Unlock()
	if idx >= len(f.batches) {
		f.cancel()
		<-ctx.Done()
		return ctx.Err()
	}
	for _, s := range f.batches[idx] {
		if err := onTick(domain.Tick{Symbol: s}); err != nil {
			return err
		}
	}
	return errors.New("connection dropped")
}

type fakePub struct {
	mu  sync.Mutex
	got []string
}

func (f *fakePub) Publish(_ context.Context, topic, key string, _ any, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if topic != "prices.tick" {
		return errors.New("wrong topic " + topic)
	}
	f.got = append(f.got, key)
	return nil
}
func (f *fakePub) PublishSync(ctx context.Context, topic, key string, v any, ts time.Time) error {
	return f.Publish(ctx, topic, key, v, ts)
}
func (f *fakePub) Close(context.Context) {}

func TestRun_ReconnectsWithBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	src := &fakeSource{batches: [][]string{{"BTC-USD"}, {}, {"ETH-USD", "BTC-USD"}}, cancel: cancel}
	pub := &fakePub{}
	var waits []time.Duration
	svc := &impl{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)), source: src, pub: pub, topic: "prices.tick",
		products: []string{"BTC-USD", "ETH-USD"},
		sleep: func(d time.Duration) <-chan time.Time {
			waits = append(waits, d)
			ch := make(chan time.Time, 1)
			ch <- time.Time{}
			return ch
		},
	}

	if err := svc.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if want := []string{"BTC-USD", "ETH-USD", "BTC-USD"}; !equal(pub.got, want) {
		t.Fatalf("published %v, want %v", pub.got, want)
	}
	if want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}; !equalD(waits, want) {
		t.Fatalf("backoff %v, want %v", waits, want)
	}
}

func TestRun_StopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	src := &fakeSource{cancel: cancel}
	svc := New(slog.New(slog.NewTextHandler(io.Discard, nil)), src, &fakePub{}, "prices.tick", nil)

	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalD(a, b []time.Duration) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
