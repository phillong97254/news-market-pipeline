package kafka

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func testConsumer(waits *[]time.Duration) *impl {
	return &impl{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		sleep: func(d time.Duration) <-chan time.Time {
			*waits = append(*waits, d)
			ch := make(chan time.Time, 1)
			ch <- time.Time{}
			return ch
		},
	}
}

func TestHandle(t *testing.T) {
	rec := &kgo.Record{Topic: "news.raw", Value: []byte("v")}

	t.Run("thử lại tới khi thành công, backoff tăng dần", func(t *testing.T) {
		var waits []time.Duration
		calls := 0
		err := testConsumer(&waits).handle(context.Background(), rec, func(context.Context, Record) error {
			calls++
			if calls < 4 {
				return errors.New("ollama down")
			}
			return nil
		})
		if err != nil || calls != 4 {
			t.Fatalf("err=%v calls=%d", err, calls)
		}
		if want := []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second}; len(waits) != 3 || waits[2] != want[2] {
			t.Fatalf("waits = %v", waits)
		}
	})

	t.Run("ErrSkip thì bỏ qua, không thử lại", func(t *testing.T) {
		var waits []time.Duration
		calls := 0
		err := testConsumer(&waits).handle(context.Background(), rec, func(context.Context, Record) error {
			calls++
			return errors.Join(ErrSkip, errors.New("bad json"))
		})
		if err != nil || calls != 1 || len(waits) != 0 {
			t.Fatalf("err=%v calls=%d waits=%v", err, calls, waits)
		}
	})

	t.Run("huỷ ctx khi đang thử lại thì trả lỗi để không commit", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var waits []time.Duration
		err := testConsumer(&waits).handle(ctx, rec, func(context.Context, Record) error {
			cancel()
			return errors.New("ollama down")
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("giữ nguyên dữ liệu bản ghi", func(t *testing.T) {
		var waits []time.Duration
		_ = testConsumer(&waits).handle(context.Background(), &kgo.Record{Topic: "t", Partition: 2, Offset: 9, Value: []byte("x")},
			func(_ context.Context, r Record) error {
				if r.Partition != 2 || r.Offset != 9 || string(r.Value) != "x" {
					t.Errorf("record = %+v", r)
				}
				return nil
			})
	})
}
