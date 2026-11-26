package enrich

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/phillong97254/news-market-pipeline/internal/domain"
	"github.com/phillong97254/news-market-pipeline/internal/gateway/ollama"
)

type fakeLLM struct {
	errs     []error // lỗi trả theo từng lần gọi; hết danh sách thì thành công
	calls    int
	attempts []int
}

func (f *fakeLLM) Classify(_ context.Context, in ollama.ClassifyInput) (domain.NewsAnalysis, error) {
	f.calls++
	f.attempts = append(f.attempts, in.Attempt)
	if f.calls <= len(f.errs) {
		return domain.NewsAnalysis{}, f.errs[f.calls-1]
	}
	return domain.NewsAnalysis{Category: "trade", Relevant: true, Impact: 2, Assets: []string{"USD"}, ProcessedAt: time.Now()}, nil
}

type fakePub struct {
	err  error
	got  []domain.EnrichedNews
	keys []string
}

func (f *fakePub) Publish(context.Context, string, string, any, time.Time) error { return nil }
func (f *fakePub) PublishSync(_ context.Context, topic, key string, v any, _ time.Time) error {
	if f.err != nil {
		return f.err
	}
	if topic != "news.enriched" {
		return errors.New("wrong topic")
	}
	f.keys = append(f.keys, key)
	f.got = append(f.got, v.(domain.EnrichedNews))
	return nil
}
func (f *fakePub) Close(context.Context) {}

type fakeDone struct {
	ids     map[string]bool
	readErr error
	markErr error
}

func (f *fakeDone) Done(_ context.Context, id string) (bool, error) {
	if f.readErr != nil {
		return false, f.readErr
	}
	return f.ids[id], nil
}

func (f *fakeDone) MarkDone(_ context.Context, id string) error {
	if f.markErr != nil {
		return f.markErr
	}
	if f.ids == nil {
		f.ids = map[string]bool{}
	}
	f.ids[id] = true
	return nil
}

func rawItem(t *testing.T) []byte {
	b, err := json.Marshal(domain.NewsItem{ID: "abc", Title: "US tariffs", Feed: "bbc-world"})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestHandle(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("gán nhãn và gửi theo id", func(t *testing.T) {
		pub := &fakePub{}
		if err := New(logger, &fakeLLM{}, pub, "news.enriched", &fakeDone{}).Handle(context.Background(), rawItem(t)); err != nil {
			t.Fatal(err)
		}
		if len(pub.got) != 1 || pub.keys[0] != "abc" || pub.got[0].Title != "US tariffs" || pub.got[0].Category != "trade" {
			t.Fatalf("published %+v", pub.got)
		}
	})

	t.Run("model trả sai 2 lần rồi đúng", func(t *testing.T) {
		llm := &fakeLLM{errs: []error{ollama.ErrBadOutput, ollama.ErrBadOutput}}
		pub := &fakePub{}
		if err := New(logger, llm, pub, "news.enriched", &fakeDone{}).Handle(context.Background(), rawItem(t)); err != nil || llm.calls != 3 {
			t.Fatalf("err=%v calls=%d", err, llm.calls)
		}
		if llm.attempts[0] != 0 || llm.attempts[2] != 2 {
			t.Fatalf("attempts = %v, want 0,1,2", llm.attempts)
		}
	})

	t.Run("model sai quá 3 lần thì lỗi vĩnh viễn", func(t *testing.T) {
		llm := &fakeLLM{errs: []error{ollama.ErrBadOutput, ollama.ErrBadOutput, ollama.ErrBadOutput}}
		err := New(logger, llm, &fakePub{}, "news.enriched", &fakeDone{}).Handle(context.Background(), rawItem(t))
		if !errors.Is(err, ErrPermanent) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("Ollama chết là lỗi tạm thời, không hỏi lại ngay", func(t *testing.T) {
		llm := &fakeLLM{errs: []error{ollama.ErrUnavailable}}
		err := New(logger, llm, &fakePub{}, "news.enriched", &fakeDone{}).Handle(context.Background(), rawItem(t))
		if err == nil || errors.Is(err, ErrPermanent) || llm.calls != 1 {
			t.Fatalf("err=%v calls=%d", err, llm.calls)
		}
	})

	t.Run("tin đã xử lý thì bỏ qua, không gọi LLM", func(t *testing.T) {
		llm, pub, done := &fakeLLM{}, &fakePub{}, &fakeDone{}
		svc := New(logger, llm, pub, "news.enriched", done)
		for range 2 {
			if err := svc.Handle(context.Background(), rawItem(t)); err != nil {
				t.Fatal(err)
			}
		}
		if llm.calls != 1 || len(pub.got) != 1 || !done.ids["abc"] {
			t.Fatalf("calls=%d published=%d done=%v", llm.calls, len(pub.got), done.ids)
		}
	})

	t.Run("gửi lỗi thì chưa đánh dấu đã xử lý", func(t *testing.T) {
		done := &fakeDone{}
		_ = New(logger, &fakeLLM{}, &fakePub{err: errors.New("broker down")}, "news.enriched", done).Handle(context.Background(), rawItem(t))
		if done.ids["abc"] {
			t.Fatal("marked done although publish failed")
		}
	})

	t.Run("Redis lỗi thì vẫn xử lý bình thường", func(t *testing.T) {
		pub := &fakePub{}
		done := &fakeDone{readErr: errors.New("redis down"), markErr: errors.New("redis down")}
		if err := New(logger, &fakeLLM{}, pub, "news.enriched", done).Handle(context.Background(), rawItem(t)); err != nil || len(pub.got) != 1 {
			t.Fatalf("err=%v published=%d", err, len(pub.got))
		}
	})

	t.Run("JSON hỏng hoặc thiếu id là lỗi vĩnh viễn", func(t *testing.T) {
		svc := New(logger, &fakeLLM{}, &fakePub{}, "news.enriched", &fakeDone{})
		for _, raw := range []string{"not json", `{"title":"x"}`} {
			if err := svc.Handle(context.Background(), []byte(raw)); !errors.Is(err, ErrPermanent) {
				t.Fatalf("%s: err = %v", raw, err)
			}
		}
	})

	t.Run("gửi lỗi là lỗi tạm thời", func(t *testing.T) {
		err := New(logger, &fakeLLM{}, &fakePub{err: errors.New("broker down")}, "news.enriched", &fakeDone{}).Handle(context.Background(), rawItem(t))
		if err == nil || errors.Is(err, ErrPermanent) {
			t.Fatalf("err = %v", err)
		}
	})
}
