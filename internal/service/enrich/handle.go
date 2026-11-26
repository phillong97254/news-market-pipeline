package enrich

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/phillong97254/news-market-pipeline/internal/domain"
	"github.com/phillong97254/news-market-pipeline/internal/gateway/ollama"
)

func (i *impl) Handle(ctx context.Context, raw []byte) error {
	var item domain.NewsItem
	if err := json.Unmarshal(raw, &item); err != nil {
		return fmt.Errorf("%w: decode news item: %v", ErrPermanent, err)
	}
	if item.ID == "" || item.Title == "" {
		return fmt.Errorf("%w: news item without id or title", ErrPermanent)
	}

	// Lỗi Redis chỉ log rồi xử lý tiếp: tệ nhất là hỏi LLM lại một tin đã có.
	done, err := i.done.Done(ctx, item.ID)
	if err != nil {
		i.logger.Warn("read processed store failed", "id", item.ID, "err", err)
	}
	if done {
		i.logger.Info("skip already enriched", "id", item.ID, "feed", item.Feed)
		return nil
	}

	analysis, err := i.classify(ctx, item)
	if err != nil {
		return err
	}
	out := domain.EnrichedNews{NewsItem: item, NewsAnalysis: analysis}
	// Gửi đồng bộ: chỉ khi broker xác nhận thì consumer mới commit bản tin gốc.
	if err := i.pub.PublishSync(ctx, i.topic, item.ID, out, analysis.ProcessedAt); err != nil {
		return fmt.Errorf("publish enriched: %w", err)
	}
	if err := i.done.MarkDone(ctx, item.ID); err != nil {
		i.logger.Warn("mark processed failed", "id", item.ID, "err", err)
	}
	i.logger.Info("enriched", "id", item.ID, "feed", item.Feed, "category", analysis.Category,
		"relevant", analysis.Relevant, "impact", analysis.Impact, "sentiment", analysis.Sentiment,
		"assets", analysis.Assets, "latency_ms", analysis.LatencyMS)
	return nil
}

func (i *impl) classify(ctx context.Context, item domain.NewsItem) (domain.NewsAnalysis, error) {
	var lastErr error
	for attempt := 0; attempt < maxBadOutput; attempt++ {
		a, err := i.llm.Classify(ctx, ollama.ClassifyInput{Title: item.Title, Summary: item.Summary, Attempt: attempt})
		if err == nil {
			return a, nil
		}
		if !errors.Is(err, ollama.ErrBadOutput) {
			return domain.NewsAnalysis{}, fmt.Errorf("classify: %w", err) // Ollama chết, sai model...: thử lại sau
		}
		lastErr = err
		i.logger.Warn("bad model output", "id", item.ID, "attempt", attempt, "err", err)
	}
	return domain.NewsAnalysis{}, fmt.Errorf("%w: %v", ErrPermanent, lastErr)
}
