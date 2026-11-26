package alert

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/phillong97254/news-market-pipeline/internal/domain"
	"github.com/phillong97254/news-market-pipeline/internal/gateway/clickhouse"
	"github.com/phillong97254/news-market-pipeline/internal/gateway/telegram"
)

func (i *impl) HandleNews(ctx context.Context, raw []byte) error {
	var n domain.EnrichedNews
	if err := json.Unmarshal(raw, &n); err != nil {
		return fmt.Errorf("%w: decode: %v", ErrPermanent, err)
	}
	if n.ID == "" {
		return fmt.Errorf("%w: missing id", ErrPermanent)
	}
	if !n.Relevant || n.Impact < i.cfg.MinImpact {
		return nil
	}
	now := i.now()
	if now.Sub(n.ProcessedAt) > i.cfg.MaxAge || (!n.PublishedAt.IsZero() && now.Sub(n.PublishedAt) > i.cfg.MaxAge) {
		i.logger.Debug("too old, skip", "id", n.ID)
		return nil
	}
	if done, err := i.sent.Done(ctx, n.ID); err != nil {
		i.logger.Warn("dedupe lookup failed, sending anyway", "id", n.ID, "err", err)
	} else if done {
		return nil
	}
	if !i.allowed() {
		i.logger.Warn("hourly alert limit reached, dropping news alert", "id", n.ID, "title", n.TitleEN)
		return nil
	}

	// Giá hiện tại chỉ là bối cảnh: ClickHouse lỗi thì vẫn gửi tin.
	prices, err := i.ch.PriceChanges(ctx, clickhouse.PriceChangesInput{Window: time.Hour})
	if err != nil {
		i.logger.Warn("price context unavailable", "err", err)
	}
	if err := i.tg.Send(ctx, telegram.SendInput{Text: formatNews(n, prices, i.cfg.Location)}); err != nil {
		if errors.Is(err, telegram.ErrRejected) {
			return fmt.Errorf("%w: %v", ErrPermanent, err)
		}
		return fmt.Errorf("send news alert: %w", err)
	}
	i.recordSent()
	if err := i.sent.MarkDone(ctx, n.ID); err != nil {
		i.logger.Warn("dedupe mark failed", "id", n.ID, "err", err)
	}
	i.logger.Info("news alert sent", "id", n.ID, "impact", n.Impact, "category", n.Category, "title", n.TitleEN)
	return nil
}
