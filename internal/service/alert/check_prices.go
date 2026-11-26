package alert

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/phillong97254/news-market-pipeline/internal/gateway/clickhouse"
	"github.com/phillong97254/news-market-pipeline/internal/gateway/telegram"
)

// Nến cuối cũ hơn mức này nghĩa là dữ liệu giá đang tắc: không so sánh để tránh báo sai.
const maxCandleAge = 5 * time.Minute

func (i *impl) CheckPrices(ctx context.Context) error {
	changes, err := i.ch.PriceChanges(ctx, clickhouse.PriceChangesInput{Window: i.cfg.PriceWindow})
	if err != nil {
		return fmt.Errorf("price changes: %w", err)
	}
	now := i.now()
	var errs []error
	for _, c := range changes {
		if math.Abs(c.Pct()) < i.cfg.PriceMovePct || now.Sub(c.ToTime) > maxCandleAge {
			continue
		}
		if c.ToTime.Sub(c.FromTime) < i.cfg.PriceWindow/2 {
			continue // thiếu nến (pipeline vừa khởi động lại): so 2 điểm quá gần nhau
		}
		i.mu.Lock()
		cooling := now.Sub(i.lastPrice[c.Symbol]) < i.cfg.PriceCooldown
		i.mu.Unlock()
		if cooling {
			continue
		}
		if !i.allowed() {
			i.logger.Warn("hourly alert limit reached, dropping price alert", "symbol", c.Symbol, "pct", c.Pct())
			continue
		}

		news, err := i.ch.RecentNews(ctx, clickhouse.RecentNewsInput{
			From: c.ToTime.Add(-i.cfg.NewsLookback), To: c.ToTime.Add(time.Minute), MinImpact: 1, Limit: 3,
		})
		if err != nil {
			i.logger.Warn("news context unavailable", "symbol", c.Symbol, "err", err)
		}
		if err := i.tg.Send(ctx, telegram.SendInput{Text: formatPrice(c, i.cfg.PriceWindow, i.cfg.NewsLookback, news, i.cfg.Location)}); err != nil {
			errs = append(errs, fmt.Errorf("send price alert %s: %w", c.Symbol, err))
			if errors.Is(err, telegram.ErrRejected) {
				i.markPrice(c.Symbol, now) // tin nhắn sai thì đừng thử lại mỗi phút
			}
			continue
		}
		i.recordSent()
		i.markPrice(c.Symbol, now)
		i.logger.Info("price alert sent", "symbol", c.Symbol, "pct", fmt.Sprintf("%.2f", c.Pct()), "news", len(news))
	}
	return errors.Join(errs...)
}

func (i *impl) markPrice(symbol string, t time.Time) {
	i.mu.Lock()
	i.lastPrice[symbol] = t
	i.mu.Unlock()
}
