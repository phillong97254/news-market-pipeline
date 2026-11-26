package ingest

import (
	"context"
	"time"

	"github.com/phillong97254/news-market-pipeline/internal/domain"
)

func (i *impl) Run(ctx context.Context) error {
	backoff := minBackoff
	for {
		start := time.Now()
		var n int64
		err := i.source.Stream(ctx, i.products, func(t domain.Tick) error {
			n++
			return i.pub.Publish(ctx, i.topic, t.Symbol, t, t.EventTime)
		})
		if ctx.Err() != nil {
			return nil
		}
		if time.Since(start) >= stableAfter {
			backoff = minBackoff
		}
		i.logger.Warn("stream ended, reconnecting", "ticks", n, "uptime", time.Since(start).Round(time.Second),
			"retry_in", backoff, "err", err)

		select {
		case <-ctx.Done():
			return nil
		case <-i.sleep(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}
