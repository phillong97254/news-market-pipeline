package news

import (
	"context"
	"sync"
	"time"
)

func (i *impl) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for _, p := range i.pollers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			i.loop(ctx, p)
		}()
	}
	wg.Wait()
	return nil
}

// loop lấy ngay lần đầu rồi lặp theo p.Every.
func (i *impl) loop(ctx context.Context, p Poller) {
	ticker := time.NewTicker(p.Every)
	defer ticker.Stop()
	for {
		i.pollOnce(ctx, p)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// pollOnce trả số bài mới đã gửi.
func (i *impl) pollOnce(ctx context.Context, p Poller) int {
	items, err := p.Fetch(ctx)
	if err != nil {
		if ctx.Err() == nil {
			i.logger.Warn("fetch failed", "poller", p.Name, "err", err)
		}
		return 0
	}

	now := i.now().UTC()
	i.seen.prune(now.Add(-seenTTL))
	sent, old, dup := 0, 0, 0
	for _, it := range items {
		switch {
		case now.Sub(it.PublishedAt) > maxAge:
			old++
		case !i.seen.add(it.ID, now): // giữ chỗ trước khi gửi để 2 poller không cùng gửi 1 bài
			dup++
		default:
			// Timestamp bản ghi = lúc nhận, để retention của topic tính theo lúc vào pipeline.
			if err := i.pub.Publish(ctx, i.topic, it.ID, it, it.IngestTime); err != nil {
				i.seen.remove(it.ID) // lần lấy sau thử lại
				i.logger.Error("publish failed", "poller", p.Name, "id", it.ID, "err", err)
				continue
			}
			sent++
		}
	}
	i.logger.Info("poll", "poller", p.Name, "fetched", len(items), "new", sent, "dup", dup, "too_old", old,
		"seen_size", i.seen.len())
	return sent
}
