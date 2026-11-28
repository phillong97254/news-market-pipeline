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

// loop lấy ngay lần đầu rồi lặp theo p.Every; lấy lỗi thì thử lại sớm hơn theo nextWait.
func (i *impl) loop(ctx context.Context, p Poller) {
	timer := time.NewTimer(p.Every)
	defer timer.Stop()
	fails := 0
	for {
		if _, ok := i.pollOnce(ctx, p); ok {
			fails = 0
		} else {
			fails++
		}
		timer.Reset(nextWait(p, fails))
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}
}

// nextWait: sau fails lần lỗi liên tiếp, chờ Retry * 2^(fails-1) nhưng không quá Every.
func nextWait(p Poller, fails int) time.Duration {
	if fails == 0 || p.Retry <= 0 {
		return p.Every
	}
	wait := p.Retry
	for n := 1; n < fails && wait < p.Every; n++ {
		wait *= 2
	}
	return min(wait, p.Every)
}

// pollOnce trả số bài mới đã gửi và việc lấy có thành công không.
func (i *impl) pollOnce(ctx context.Context, p Poller) (int, bool) {
	items, err := p.Fetch(ctx)
	if err != nil {
		if ctx.Err() == nil {
			i.logger.Warn("fetch failed", "poller", p.Name, "err", err, "retry_in", nextWait(p, 1))
		}
		return 0, false
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
	return sent, true
}
