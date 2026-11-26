package social

import (
	"context"
	"fmt"
	"time"

	"github.com/phillong97254/news-market-pipeline/internal/domain"
	"github.com/phillong97254/news-market-pipeline/internal/gateway/jetstream"
)

func (i *impl) Run(ctx context.Context) error {
	backoff := minBackoff
	for {
		start := time.Now()
		var n int64
		// Đọc tiếp ngay sau bài cuối: không đếm trùng; có thể hụt vài bài cùng micro giây (không đáng kể).
		cursor := i.cursor
		if cursor > 0 {
			cursor++
		}
		err := i.source.Stream(ctx, jetstream.StreamInput{Cursor: cursor}, func(p jetstream.Post) error {
			n++
			return i.handle(ctx, p)
		})
		if ctx.Err() != nil {
			i.flush(context.Background()) // ghi nốt phút đang đếm
			return nil
		}
		if time.Since(start) >= stableAfter {
			backoff = minBackoff
		}
		i.logger.Warn("stream ended, reconnecting", "posts", n, "uptime", time.Since(start).Round(time.Second),
			"retry_in", backoff, "err", err)
		select {
		case <-ctx.Done():
			i.flush(context.Background())
			return nil
		case <-i.sleep(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// handle cộng bài vào phút của nó; sang phút mới thì ghi phút cũ. Bài đến trễ (phút cũ hơn) tính vào phút hiện tại.
func (i *impl) handle(ctx context.Context, p jetstream.Post) error {
	m := p.TimeUS / int64(time.Minute/time.Microsecond)
	if i.minute == 0 {
		i.minute = m
	}
	if m > i.minute {
		if err := i.flush(ctx); err != nil {
			return err
		}
		i.minute = m
	}
	i.all++
	for _, t := range match(i.topics, p.Text) {
		i.counts[t]++
	}
	i.cursor = max(i.cursor, p.TimeUS)
	return nil
}

// flush ghi 1 bản ghi cho mỗi chủ đề (kể cả 0, để biểu đồ liền mạch) và 1 bản ghi _all.
func (i *impl) flush(ctx context.Context) error {
	if i.minute == 0 || i.all == 0 {
		return nil
	}
	minute := time.Unix(i.minute*60, 0).UTC()
	now := i.now().UTC()
	publish := func(topic string, posts int) error {
		rec := domain.SocialMention{Source: "bluesky", Topic: topic, Minute: minute, Posts: posts, IngestTime: now}
		if err := i.pub.Publish(ctx, i.topic, topic, rec, minute); err != nil {
			return fmt.Errorf("publish %s: %w", topic, err)
		}
		return nil
	}
	if err := publish(domain.SocialAllTopic, i.all); err != nil {
		return err
	}
	matched := 0
	for _, t := range i.topics {
		matched += i.counts[t.Name]
		if err := publish(t.Name, i.counts[t.Name]); err != nil {
			return err
		}
	}
	if minute.Minute()%10 == 0 {
		i.logger.Info("minute counted", "minute", minute.Format("15:04"), "posts", i.all, "topic_hits", matched)
	}
	i.all = 0
	clear(i.counts)
	return nil
}
