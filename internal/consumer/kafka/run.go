package kafka

import (
	"context"
	"errors"

	"github.com/twmb/franz-go/pkg/kgo"
)

func (i *impl) Run(ctx context.Context, h Handler) error {
	for {
		fetches := i.client.PollRecords(ctx, 1)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			i.client.AllowRebalance()
			return nil
		}
		fetches.EachError(func(topic string, p int32, err error) {
			i.logger.Warn("fetch error", "topic", topic, "partition", p, "err", err)
		})
		for _, rec := range fetches.Records() {
			if err := i.handle(ctx, rec, h); err != nil {
				i.client.AllowRebalance()
				return nil // ctx bị huỷ khi đang chờ thử lại; bản ghi chưa commit, lần sau xử lý lại
			}
			if err := i.client.CommitRecords(ctx, rec); err != nil && ctx.Err() == nil {
				i.logger.Warn("commit failed", "partition", rec.Partition, "offset", rec.Offset, "err", err)
			}
		}
		i.client.AllowRebalance()
	}
}

// handle chỉ trả lỗi khi ctx bị huỷ.
func (i *impl) handle(ctx context.Context, rec *kgo.Record, h Handler) error {
	r := Record{
		Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset,
		Key: rec.Key, Value: rec.Value, Timestamp: rec.Timestamp,
	}
	wait := minRetry
	for {
		err := h(ctx, r)
		if err == nil {
			return nil
		}
		if errors.Is(err, ErrSkip) {
			i.logger.Warn("skip record", "partition", r.Partition, "offset", r.Offset, "err", err)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		i.logger.Warn("handle failed, retrying", "partition", r.Partition, "offset", r.Offset, "retry_in", wait, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-i.sleep(wait):
		}
		wait = min(wait*2, maxRetry)
	}
}
