package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// key quyết định partition: cùng key thì cùng partition, giữ đúng thứ tự.
func (i *impl) Publish(ctx context.Context, topic, key string, value any, ts time.Time) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode value: %w", err)
	}
	rec := &kgo.Record{Topic: topic, Key: []byte(key), Value: raw, Timestamp: ts}
	i.client.Produce(ctx, rec, func(r *kgo.Record, err error) {
		if err != nil {
			i.logger.Error("produce failed", "topic", r.Topic, "key", string(r.Key), "err", err)
		}
	})
	return nil
}

func (i *impl) Close(ctx context.Context) {
	if err := i.client.Flush(ctx); err != nil {
		i.logger.Warn("flush on close", "err", err)
	}
	i.client.Close()
}
