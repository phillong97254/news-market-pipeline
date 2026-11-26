package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func (i *impl) PublishSync(ctx context.Context, topic, key string, value any, ts time.Time) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode value: %w", err)
	}
	rec := &kgo.Record{Topic: topic, Key: []byte(key), Value: raw, Timestamp: ts}
	if err := i.client.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("produce %s: %w", topic, err)
	}
	return nil
}
