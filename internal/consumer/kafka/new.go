package kafka

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	minRetry = 2 * time.Second
	maxRetry = time.Minute
)

type impl struct {
	logger *slog.Logger
	client *kgo.Client
	sleep  func(time.Duration) <-chan time.Time
}

// New: group mới bắt đầu đọc từ đầu topic. BlockRebalanceOnPoll để offset không bị đổi chủ khi đang xử lý dở.
func New(logger *slog.Logger, brokers []string, group, topic string) (Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	return &impl{
		logger: logger.With("consumer", group, "topic", topic),
		client: client,
		sleep:  time.After,
	}, nil
}

func (i *impl) Close() { i.client.Close() }
