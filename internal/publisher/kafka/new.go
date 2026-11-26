package kafka

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type impl struct {
	logger *slog.Logger
	client *kgo.Client
}

// New kết nối tới brokers. Producer mặc định của franz-go là idempotent và acks=all,
// nên gửi lại khi lỗi mạng không tạo bản ghi trùng.
func New(logger *slog.Logger, brokers []string) (Publisher, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ProducerLinger(50*time.Millisecond),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return &impl{logger: logger.With("publisher", "kafka"), client: client}, nil
}
