package ingest

import (
	"log/slog"
	"time"

	"github.com/phillong97254/news-market-pipeline/internal/gateway/coinbase"
	"github.com/phillong97254/news-market-pipeline/internal/publisher/kafka"
)

const (
	minBackoff = time.Second
	maxBackoff = 30 * time.Second
	// Kết nối sống lâu hơn mức này thì coi là ổn, lần đứt sau bắt đầu lại từ minBackoff.
	stableAfter = time.Minute
)

type impl struct {
	logger   *slog.Logger
	source   coinbase.Gateway
	pub      kafka.Publisher
	topic    string
	products []string
	sleep    func(time.Duration) <-chan time.Time // thay được trong test
}

func New(logger *slog.Logger, source coinbase.Gateway, pub kafka.Publisher, topic string, products []string) Service {
	return &impl{
		logger:   logger.With("service", "ingest"),
		source:   source,
		pub:      pub,
		topic:    topic,
		products: products,
		sleep:    time.After,
	}
}
