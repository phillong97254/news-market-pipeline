package social

import (
	"log/slog"
	"time"

	"github.com/phillong97254/news-market-pipeline/internal/gateway/jetstream"
	"github.com/phillong97254/news-market-pipeline/internal/publisher/kafka"
)

const (
	minBackoff  = time.Second
	maxBackoff  = time.Minute
	stableAfter = time.Minute
)

type impl struct {
	logger *slog.Logger
	source jetstream.Gateway
	pub    kafka.Publisher
	topic  string
	topics []Topic
	now    func() time.Time
	sleep  func(time.Duration) <-chan time.Time

	cursor int64 // time_us của bài cuối đã đếm
	minute int64 // phút (Unix) đang đếm; 0 = chưa có
	counts map[string]int
	all    int
}

func New(logger *slog.Logger, source jetstream.Gateway, pub kafka.Publisher, topic string, topics []Topic) Service {
	if len(topics) == 0 {
		topics = DefaultTopics
	}
	return &impl{
		logger: logger.With("service", "social"),
		source: source,
		pub:    pub,
		topic:  topic,
		topics: topics,
		now:    time.Now,
		sleep:  time.After,
		counts: map[string]int{},
	}
}
