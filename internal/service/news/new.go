package news

import (
	"log/slog"
	"time"

	"github.com/phillong97254/news-market-pipeline/internal/publisher/kafka"
)

const (
	// Bỏ qua bài cũ hơn mức này (feed thường giữ bài vài ngày; lần chạy đầu không cần đẩy hết).
	maxAge = 48 * time.Hour
	// Nhớ ID đã gửi lâu hơn maxAge một chút để không gửi lại bài vẫn còn trong feed.
	seenTTL = maxAge + 24*time.Hour
)

type impl struct {
	logger  *slog.Logger
	pub     kafka.Publisher
	topic   string
	pollers []Poller
	seen    *seenSet
	now     func() time.Time
}

func New(logger *slog.Logger, pub kafka.Publisher, topic string, pollers []Poller) Service {
	return &impl{
		logger:  logger.With("service", "news"),
		pub:     pub,
		topic:   topic,
		pollers: pollers,
		seen:    newSeenSet(),
		now:     time.Now,
	}
}
