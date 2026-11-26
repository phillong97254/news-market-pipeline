// social-producer: đọc bài đăng công khai trên Bluesky (Jetstream), đếm số bài nhắc tới từng chủ đề mỗi phút,
// ghi vào Redpanda topic social.mentions. Chỉ ghi số đếm, không lưu nội dung hay tác giả bài.
//
// Cấu hình qua env:
//
//	KAFKA_BROKERS   mặc định redpanda:9092
//	SOCIAL_TOPIC    mặc định social.mentions
//	JETSTREAM_URL   mặc định wss://jetstream2.us-east.bsky.network/subscribe
package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/phillong97254/news-market-pipeline/internal/gateway/jetstream"
	"github.com/phillong97254/news-market-pipeline/internal/publisher/kafka"
	"github.com/phillong97254/news-market-pipeline/internal/service/social"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("app", "social-producer")
	slog.SetDefault(logger)

	brokers := strings.Split(env("KAFKA_BROKERS", "redpanda:9092"), ",")
	topic := env("SOCIAL_TOPIC", "social.mentions")

	pub, err := kafka.New(logger, brokers)
	if err != nil {
		log.Fatal(err)
	}
	svc := social.New(logger, jetstream.New(os.Getenv("JETSTREAM_URL")), pub, topic, nil)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("starting", "brokers", brokers, "topic", topic, "topics", len(social.DefaultTopics))
	if err := svc.Run(ctx); err != nil {
		logger.Error("run", "err", err)
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pub.Close(closeCtx)
	logger.Info("stopped")
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
