// nlp-worker: đọc news.raw, nhờ Ollama gán nhãn (chủ đề, sắc thái, mức ảnh hưởng, tài sản), ghi vào news.enriched.
//
// Chạy với network_mode: host để gọi Ollama ở 127.0.0.1 của Oracle (Ollama không mở ra ngoài).
// Cấu hình qua env:
//
//	KAFKA_BROKERS   mặc định localhost:19092
//	IN_TOPIC        mặc định news.raw
//	OUT_TOPIC       mặc định news.enriched
//	GROUP           mặc định nlp-worker
//	OLLAMA_URL      mặc định http://127.0.0.1:11434
//	OLLAMA_MODEL    mặc định qwen2.5:7b
//	REDIS_ADDR      Redis nhớ tin đã xử lý (rỗng = tắt), ví dụ 127.0.0.1:6379
//	REDIS_PASSWORD  mật khẩu Redis (lấy từ env_file của Redis dùng chung, không ghi trong repo)
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/phillong97254/news-market-pipeline/internal/cache/processed"
	consumer "github.com/phillong97254/news-market-pipeline/internal/consumer/kafka"
	"github.com/phillong97254/news-market-pipeline/internal/gateway/ollama"
	"github.com/phillong97254/news-market-pipeline/internal/publisher/kafka"
	"github.com/phillong97254/news-market-pipeline/internal/service/enrich"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("app", "nlp-worker")
	slog.SetDefault(logger)

	brokers := splitList(env("KAFKA_BROKERS", "localhost:19092"))
	in := env("IN_TOPIC", "news.raw")
	out := env("OUT_TOPIC", "news.enriched")
	group := env("GROUP", "nlp-worker")
	model := env("OLLAMA_MODEL", "qwen2.5:7b")

	pub, err := kafka.New(logger, brokers)
	if err != nil {
		log.Fatal(err)
	}
	cons, err := consumer.New(logger, brokers, group, in)
	if err != nil {
		log.Fatal(err)
	}
	// Key giữ 30 ngày: lâu hơn thời gian news.raw giữ dữ liệu.
	done := processed.New(os.Getenv("REDIS_ADDR"), os.Getenv("REDIS_PASSWORD"), "nlp:done:", 30*24*time.Hour)
	svc := enrich.New(logger, ollama.New(os.Getenv("OLLAMA_URL"), model), pub, out, done)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("starting", "brokers", brokers, "in", in, "out", out, "group", group, "model", model,
		"dedupe", os.Getenv("REDIS_ADDR") != "")
	err = cons.Run(ctx, func(ctx context.Context, r consumer.Record) error {
		if err := svc.Handle(ctx, r.Value); err != nil {
			if errors.Is(err, enrich.ErrPermanent) {
				return fmt.Errorf("%w: %v", consumer.ErrSkip, err)
			}
			return err
		}
		return nil
	})
	if err != nil {
		logger.Error("run", "err", err)
	}

	cons.Close()
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

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
