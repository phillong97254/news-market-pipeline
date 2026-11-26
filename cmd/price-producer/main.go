// price-producer: đọc giá coin thời gian thực từ Coinbase rồi đẩy vào topic prices.tick trên Redpanda.
//
// Cấu hình qua env:
//
//	KAFKA_BROKERS   mặc định localhost:19092 (trong docker: redpanda:9092)
//	PRICE_TOPIC     mặc định prices.tick
//	PRODUCTS        mặc định BTC-USD,ETH-USD
//	COINBASE_WS_URL mặc định wss://ws-feed.exchange.coinbase.com
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

	"github.com/phillong97254/news-market-pipeline/internal/gateway/coinbase"
	"github.com/phillong97254/news-market-pipeline/internal/publisher/kafka"
	"github.com/phillong97254/news-market-pipeline/internal/service/ingest"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("app", "price-producer")
	slog.SetDefault(logger)

	brokers := splitList(env("KAFKA_BROKERS", "localhost:19092"))
	topic := env("PRICE_TOPIC", "prices.tick")
	products := splitList(env("PRODUCTS", "BTC-USD,ETH-USD"))

	pub, err := kafka.New(logger, brokers)
	if err != nil {
		log.Fatal(err)
	}
	source := coinbase.New(os.Getenv("COINBASE_WS_URL"), 30*time.Second)
	svc := ingest.New(logger, source, pub, topic, products)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("starting", "brokers", brokers, "topic", topic, "products", products)
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

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
