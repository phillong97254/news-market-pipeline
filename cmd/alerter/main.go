// alerter: gửi cảnh báo Telegram (giai đoạn 5).
//   - Tin: đọc news.enriched, báo tin liên quan có impact >= ALERT_MIN_IMPACT, kèm giá BTC/ETH hiện tại.
//   - Giá: mỗi phút hỏi ClickHouse, báo khi giá đổi >= PRICE_MOVE_PCT trong PRICE_WINDOW, kèm tin ngay trước đó.
//   - Sức khoẻ: mỗi phút kiểm tra giá, tin, gán nhãn và ClickHouse còn có dữ liệu mới; báo khi tắc và khi chạy lại.
//   - Tóm tắt: mỗi ngày một lần sau DIGEST_AT (giờ địa phương).
//
// Chạy với network_mode: host (Kafka localhost:19092, ClickHouse 127.0.0.1:8123, Redis 127.0.0.1:6379).
// Cấu hình qua env:
//
//	TG_TOKEN, TG_CHAT_ID   bắt buộc; lấy từ env_file của bot Vietlott, không ghi trong repo
//	KAFKA_BROKERS          mặc định localhost:19092
//	IN_TOPIC               mặc định news.enriched
//	GROUP                  mặc định alerter
//	CLICKHOUSE_URL         mặc định http://127.0.0.1:8123
//	CLICKHOUSE_USER        mặc định grafana (user chỉ đọc), mật khẩu ở CH_GRAFANA_PASSWORD
//	REDIS_ADDR, REDIS_PASSWORD  nhớ tin đã báo (rỗng = tắt)
//	ALERT_MIN_IMPACT       mặc định 3
//	ALERT_MAX_AGE          mặc định 6h
//	ALERT_MAX_PER_HOUR     mặc định 10
//	PRICE_WINDOW           mặc định 15m
//	PRICE_MOVE_PCT         mặc định 1.5
//	PRICE_COOLDOWN         mặc định 1h
//	ALERT_UTC_OFFSET       giờ hiển thị, mặc định 7 (giờ Việt Nam)
//	DIGEST_AT              giờ gửi tóm tắt, mặc định 21:00; off = tắt
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/phillong97254/news-market-pipeline/internal/cache/processed"
	consumer "github.com/phillong97254/news-market-pipeline/internal/consumer/kafka"
	"github.com/phillong97254/news-market-pipeline/internal/gateway/clickhouse"
	"github.com/phillong97254/news-market-pipeline/internal/gateway/telegram"
	"github.com/phillong97254/news-market-pipeline/internal/service/alert"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("app", "alerter")
	slog.SetDefault(logger)

	token, chatID := os.Getenv("TG_TOKEN"), os.Getenv("TG_CHAT_ID")
	if token == "" || chatID == "" {
		log.Fatal("TG_TOKEN and TG_CHAT_ID are required")
	}
	brokers := splitList(env("KAFKA_BROKERS", "localhost:19092"))
	in := env("IN_TOPIC", "news.enriched")
	group := env("GROUP", "alerter")
	offset := envInt("ALERT_UTC_OFFSET", 7)
	cfg := alert.Config{
		MinImpact:     envInt("ALERT_MIN_IMPACT", 3),
		MaxAge:        envDuration("ALERT_MAX_AGE", 6*time.Hour),
		MaxPerHour:    envInt("ALERT_MAX_PER_HOUR", 10),
		PriceWindow:   envDuration("PRICE_WINDOW", 15*time.Minute),
		PriceMovePct:  envFloat("PRICE_MOVE_PCT", 1.5),
		PriceCooldown: envDuration("PRICE_COOLDOWN", time.Hour),
		NewsLookback:  time.Hour,
		Location:      time.FixedZone(fmt.Sprintf("UTC%+d", offset), offset*3600),
		DigestAt:      digestAt(env("DIGEST_AT", "21:00")),
	}

	cons, err := consumer.New(logger, brokers, group, in)
	if err != nil {
		log.Fatal(err)
	}
	sent := processed.New(os.Getenv("REDIS_ADDR"), os.Getenv("REDIS_PASSWORD"), "alert:news:", 7*24*time.Hour)
	ch := clickhouse.New(os.Getenv("CLICKHOUSE_URL"), env("CLICKHOUSE_USER", "grafana"), os.Getenv("CH_GRAFANA_PASSWORD"))
	svc := alert.New(logger, telegram.New(os.Getenv("TELEGRAM_API_URL"), token, chatID), ch, sent, cfg)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("starting", "brokers", brokers, "in", in, "group", group, "min_impact", cfg.MinImpact,
		"price_window", cfg.PriceWindow, "price_move_pct", cfg.PriceMovePct, "dedupe", os.Getenv("REDIS_ADDR") != "")

	newsDone := make(chan struct{})
	go func() {
		defer close(newsDone)
		err := cons.Run(ctx, func(ctx context.Context, r consumer.Record) error {
			if err := svc.HandleNews(ctx, r.Value); err != nil {
				if errors.Is(err, alert.ErrPermanent) {
					return fmt.Errorf("%w: %v", consumer.ErrSkip, err)
				}
				return err
			}
			return nil
		})
		if err != nil {
			logger.Error("news consumer", "err", err)
		}
	}()

	// Một lần kiểm tra lỗi không làm dừng vòng lặp.
	tick := time.NewTicker(time.Minute)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			<-newsDone
			cons.Close()
			logger.Info("stopped")
			return
		case <-tick.C:
			if err := svc.CheckPrices(ctx); err != nil && ctx.Err() == nil {
				logger.Warn("check prices", "err", err)
			}
			if err := svc.CheckHealth(ctx); err != nil && ctx.Err() == nil {
				logger.Warn("check health", "err", err)
			}
			if err := svc.MaybeSendDigest(ctx); err != nil && ctx.Err() == nil {
				logger.Warn("digest", "err", err)
			}
		}
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v, err := strconv.ParseFloat(os.Getenv(key), 64); err == nil {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

// digestAt đổi "HH:MM" thành khoảng cách từ 0h; "off" hoặc sai định dạng thì tắt (-1).
func digestAt(s string) time.Duration {
	t, err := time.Parse("15:04", s)
	if err != nil {
		if s != "off" {
			slog.Warn("bad DIGEST_AT, digest disabled", "value", s)
		}
		return -1
	}
	return time.Duration(t.Hour())*time.Hour + time.Duration(t.Minute())*time.Minute
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
