// news-producer: lấy tin chính trị/kinh tế từ GDELT và RSS, bỏ tin trùng, đẩy vào topic news.raw.
//
// Cấu hình qua env:
//
//	KAFKA_BROKERS   mặc định localhost:19092 (trong docker: redpanda:9092)
//	NEWS_TOPIC      mặc định news.raw
//	GDELT_QUERY     truy vấn GDELT (mặc định: defaultGDELTQuery); để "off" thì tắt GDELT
//	GDELT_EVERY     mặc định 15m (GDELT cập nhật 15 phút/lần, giới hạn 1 request/5 giây)
//	RSS_FEEDS       "tên=url,tên=url" (mặc định: defaultFeeds)
//	RSS_EVERY       mặc định 10m
//	USER_AGENT      mặc định "news-market-pipeline/0.1 (personal research)"
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

	"github.com/phillong97254/news-market-pipeline/internal/domain"
	"github.com/phillong97254/news-market-pipeline/internal/gateway/gdelt"
	"github.com/phillong97254/news-market-pipeline/internal/gateway/rss"
	"github.com/phillong97254/news-market-pipeline/internal/publisher/kafka"
	"github.com/phillong97254/news-market-pipeline/internal/service/news"
)

const defaultGDELTQuery = `(tariff OR tariffs OR sanctions OR election OR "federal reserve" OR "interest rate" ` +
	`OR inflation OR "central bank" OR "trade war" OR "crypto regulation") sourcelang:english`

// Chỉ dùng tiêu đề, tóm tắt và link của feed công khai; không lưu nội dung bài báo.
const defaultFeeds = "bbc-world=https://feeds.bbci.co.uk/news/world/rss.xml," +
	"bbc-business=https://feeds.bbci.co.uk/news/business/rss.xml," +
	"coindesk=https://www.coindesk.com/arc/outboundfeeds/rss/," +
	"vnexpress-the-gioi=https://vnexpress.net/rss/the-gioi.rss," +
	"vnexpress-kinh-doanh=https://vnexpress.net/rss/kinh-doanh.rss"

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("app", "news-producer")
	slog.SetDefault(logger)

	brokers := splitList(env("KAFKA_BROKERS", "localhost:19092"))
	topic := env("NEWS_TOPIC", "news.raw")
	ua := env("USER_AGENT", "news-market-pipeline/0.1 (personal research)")

	var pollers []news.Poller

	if q := env("GDELT_QUERY", defaultGDELTQuery); q != "off" {
		g := gdelt.New(os.Getenv("GDELT_URL"), ua)
		// GDELT hay trả 429 cho IP của Oracle: thử lại sau 1, 2, 4, 8 phút, và mỗi lần lấy nhìn lại 3 giờ
		// để một lần thành công bù được các lần bị chặn trước đó (bài trùng bị bỏ theo ID).
		input := gdelt.SearchInput{Query: q, Timespan: 3 * time.Hour, Max: 250}
		pollers = append(pollers, news.Poller{
			Name:  "gdelt",
			Every: duration("GDELT_EVERY", 15*time.Minute),
			Retry: time.Minute,
			Fetch: func(ctx context.Context) ([]domain.NewsItem, error) { return g.SearchArticles(ctx, input) },
		})
	}

	r := rss.New(ua)
	rssEvery := duration("RSS_EVERY", 10*time.Minute)
	for _, f := range parseFeeds(env("RSS_FEEDS", defaultFeeds)) {
		pollers = append(pollers, news.Poller{
			Name:  "rss:" + f.Name,
			Every: rssEvery,
			Fetch: func(ctx context.Context) ([]domain.NewsItem, error) { return r.Fetch(ctx, f) },
		})
	}
	if len(pollers) == 0 {
		log.Fatal("no news source configured")
	}

	pub, err := kafka.New(logger, brokers)
	if err != nil {
		log.Fatal(err)
	}
	svc := news.New(logger, pub, topic, pollers)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	names := make([]string, 0, len(pollers))
	for _, p := range pollers {
		names = append(names, p.Name)
	}
	logger.Info("starting", "brokers", brokers, "topic", topic, "pollers", names)
	if err := svc.Run(ctx); err != nil {
		logger.Error("run", "err", err)
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pub.Close(closeCtx)
	logger.Info("stopped")
}

func parseFeeds(s string) []rss.Feed {
	var out []rss.Feed
	for _, part := range splitList(s) {
		name, u, ok := strings.Cut(part, "=")
		if !ok || name == "" || u == "" {
			log.Fatalf("bad RSS_FEEDS entry %q (want name=url)", part)
		}
		out = append(out, rss.Feed{Name: strings.TrimSpace(name), URL: strings.TrimSpace(u)})
	}
	return out
}

func duration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		log.Fatalf("bad %s=%q: %v", key, v, err)
	}
	return d
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
