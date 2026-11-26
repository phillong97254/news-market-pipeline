// Package clickhouse đọc dữ liệu đã tổng hợp từ ClickHouse qua HTTP interface (database market, giai đoạn 4).
// Dùng user chỉ đọc; query có tham số ({name:Type}) nên không ghép chuỗi SQL.
package clickhouse

import (
	"context"
	"time"
)

type Gateway interface {
	// PriceChanges: giá đóng cửa của mỗi cặp ở đầu và cuối cửa sổ Window, tính trên các nến 1 phút đã đóng.
	PriceChanges(ctx context.Context, input PriceChangesInput) ([]PriceChange, error)
	// RecentNews: tin liên quan đã gán nhãn trong [From, To), impact cao trước, mới trước.
	RecentNews(ctx context.Context, input RecentNewsInput) ([]NewsSummary, error)
	// PipelineHealth: thời điểm dữ liệu mới nhất của từng tầng và số tin đang chờ gán nhãn.
	PipelineHealth(ctx context.Context) (Health, error)
	// PriceSummary: open/high/low/close của mỗi cặp trong [From, To).
	PriceSummary(ctx context.Context, input RangeInput) ([]PriceRange, error)
	// NewsStats: số tin, số tin liên quan, sentiment TB và số tin theo loại trong [From, To) (theo published_at).
	NewsStats(ctx context.Context, input RangeInput) (NewsStats, error)
	// SocialTopics: tổng số bài Bluesky theo chủ đề trong [From, To) và trung bình cùng độ dài của 7 ngày trước đó.
	SocialTopics(ctx context.Context, input RangeInput) ([]SocialTopic, error)
}

type RangeInput struct {
	From time.Time
	To   time.Time
}

type Health struct {
	LastTick     time.Time
	LastNews     time.Time // ingest_time mới nhất của news.raw
	LastEnriched time.Time // processed_at mới nhất của news.enriched
	Backlog      int       // tin 24h qua có trong news mà chưa có trong news_enriched
}

type PriceRange struct {
	Symbol string
	Open   float64
	High   float64
	Low    float64
	Close  float64
}

func (p PriceRange) Pct() float64 {
	if p.Open == 0 {
		return 0
	}
	return (p.Close/p.Open - 1) * 100
}

type NewsStats struct {
	Total        int
	Relevant     int
	AvgSentiment float64
	ByCategory   []CategoryCount // nhiều trước
}

type CategoryCount struct {
	Category string
	Count    int
}

type SocialTopic struct {
	Topic    string
	Posts    int
	Baseline float64 // trung bình mỗi khoảng cùng độ dài trong 7 ngày trước; 0 nếu chưa có dữ liệu
}

type PriceChangesInput struct {
	Window time.Duration // làm tròn xuống phút, tối thiểu 1 phút
}

type PriceChange struct {
	Symbol    string
	FromPrice float64
	ToPrice   float64
	FromTime  time.Time
	ToTime    time.Time
}

// Pct là phần trăm thay đổi (1.5 nghĩa là +1,5%).
func (p PriceChange) Pct() float64 {
	if p.FromPrice == 0 {
		return 0
	}
	return (p.ToPrice/p.FromPrice - 1) * 100
}

type RecentNewsInput struct {
	From      time.Time
	To        time.Time
	MinImpact int
	Limit     int
}

type NewsSummary struct {
	ID          string
	PublishedAt time.Time
	Title       string
	Category    string
	Sentiment   float64
	Impact      int
	Feed        string
	URL         string
}
