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
