// Package alert gửi cảnh báo Telegram khi có tin ảnh hưởng cao hoặc giá biến động mạnh,
// kèm bối cảnh: giá hiện tại cho cảnh báo tin, các tin liên quan ngay trước đó cho cảnh báo giá.
// Chỉ báo trùng thời gian, không phải tín hiệu giao dịch.
package alert

import (
	"context"
	"errors"
	"time"
)

// ErrPermanent: bản ghi hỏng hoặc Telegram từ chối tin nhắn; gửi lại không giúp gì.
var ErrPermanent = errors.New("permanent alert error")

type Service interface {
	// HandleNews xử lý 1 bản ghi news.enriched. Trả lỗi khác ErrPermanent thì nên thử lại.
	HandleNews(ctx context.Context, raw []byte) error
	// CheckPrices so giá đầu và cuối cửa sổ, gửi cảnh báo nếu vượt ngưỡng. Gọi mỗi phút.
	CheckPrices(ctx context.Context) error
	// CheckHealth báo khi một tầng của pipeline ngừng có dữ liệu mới, và báo lại khi nó chạy lại. Gọi mỗi phút.
	CheckHealth(ctx context.Context) error
	// MaybeSendDigest gửi tóm tắt 24 giờ mỗi ngày một lần sau giờ DigestAt. Gọi mỗi phút.
	MaybeSendDigest(ctx context.Context) error
}

type Config struct {
	MinImpact     int           // tin có impact >= mức này mới báo (mặc định 3)
	MaxAge        time.Duration // bỏ tin xuất bản hoặc gán nhãn quá lâu (mặc định 6h), tránh báo lại tồn đọng
	PriceWindow   time.Duration // cửa sổ đo biến động giá (mặc định 15 phút)
	PriceMovePct  float64       // ngưỡng % tuyệt đối (mặc định 1.5)
	PriceCooldown time.Duration // không báo lại cùng cặp trong khoảng này (mặc định 1h)
	NewsLookback  time.Duration // cảnh báo giá kèm tin trong khoảng này trước đó (mặc định 1h)
	MaxPerHour    int           // tổng số tin nhắn tối đa mỗi giờ, chống spam (mặc định 10)
	Location      *time.Location
	DigestAt      time.Duration // giờ gửi tóm tắt tính từ 0h địa phương (mặc định 21h); âm = tắt
}

func (c Config) withDefaults() Config {
	if c.MinImpact <= 0 {
		c.MinImpact = 3
	}
	if c.MaxAge <= 0 {
		c.MaxAge = 6 * time.Hour
	}
	if c.PriceWindow <= 0 {
		c.PriceWindow = 15 * time.Minute
	}
	if c.PriceMovePct <= 0 {
		c.PriceMovePct = 1.5
	}
	if c.PriceCooldown <= 0 {
		c.PriceCooldown = time.Hour
	}
	if c.NewsLookback <= 0 {
		c.NewsLookback = time.Hour
	}
	if c.MaxPerHour <= 0 {
		c.MaxPerHour = 10
	}
	if c.DigestAt == 0 {
		c.DigestAt = 21 * time.Hour
	}
	if c.Location == nil {
		c.Location = time.UTC
	}
	return c
}
