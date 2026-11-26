package coinbase

import "time"

const DefaultURL = "wss://ws-feed.exchange.coinbase.com"

type impl struct {
	url         string
	readTimeout time.Duration
	now         func() time.Time
}

// New tạo gateway. readTimeout: không nhận được tin nào trong khoảng này thì coi như kết nối chết
// (kênh heartbeat gửi mỗi giây nên 30s là dư).
func New(url string, readTimeout time.Duration) Gateway {
	if url == "" {
		url = DefaultURL
	}
	return &impl{url: url, readTimeout: readTimeout, now: time.Now}
}
