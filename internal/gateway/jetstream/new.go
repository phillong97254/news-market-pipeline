package jetstream

import "time"

const DefaultURL = "wss://jetstream2.us-east.bsky.network/subscribe"

type impl struct {
	url         string
	readTimeout time.Duration
}

// New: luồng có hàng chục bài mỗi giây, 60s không có gì nghĩa là kết nối đã chết.
func New(url string) Gateway {
	if url == "" {
		url = DefaultURL
	}
	return &impl{url: url, readTimeout: 60 * time.Second}
}
