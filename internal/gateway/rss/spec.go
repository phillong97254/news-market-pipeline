// Package rss đọc feed RSS 2.0 và Atom. Chỉ lấy tiêu đề, tóm tắt ngắn và link.
// Dùng conditional GET (ETag / Last-Modified): feed chưa đổi thì server trả 304, không tải lại.
package rss

import (
	"context"

	"github.com/phillong97254/news-market-pipeline/internal/domain"
)

type Gateway interface {
	// Fetch trả các bài trong feed; feed chưa đổi từ lần trước thì trả danh sách rỗng.
	Fetch(ctx context.Context, feed Feed) ([]domain.NewsItem, error)
}

type Feed struct {
	Name string // ví dụ "bbc-world", ghi vào NewsItem.Feed
	URL  string
}
