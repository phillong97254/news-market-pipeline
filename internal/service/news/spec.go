// Package news lấy tin định kỳ từ nhiều nguồn, bỏ tin trùng (theo ID của URL) và đẩy vào topic news.raw.
package news

import (
	"context"
	"time"

	"github.com/phillong97254/news-market-pipeline/internal/domain"
)

type Service interface {
	// Run chạy mọi poller song song tới khi ctx bị huỷ. Một lần lấy lỗi chỉ được log, không dừng vòng lặp.
	Run(ctx context.Context) error
}

// Poller là một nguồn tin và chu kỳ lấy. main tạo poller từ gateway GDELT/RSS.
type Poller struct {
	Name  string
	Every time.Duration
	Fetch func(ctx context.Context) ([]domain.NewsItem, error)
}
