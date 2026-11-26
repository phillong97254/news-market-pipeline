// Package gdelt tìm bài báo qua GDELT DOC 2.0 API (miễn phí, không cần key).
// Giới hạn của GDELT: tối đa 1 request mỗi 5 giây; timespan phải lớn hơn 15 phút.
package gdelt

import (
	"context"
	"errors"
	"time"

	"github.com/phillong97254/news-market-pipeline/internal/domain"
)

var (
	ErrRateLimited = errors.New("gdelt rate limited")
	ErrBadResponse = errors.New("gdelt returned a non-JSON response")
)

type Gateway interface {
	SearchArticles(ctx context.Context, input SearchInput) ([]domain.NewsItem, error)
}

type SearchInput struct {
	Query    string        // cú pháp GDELT, ví dụ `(tariff OR sanctions) sourcelang:english`
	Timespan time.Duration // cửa sổ nhìn lại, làm tròn xuống phút
	Max      int           // tối đa 250
}
