// Package enrich đọc một bản tin thô, nhờ LLM gán nhãn rồi gửi bản đã gán nhãn vào news.enriched.
package enrich

import (
	"context"
	"errors"
)

// ErrPermanent: bản ghi không xử lý được dù thử lại (JSON hỏng, model trả sai nhiều lần). Nên bỏ qua.
var ErrPermanent = errors.New("permanent enrich error")

type Service interface {
	// Handle xử lý 1 bản tin (JSON của domain.NewsItem). Lỗi khác ErrPermanent là lỗi tạm thời, nên thử lại.
	Handle(ctx context.Context, raw []byte) error
}
