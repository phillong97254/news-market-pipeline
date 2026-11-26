// Package coinbase đọc luồng giá công khai của Coinbase Exchange qua WebSocket (kênh ticker, không cần key).
package coinbase

import (
	"context"

	"github.com/phillong97254/news-market-pipeline/internal/domain"
)

type Gateway interface {
	// Stream mở 1 kết nối, gọi onTick cho mỗi giao dịch cho tới khi kết nối đứt,
	// ctx bị huỷ, hoặc onTick trả lỗi. Không tự kết nối lại: việc đó do service lo.
	Stream(ctx context.Context, products []string, onTick func(domain.Tick) error) error
}
