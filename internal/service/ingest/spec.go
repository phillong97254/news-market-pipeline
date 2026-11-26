// Package ingest nối nguồn giá với Redpanda và giữ kết nối sống (tự kết nối lại).
package ingest

import "context"

type Service interface {
	// Run chạy tới khi ctx bị huỷ. Lỗi kết nối không làm dừng: chờ backoff rồi kết nối lại.
	Run(ctx context.Context) error
}
