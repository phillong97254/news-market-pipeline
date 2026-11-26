// Package kafka đẩy bản ghi JSON vào Redpanda (Kafka API) bằng franz-go. Một client dùng chung cho mọi topic.
package kafka

import (
	"context"
	"time"
)

type Publisher interface {
	// Publish mã hoá value thành JSON rồi gửi bất đồng bộ (gom lô). Lỗi gửi được log trong callback.
	// Chặn lại khi bộ đệm đầy (backpressure) thay vì làm rơi dữ liệu.
	Publish(ctx context.Context, topic, key string, value any, ts time.Time) error
	// PublishSync gửi và chờ broker xác nhận. Dùng khi cần commit offset đầu vào sau khi gửi thành công.
	PublishSync(ctx context.Context, topic, key string, value any, ts time.Time) error
	// Close đẩy hết dữ liệu còn trong bộ đệm rồi đóng kết nối.
	Close(ctx context.Context)
}
