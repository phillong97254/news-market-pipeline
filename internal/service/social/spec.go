// Package social đếm số bài Bluesky nhắc tới từng chủ đề mỗi phút và ghi vào Kafka (social.mentions).
// Chỉ ghi số đếm; nội dung và tác giả bài không rời khỏi tiến trình.
package social

import "context"

type Service interface {
	// Run đọc Jetstream tới khi ctx bị huỷ, tự kết nối lại và đọc tiếp từ cursor.
	Run(ctx context.Context) error
}
