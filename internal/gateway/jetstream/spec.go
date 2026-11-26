// Package jetstream đọc luồng bài đăng công khai của Bluesky qua Jetstream (WebSocket JSON, miễn phí, không cần key).
// Chỉ lấy bài mới tạo (app.bsky.feed.post, operation=create); không lấy tác giả, chỉ text và thời gian.
package jetstream

import "context"

type Gateway interface {
	// Stream chạy tới khi kết nối đứt hoặc ctx bị huỷ. Cursor > 0 thì đọc tiếp từ thời điểm đó (micro giây).
	Stream(ctx context.Context, input StreamInput, onPost func(Post) error) error
}

type StreamInput struct {
	Cursor int64
}

type Post struct {
	TimeUS int64 // thời điểm Jetstream nhận sự kiện (micro giây Unix), dùng làm cursor
	Text   string
	Langs  []string
}
