// Package processed nhớ ID bản tin đã gán nhãn (Redis), để nlp-worker không hỏi LLM lại cho tin trùng
// (news-producer gửi lại tin khi khởi động lại). Mất dữ liệu ở đây chỉ làm xử lý lại, không làm sai kết quả.
package processed

import "context"

type Store interface {
	// Done cho biết ID đã được xử lý xong chưa.
	Done(ctx context.Context, id string) (bool, error)
	// MarkDone gọi sau khi đã ghi kết quả thành công.
	MarkDone(ctx context.Context, id string) error
}
