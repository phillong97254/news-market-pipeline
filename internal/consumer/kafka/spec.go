// Package kafka đọc topic theo consumer group, xử lý từng bản ghi một và chỉ commit offset sau khi xử lý xong
// (at-least-once: dừng giữa chừng thì lần sau xử lý lại bản ghi đó).
package kafka

import (
	"context"
	"errors"
	"time"
)

// ErrSkip: handler gặp lỗi vĩnh viễn với bản ghi này (dữ liệu hỏng...). Consumer log, commit và đi tiếp.
var ErrSkip = errors.New("skip record")

type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp time.Time
}

type Handler func(ctx context.Context, r Record) error

type Consumer interface {
	// Run chạy tới khi ctx bị huỷ. Handler trả lỗi khác ErrSkip thì chờ (backoff) rồi thử lại chính bản ghi đó,
	// nên Ollama chết thì pipeline dừng lại chờ chứ không bỏ tin.
	Run(ctx context.Context, h Handler) error
	Close()
}
