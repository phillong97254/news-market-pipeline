// Package telegram gửi tin nhắn qua Telegram Bot API (sendMessage).
// Token nằm trong URL của API, nên mọi lỗi trả ra đều đã bỏ URL: không bao giờ log token.
package telegram

import (
	"context"
	"errors"
)

var (
	// ErrRejected: Telegram từ chối (sai chat_id, bot bị chặn, HTML sai...). Gửi lại cũng không được.
	ErrRejected = errors.New("telegram rejected message")
	// ErrUnavailable: lỗi mạng, 5xx hoặc 429. Thử lại sau.
	ErrUnavailable = errors.New("telegram unavailable")
)

type Gateway interface {
	// Send gửi text dạng HTML (chỉ các thẻ Telegram hỗ trợ: b, i, a, code...).
	Send(ctx context.Context, input SendInput) error
}

type SendInput struct {
	Text string
}
