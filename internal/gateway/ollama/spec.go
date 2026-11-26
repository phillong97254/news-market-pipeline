// Package ollama gán nhãn bản tin bằng LLM chạy cục bộ qua Ollama (/api/chat, structured output).
package ollama

import (
	"context"
	"errors"

	"github.com/phillong97254/news-market-pipeline/internal/domain"
)

var (
	// ErrBadOutput: model trả nội dung không đọc được theo schema. Thử lại có thể ra kết quả khác.
	ErrBadOutput = errors.New("ollama returned invalid output")
	// ErrUnavailable: Ollama không trả lời hoặc lỗi 5xx. Nên chờ rồi thử lại.
	ErrUnavailable = errors.New("ollama unavailable")
)

type Gateway interface {
	Classify(ctx context.Context, input ClassifyInput) (domain.NewsAnalysis, error)
}

type ClassifyInput struct {
	Title   string
	Summary string
	// Attempt: 0 cho lần đầu (temperature 0, kết quả ổn định). Lần thử lại đổi seed và tăng nhẹ temperature
	// để không lặp lại đúng đầu ra hỏng.
	Attempt int
}
