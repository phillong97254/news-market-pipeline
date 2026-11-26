package ollama

import (
	"net/http"
	"time"
)

const DefaultURL = "http://127.0.0.1:11434"

// Câu trả lời đúng dài khoảng 50-80 token; vượt 200 coi như model đang lặp, cắt và báo ErrBadOutput.
const maxOutputTokens = 200

type impl struct {
	baseURL string
	model   string
	http    *http.Client
	now     func() time.Time
}

// New: timeout dài vì CPU ARM sinh ~6 token/giây; lần đầu còn phải nạp model (~50s).
func New(baseURL, model string) Gateway {
	if baseURL == "" {
		baseURL = DefaultURL
	}
	return &impl{
		baseURL: baseURL,
		model:   model,
		http:    &http.Client{Timeout: 5 * time.Minute},
		now:     time.Now,
	}
}
