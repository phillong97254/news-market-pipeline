package gdelt

import (
	"net/http"
	"time"
)

const DefaultURL = "https://api.gdeltproject.org/api/v2/doc/doc"

type impl struct {
	baseURL   string
	userAgent string
	http      *http.Client
	now       func() time.Time
}

func New(baseURL, userAgent string) Gateway {
	if baseURL == "" {
		baseURL = DefaultURL
	}
	// Server GDELT bắt tay TLS rất chậm (đo từ Oracle: ~10-13s), vượt mức 10s mặc định của Go.
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSHandshakeTimeout = 30 * time.Second
	return &impl{
		baseURL:   baseURL,
		userAgent: userAgent,
		http:      &http.Client{Timeout: 60 * time.Second, Transport: tr},
		now:       time.Now,
	}
}
