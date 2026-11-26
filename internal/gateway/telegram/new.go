package telegram

import (
	"net/http"
	"time"
)

const DefaultURL = "https://api.telegram.org"

type impl struct {
	baseURL string
	token   string
	chatID  string
	http    *http.Client
}

func New(baseURL, token, chatID string) Gateway {
	if baseURL == "" {
		baseURL = DefaultURL
	}
	return &impl{baseURL: baseURL, token: token, chatID: chatID, http: &http.Client{Timeout: 20 * time.Second}}
}
