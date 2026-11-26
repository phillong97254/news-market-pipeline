package clickhouse

import (
	"net/http"
	"time"
)

const DefaultURL = "http://127.0.0.1:8123"

type impl struct {
	baseURL  string
	user     string
	password string
	http     *http.Client
}

func New(baseURL, user, password string) Gateway {
	if baseURL == "" {
		baseURL = DefaultURL
	}
	return &impl{baseURL: baseURL, user: user, password: password, http: &http.Client{Timeout: 30 * time.Second}}
}
