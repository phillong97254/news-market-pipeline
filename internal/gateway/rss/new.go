package rss

import (
	"net/http"
	"sync"
	"time"
)

const maxSummary = 500 // ký tự

type cacheHeaders struct {
	etag         string
	lastModified string
}

type impl struct {
	userAgent string
	http      *http.Client
	now       func() time.Time

	mu    sync.Mutex
	cache map[string]cacheHeaders // theo URL feed
}

func New(userAgent string) Gateway {
	return &impl{
		userAgent: userAgent,
		http:      &http.Client{Timeout: 30 * time.Second},
		now:       time.Now,
		cache:     map[string]cacheHeaders{},
	}
}
