package gdelt

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSearchArticles(t *testing.T) {
	fixed := time.Date(2026, 11, 26, 5, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		status    int
		body      string
		wantErr   error
		wantN     int
		wantTitle string
	}{
		{
			name:   "ok",
			status: 200,
			body: `{"articles":[
				{"url":"https://x.com/a?utm_source=g","title":"Embraer rejects  proposed U . S . tariff - Xinhua","seendate":"20261126T043000Z","domain":"x.com","language":"English","sourcecountry":"China"},
				{"url":"","title":"no url"},
				{"url":"https://x.com/b","title":"Bad date","seendate":"??"}]}`,
			wantN:     2,
			wantTitle: "Embraer rejects proposed U . S . tariff - Xinhua",
		},
		{name: "empty", status: 200, body: `{}`, wantN: 0},
		{name: "text error with 200", status: 200, body: "Timespan is too short.\n", wantErr: ErrBadResponse},
		{name: "rate limited", status: 429, body: "Please limit requests to one every 5 seconds", wantErr: ErrRateLimited},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotQuery string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotQuery = r.URL.RawQuery
				if r.Header.Get("User-Agent") != "test-agent" {
					t.Errorf("user agent = %q", r.Header.Get("User-Agent"))
				}
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer srv.Close()

			g := &impl{baseURL: srv.URL, userAgent: "test-agent", http: srv.Client(), now: func() time.Time { return fixed }}
			items, err := g.SearchArticles(context.Background(), SearchInput{Query: "tariff", Timespan: time.Hour, Max: 250})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if len(items) != tt.wantN {
				t.Fatalf("got %d items, want %d", len(items), tt.wantN)
			}
			if gotQuery == "" {
				t.Fatal("no request made")
			}
			if tt.wantN == 0 {
				return
			}
			first := items[0]
			if first.Title != tt.wantTitle || first.Source != "gdelt" || first.Country != "China" {
				t.Fatalf("item = %+v", first)
			}
			if !first.PublishedAt.Equal(time.Date(2026, 11, 26, 4, 30, 0, 0, time.UTC)) {
				t.Fatalf("published = %v", first.PublishedAt)
			}
			if !items[1].PublishedAt.Equal(fixed) {
				t.Fatalf("bad seendate should fall back to now, got %v", items[1].PublishedAt)
			}
		})
	}
}
