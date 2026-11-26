package clickhouse

import "time"

type newsRow struct {
	ID          string    `json:"id"`
	PublishedAt time.Time `json:"published_at"`
	Title       string    `json:"title"`
	Category    string    `json:"category"`
	Sentiment   float64   `json:"sentiment"`
	Impact      int       `json:"impact"`
	Feed        string    `json:"feed"`
	URL         string    `json:"url"`
}
