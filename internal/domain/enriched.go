package domain

import "time"

// NewsAnalysis là nhãn do LLM gán cho một bản tin.
type NewsAnalysis struct {
	TitleEN     string    `json:"title_en"`
	Category    string    `json:"category"`  // xem Categories
	Relevant    bool      `json:"relevant"`  // thị trường có thể phản ứng với tin này
	Sentiment   float64   `json:"sentiment"` // -1 (xấu cho tài sản rủi ro) .. 1 (tốt)
	Impact      int       `json:"impact"`    // 0 không, 1 thấp, 2 vừa, 3 cao
	Assets      []string  `json:"assets"`    // xem Assets
	Model       string    `json:"model"`
	ProcessedAt time.Time `json:"processed_at"`
	LatencyMS   int64     `json:"latency_ms"`
}

// EnrichedNews: bản tin gốc + nhãn, JSON phẳng (dễ đọc vào ClickHouse ở giai đoạn 4).
type EnrichedNews struct {
	NewsItem
	NewsAnalysis
}

var Categories = []string{
	"trade", "monetary_policy", "fiscal", "election", "geopolitics",
	"sanctions", "crypto_regulation", "corporate", "other",
}

var Assets = []string{"BTC", "ETH", "CRYPTO", "US_STOCKS", "VN_STOCKS", "USD", "GOLD", "OIL"}
