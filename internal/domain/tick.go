// Package domain chứa các bản ghi đi qua pipeline.
package domain

import "time"

// Tick là một giao dịch khớp trên sàn, đã chuẩn hoá giữa các nguồn.
// Price và Size giữ dạng chuỗi thập phân như sàn trả về để không mất độ chính xác
// (ClickHouse sẽ đọc thành Decimal ở giai đoạn 4).
type Tick struct {
	Source     string    `json:"source"`      // ví dụ "coinbase"
	Symbol     string    `json:"symbol"`      // ví dụ "BTC-USD"
	Price      string    `json:"price"`       // giá khớp
	Size       string    `json:"size"`        // khối lượng lệnh khớp cuối
	Side       string    `json:"side"`        // "buy" | "sell" (phía taker)
	TradeID    int64     `json:"trade_id"`    // id giao dịch của sàn, dùng để loại trùng
	EventTime  time.Time `json:"event_time"`  // thời điểm khớp theo sàn (UTC)
	IngestTime time.Time `json:"ingest_time"` // thời điểm producer nhận được (UTC)
}
