package clickhouse

import "time"

type priceChangeRow struct {
	Symbol    string    `json:"symbol"`
	FromPrice float64   `json:"from_price"`
	ToPrice   float64   `json:"to_price"`
	FromTime  time.Time `json:"from_time"`
	ToTime    time.Time `json:"to_time"`
}
