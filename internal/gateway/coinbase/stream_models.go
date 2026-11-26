package coinbase

import "time"

// Định dạng theo tài liệu Coinbase Exchange WebSocket.

type subscribeRequest struct {
	Type       string   `json:"type"` // "subscribe"
	ProductIDs []string `json:"product_ids"`
	Channels   []string `json:"channels"`
}

// message gom các trường cần dùng của mọi loại tin; Type quyết định trường nào có nghĩa.
type message struct {
	Type      string    `json:"type"` // ticker | heartbeat | subscriptions | error
	ProductID string    `json:"product_id"`
	Price     string    `json:"price"`
	LastSize  string    `json:"last_size"`
	Side      string    `json:"side"`
	TradeID   int64     `json:"trade_id"`
	Time      time.Time `json:"time"`
	Message   string    `json:"message"` // khi type=error
	Reason    string    `json:"reason"`  // khi type=error
}
