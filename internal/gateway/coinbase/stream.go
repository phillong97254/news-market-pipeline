package coinbase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/coder/websocket"

	"github.com/phillong97254/news-market-pipeline/internal/domain"
)

// ErrServer: Coinbase gửi tin type=error (ví dụ sai product id).
var ErrServer = errors.New("coinbase error message")

func (i *impl) Stream(ctx context.Context, products []string, onTick func(domain.Tick) error) error {
	conn, _, err := websocket.Dial(ctx, i.url, nil)
	if err != nil {
		return fmt.Errorf("dial %s: %w", i.url, err)
	}
	defer conn.CloseNow()
	conn.SetReadLimit(1 << 20)

	sub, err := json.Marshal(subscribeRequest{
		Type: "subscribe", ProductIDs: products, Channels: []string{"ticker", "heartbeat"},
	})
	if err != nil {
		return fmt.Errorf("encode subscribe: %w", err)
	}
	if err := conn.Write(ctx, websocket.MessageText, sub); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	for {
		raw, err := i.read(ctx, conn)
		if err != nil {
			return err
		}
		tick, ok, err := i.parse(raw)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if err := onTick(tick); err != nil {
			return fmt.Errorf("handle tick: %w", err)
		}
	}
}

func (i *impl) read(ctx context.Context, conn *websocket.Conn) ([]byte, error) {
	readCtx, cancel := context.WithTimeout(ctx, i.readTimeout)
	defer cancel()
	_, raw, err := conn.Read(readCtx)
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	return raw, nil
}

// parse trả ok=false với tin không phải giao dịch (heartbeat, subscriptions...).
func (i *impl) parse(raw []byte) (domain.Tick, bool, error) {
	var m message
	if err := json.Unmarshal(raw, &m); err != nil {
		return domain.Tick{}, false, fmt.Errorf("decode message: %w", err)
	}
	switch m.Type {
	case "ticker":
		return domain.Tick{
			Source:     "coinbase",
			Symbol:     m.ProductID,
			Price:      m.Price,
			Size:       m.LastSize,
			Side:       m.Side,
			TradeID:    m.TradeID,
			EventTime:  m.Time.UTC(),
			IngestTime: i.now().UTC(),
		}, true, nil
	case "error":
		return domain.Tick{}, false, fmt.Errorf("%w: %s %s", ErrServer, m.Message, m.Reason)
	default:
		return domain.Tick{}, false, nil
	}
}
