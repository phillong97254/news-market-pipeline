package coinbase

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/phillong97254/news-market-pipeline/internal/domain"
)

func TestParse(t *testing.T) {
	fixed := time.Date(2026, 11, 25, 3, 0, 0, 0, time.UTC)
	g := &impl{now: func() time.Time { return fixed }}

	tests := []struct {
		name    string
		raw     string
		want    domain.Tick
		wantOK  bool
		wantErr error
	}{
		{
			name: "ticker",
			raw: `{"type":"ticker","sequence":1,"product_id":"BTC-USD","price":"91234.56","last_size":"0.0012",
				"side":"buy","trade_id":42,"time":"2026-11-25T02:59:59.123456Z","best_bid":"91234.00"}`,
			want: domain.Tick{
				Source: "coinbase", Symbol: "BTC-USD", Price: "91234.56", Size: "0.0012", Side: "buy",
				TradeID: 42, EventTime: time.Date(2026, 11, 25, 2, 59, 59, 123456000, time.UTC), IngestTime: fixed,
			},
			wantOK: true,
		},
		{name: "heartbeat bỏ qua", raw: `{"type":"heartbeat","product_id":"BTC-USD"}`},
		{name: "subscriptions bỏ qua", raw: `{"type":"subscriptions","channels":[]}`},
		{name: "error", raw: `{"type":"error","message":"Failed to subscribe","reason":"XYZ is not a valid product"}`, wantErr: ErrServer},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok, err := g.parse([]byte(tt.raw))
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if got != tt.want {
				t.Fatalf("tick = %+v\nwant   %+v", got, tt.want)
			}
		})
	}
}

func TestParse_InvalidJSON(t *testing.T) {
	g := &impl{now: time.Now}
	if _, _, err := g.parse([]byte("not json")); err == nil {
		t.Fatal("want decode error")
	}
}

// Server giả: kiểm tra tin subscribe, gửi heartbeat + 2 ticker rồi đóng.
func TestStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer c.CloseNow()
		_, raw, err := c.Read(r.Context())
		if err != nil {
			t.Error(err)
			return
		}
		var sub subscribeRequest
		if err := json.Unmarshal(raw, &sub); err != nil || sub.Type != "subscribe" || len(sub.ProductIDs) != 2 {
			t.Errorf("bad subscribe: %s", raw)
		}
		for _, m := range []string{
			`{"type":"heartbeat"}`,
			`{"type":"ticker","product_id":"BTC-USD","price":"1","trade_id":1,"time":"2026-11-25T00:00:00Z"}`,
			`{"type":"ticker","product_id":"ETH-USD","price":"2","trade_id":2,"time":"2026-11-25T00:00:01Z"}`,
		} {
			_ = c.Write(r.Context(), websocket.MessageText, []byte(m))
		}
		c.Close(websocket.StatusNormalClosure, "bye")
	}))
	defer srv.Close()

	g := New("ws"+strings.TrimPrefix(srv.URL, "http"), 2*time.Second)
	var got []string
	err := g.Stream(context.Background(), []string{"BTC-USD", "ETH-USD"}, func(tk domain.Tick) error {
		got = append(got, tk.Symbol)
		return nil
	})
	if err == nil {
		t.Fatal("want error when server closes the connection")
	}
	if len(got) != 2 || got[0] != "BTC-USD" || got[1] != "ETH-USD" {
		t.Fatalf("got ticks %v", got)
	}
}

func TestStream_ReadTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		_, _, _ = c.Read(r.Context()) // nhận subscribe rồi im lặng
		<-r.Context().Done()
	}))
	defer srv.Close()

	g := New("ws"+strings.TrimPrefix(srv.URL, "http"), 200*time.Millisecond)
	start := time.Now()
	err := g.Stream(context.Background(), []string{"BTC-USD"}, func(domain.Tick) error { return nil })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want deadline exceeded", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("read timeout not applied")
	}
}
