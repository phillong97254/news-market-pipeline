package clickhouse

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func fakeServer(t *testing.T, status int, body string, check func(r *http.Request, sql string)) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-ClickHouse-User") != "grafana" || r.Header.Get("X-ClickHouse-Key") != "pw" {
			t.Errorf("auth headers missing")
		}
		sql, _ := io.ReadAll(r.Body)
		check(r, string(sql))
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestPriceChanges(t *testing.T) {
	body := `{"symbol":"BTC-USD","from_price":60000,"to_price":61200,"from_time":"2026-11-26T07:00:00Z","to_time":"2026-11-26T07:15:00Z"}
{"symbol":"ETH-USD","from_price":1700,"to_price":1683,"from_time":"2026-11-26T07:00:00Z","to_time":"2026-11-26T07:15:00Z"}
`
	srv := fakeServer(t, 200, body, func(r *http.Request, sql string) {
		if r.URL.Query().Get("param_w") != "15" || !strings.Contains(sql, "{w:UInt32}") {
			t.Errorf("params = %v", r.URL.Query())
		}
	})
	got, err := New(srv.URL, "grafana", "pw").PriceChanges(context.Background(), PriceChangesInput{Window: 15 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Symbol != "BTC-USD" || got[0].ToTime.Minute() != 15 {
		t.Fatalf("got %+v", got)
	}
	if p := got[0].Pct(); p < 1.99 || p > 2.01 {
		t.Fatalf("BTC pct = %v, want 2", p)
	}
	if p := got[1].Pct(); p > -0.99 || p < -1.01 {
		t.Fatalf("ETH pct = %v, want -1", p)
	}
}

func TestRecentNews(t *testing.T) {
	body := `{"id":"a1","published_at":"2026-11-26T06:50:00.000Z","title":"Fed holds rates","category":"monetary_policy","sentiment":-0.4,"impact":3,"feed":"bbc-business","url":"https://x/1"}` + "\n"
	from := time.Date(2026, 11, 26, 6, 0, 0, 0, time.UTC)
	srv := fakeServer(t, 200, body, func(r *http.Request, _ string) {
		q := r.URL.Query()
		if q.Get("param_from") != "1795672800000" || q.Get("param_min_impact") != "2" || q.Get("param_limit") != "3" {
			t.Errorf("params = %v", q)
		}
	})
	got, err := New(srv.URL, "grafana", "pw").RecentNews(context.Background(),
		RecentNewsInput{From: from, To: from.Add(time.Hour), MinImpact: 2, Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Impact != 3 || got[0].PublishedAt.Minute() != 50 {
		t.Fatalf("got %+v", got)
	}
}

func TestQuery_Error(t *testing.T) {
	srv := fakeServer(t, 500, "Code: 60. DB::Exception: Unknown table", func(*http.Request, string) {})
	_, err := New(srv.URL, "grafana", "pw").PriceChanges(context.Background(), PriceChangesInput{Window: time.Minute})
	if err == nil || !strings.Contains(err.Error(), "Unknown table") {
		t.Fatalf("err = %v", err)
	}
}
