package ollama

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestNormalize(t *testing.T) {
	tests := []struct {
		name       string
		in         labels
		wantAssets []string
		wantImpact int
		wantSent   float64
		wantErr    error
	}{
		{
			name:       "bỏ tài sản trùng và lạ, sắp xếp",
			in:         labels{Category: "trade", Relevant: true, Sentiment: -0.5, Impact: 2, Assets: []string{"USD", "USD", "CRYPTO", "NASDAQ"}},
			wantAssets: []string{"CRYPTO", "USD"}, wantImpact: 2, wantSent: -0.5,
		},
		{
			name:       "kẹp số ngoài khoảng",
			in:         labels{Category: "trade", Relevant: true, Sentiment: -3, Impact: 9},
			wantAssets: []string{}, wantImpact: 3, wantSent: -1,
		},
		{
			name:       "không liên quan thì impact 0, không tài sản",
			in:         labels{Category: "other", Relevant: false, Impact: 2, Assets: []string{"BTC"}},
			wantAssets: []string{}, wantImpact: 0,
		},
		{name: "category lạ", in: labels{Category: "weather"}, wantErr: ErrBadOutput},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, err := normalize(tt.in)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if !reflect.DeepEqual(a.Assets, tt.wantAssets) || a.Impact != tt.wantImpact || a.Sentiment != tt.wantSent {
				t.Fatalf("got %+v", a)
			}
		})
	}
}

func TestClassify(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		body    string
		wantErr error
		wantCat string
	}{
		{
			name: "ok", status: 200,
			body:    `{"message":{"role":"assistant","content":"{\"title_en\":\"US tariffs\",\"category\":\"trade\",\"relevant\":true,\"sentiment\":-0.5,\"impact\":2,\"assets\":[\"USD\"]}"}}`,
			wantCat: "trade",
		},
		{name: "nội dung không phải JSON", status: 200, body: `{"message":{"content":"Sure! Here is"}}`, wantErr: ErrBadOutput},
		{name: "Ollama lỗi 500", status: 500, body: `{"error":"out of memory"}`, wantErr: ErrUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req chatRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
				}
				if req.Model != "qwen2.5:7b" || req.Stream || req.Format == nil || len(req.Messages) != 2 ||
					!strings.Contains(req.Messages[1].Content, "Summary: s") {
					t.Errorf("bad request: %+v", req)
				}
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer srv.Close()

			g := New(srv.URL, "qwen2.5:7b")
			a, err := g.Classify(context.Background(), ClassifyInput{Title: "t", Summary: "s"})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if err == nil && (a.Category != tt.wantCat || a.Model != "qwen2.5:7b" || a.ProcessedAt.IsZero()) {
				t.Fatalf("analysis = %+v", a)
			}
		})
	}
}

func TestOptions(t *testing.T) {
	first, retry := options(0), options(2)
	if first.Temperature != 0 || first.Seed != 1 || first.NumPredict != maxOutputTokens {
		t.Fatalf("first = %+v", first)
	}
	if retry.Temperature == 0 || retry.Seed == first.Seed {
		t.Fatalf("retry must differ: %+v", retry)
	}
}

func TestSchema_LimitsAssets(t *testing.T) {
	assets := outputSchema()["properties"].(map[string]any)["assets"].(map[string]any)
	if assets["maxItems"] != 4 {
		t.Fatalf("assets schema = %v", assets)
	}
}

func TestClassify_Down(t *testing.T) {
	g := &impl{baseURL: "http://127.0.0.1:1", model: "m", http: &http.Client{Timeout: time.Second}, now: time.Now}
	if _, err := g.Classify(context.Background(), ClassifyInput{Title: "t"}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
}
