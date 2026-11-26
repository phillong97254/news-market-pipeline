package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const testToken = "123:secret-token"

func TestSend(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		body    string
		wantErr error
	}{
		{name: "ok", status: 200, body: `{"ok":true,"result":{}}`},
		{name: "sai chat", status: 400, body: `{"ok":false,"description":"Bad Request: chat not found"}`, wantErr: ErrRejected},
		{name: "bị chặn", status: 403, body: `{"ok":false,"description":"Forbidden: bot was blocked by the user"}`, wantErr: ErrRejected},
		{name: "429", status: 429, body: `{"ok":false,"parameters":{"retry_after":7}}`, wantErr: ErrUnavailable},
		{name: "502", status: 502, body: `<html>`, wantErr: ErrUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/bot"+testToken+"/sendMessage" {
					t.Errorf("path = %s", r.URL.Path)
				}
				var req sendRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
				}
				if req.ChatID != "42" || req.Text != "<b>hi</b>" || req.ParseMode != "HTML" || !req.LinkPreviewOptions.IsDisabled {
					t.Errorf("bad request: %+v", req)
				}
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer srv.Close()

			err := New(srv.URL, testToken, "42").Send(context.Background(), SendInput{Text: "<b>hi</b>"})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if err != nil && strings.Contains(err.Error(), "secret") {
				t.Fatalf("error leaks token: %v", err)
			}
		})
	}
}

func TestSend_NetworkErrorHidesToken(t *testing.T) {
	err := New("http://127.0.0.1:1", testToken, "42").Send(context.Background(), SendInput{Text: "x"})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
	if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "/bot") {
		t.Fatalf("error leaks token: %v", err)
	}
}
