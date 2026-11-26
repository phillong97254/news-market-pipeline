package jetstream

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/coder/websocket"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		ok   bool
	}{
		{"bài mới", `{"did":"x","time_us":1795680000000000,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.post","record":{"text":"Tariffs!","langs":["en"]}}}`, true},
		{"xoá bài", `{"kind":"commit","commit":{"operation":"delete","collection":"app.bsky.feed.post"}}`, false},
		{"like", `{"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.like","record":{}}}`, false},
		{"identity", `{"kind":"identity","identity":{}}`, false},
		{"hỏng", `{`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, ok := parse([]byte(tt.raw))
			if ok != tt.ok {
				t.Fatalf("ok = %v", ok)
			}
			if ok && (p.Text != "Tariffs!" || p.TimeUS != 1795680000000000 || p.Langs[0] != "en") {
				t.Fatalf("post = %+v", p)
			}
		})
	}
}

func TestStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("wantedCollections") != postCollection || r.URL.Query().Get("cursor") != "42" {
			t.Errorf("query = %v", r.URL.RawQuery)
		}
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer c.CloseNow()
		for _, m := range []string{
			`{"kind":"identity"}`,
			`{"time_us":43,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.post","record":{"text":"a"}}}`,
			`{"time_us":44,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.post","record":{"text":"b"}}}`,
		} {
			_ = c.Write(r.Context(), websocket.MessageText, []byte(m))
		}
		c.Close(websocket.StatusNormalClosure, "done")
	}))
	defer srv.Close()

	var got []string
	err := New("ws"+strings.TrimPrefix(srv.URL, "http")).Stream(context.Background(), StreamInput{Cursor: 42}, func(p Post) error {
		got = append(got, p.Text)
		return nil
	})
	if err == nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("err = %v, got = %v", err, got)
	}
}
