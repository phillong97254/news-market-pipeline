package jetstream

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"

	"github.com/coder/websocket"
)

const postCollection = "app.bsky.feed.post"

func (i *impl) Stream(ctx context.Context, input StreamInput, onPost func(Post) error) error {
	u, err := url.Parse(i.url)
	if err != nil {
		return fmt.Errorf("parse url: %w", err)
	}
	q := u.Query()
	q.Set("wantedCollections", postCollection)
	if input.Cursor > 0 {
		q.Set("cursor", strconv.FormatInt(input.Cursor, 10))
	}
	u.RawQuery = q.Encode()

	conn, _, err := websocket.Dial(ctx, u.String(), nil)
	if err != nil {
		return fmt.Errorf("dial jetstream: %w", err)
	}
	defer conn.CloseNow()
	conn.SetReadLimit(1 << 20)

	for {
		readCtx, cancel := context.WithTimeout(ctx, i.readTimeout)
		_, raw, err := conn.Read(readCtx)
		cancel()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		p, ok := parse(raw)
		if !ok {
			continue
		}
		if err := onPost(p); err != nil {
			return fmt.Errorf("handle post: %w", err)
		}
	}
}

// parse bỏ qua sự kiện không phải bài mới (sửa, xoá, identity, account) và JSON hỏng.
func parse(raw []byte) (Post, bool) {
	var e event
	if err := json.Unmarshal(raw, &e); err != nil {
		return Post{}, false
	}
	if e.Kind != "commit" || e.Commit == nil || e.Commit.Operation != "create" ||
		e.Commit.Collection != postCollection || e.Commit.Record == nil {
		return Post{}, false
	}
	return Post{TimeUS: e.TimeUS, Text: e.Commit.Record.Text, Langs: e.Commit.Record.Langs}, true
}
