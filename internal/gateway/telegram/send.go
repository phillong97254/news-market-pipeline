package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

func (i *impl) Send(ctx context.Context, input SendInput) error {
	body, err := json.Marshal(sendRequest{
		ChatID: i.chatID, Text: input.Text, ParseMode: "HTML",
		LinkPreviewOptions: linkPreviewOptions{IsDisabled: true},
	})
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, i.baseURL+"/bot"+i.token+"/sendMessage", bytes.NewReader(body))
	if err != nil {
		return errors.New("build telegram request") // không bọc err: nó chứa URL có token
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := i.http.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUnavailable, stripURL(err))
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return fmt.Errorf("%w: read body: %v", ErrUnavailable, stripURL(err))
	}
	var r apiResponse
	_ = json.Unmarshal(raw, &r) // body lạ thì dựa vào status code

	switch {
	case resp.StatusCode == http.StatusOK && r.OK:
		return nil
	case resp.StatusCode == http.StatusTooManyRequests:
		return fmt.Errorf("%w: rate limited, retry after %ds", ErrUnavailable, r.Parameters.RetryAfter)
	case resp.StatusCode >= 500:
		return fmt.Errorf("%w: status %d", ErrUnavailable, resp.StatusCode)
	default:
		return fmt.Errorf("%w: status %d: %s", ErrRejected, resp.StatusCode, r.Description)
	}
}

// stripURL bỏ URL (có token) khỏi lỗi của net/http.
func stripURL(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return fmt.Errorf("%s: %w", ue.Op, ue.Err)
	}
	return err
}
