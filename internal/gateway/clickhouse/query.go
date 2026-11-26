package clickhouse

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// query chạy SQL (đã có FORMAT JSONEachRow) và decode từng dòng vào hàm row.
func (i *impl) query(ctx context.Context, sql string, params map[string]string, row func(dec *json.Decoder) error) error {
	q := url.Values{}
	q.Set("date_time_output_format", "iso")               // 2026-11-26T07:35:00Z
	q.Set("output_format_json_quote_64bit_integers", "0") // count() là UInt64: trả số, không trả chuỗi
	for k, v := range params {
		q.Set("param_"+k, v)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, i.baseURL+"/?"+q.Encode(), strings.NewReader(sql))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("X-ClickHouse-User", i.user)
	req.Header.Set("X-ClickHouse-Key", i.password)

	resp, err := i.http.Do(req)
	if err != nil {
		return fmt.Errorf("clickhouse request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("clickhouse status %d: %s", resp.StatusCode, bytes.TrimSpace(msg))
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		if len(bytes.TrimSpace(sc.Bytes())) == 0 {
			continue
		}
		if err := row(json.NewDecoder(bytes.NewReader(sc.Bytes()))); err != nil {
			return fmt.Errorf("decode row: %w", err)
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("read rows: %w", err)
	}
	return nil
}
