package gdelt

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/phillong97254/news-market-pipeline/internal/domain"
)

func (i *impl) SearchArticles(ctx context.Context, input SearchInput) ([]domain.NewsItem, error) {
	q := url.Values{}
	q.Set("query", input.Query)
	q.Set("mode", "artlist")
	q.Set("format", "json")
	q.Set("sort", "datedesc")
	q.Set("timespan", strconv.Itoa(int(input.Timespan.Minutes()))+"min")
	q.Set("maxrecords", strconv.Itoa(input.Max))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, i.baseURL+"?"+q.Encode(), nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("User-Agent", i.userAgent)

	resp, err := i.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("gdelt request: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, ErrRateLimited
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("gdelt status %d: %s", resp.StatusCode, snippet(body))
	}
	// GDELT báo lỗi truy vấn bằng văn bản thường kèm status 200 (ví dụ "Timespan is too short.").
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, fmt.Errorf("%w: %s", ErrBadResponse, snippet(body))
	}

	var out artListResponse
	if err := json.Unmarshal(trimmed, &out); err != nil {
		return nil, fmt.Errorf("decode gdelt: %w", err)
	}
	now := i.now().UTC()
	items := make([]domain.NewsItem, 0, len(out.Articles))
	for _, a := range out.Articles {
		if a.URL == "" || a.Title == "" {
			continue
		}
		seen, err := time.Parse(seenDateLayout, a.SeenDate)
		if err != nil {
			seen = now
		}
		items = append(items, domain.NewsItem{
			ID:          domain.NewsID(a.URL),
			Source:      "gdelt",
			Feed:        "gdelt",
			URL:         a.URL,
			Title:       strings.Join(strings.Fields(a.Title), " "),
			Language:    a.Language,
			Domain:      a.Domain,
			Country:     a.SourceCountry,
			PublishedAt: seen.UTC(),
			IngestTime:  now,
		})
	}
	return items, nil
}

func snippet(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 200 {
		s = s[:200] + "…"
	}
	return s
}
