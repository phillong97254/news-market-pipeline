package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"strings"
	"time"
)

// NewsItem là một bản tin đã chuẩn hoá giữa các nguồn. Chỉ giữ metadata (tiêu đề, tóm tắt, link),
// không lưu nội dung bài báo.
type NewsItem struct {
	ID          string    `json:"id"`     // NewsID(URL): cùng bài từ 2 nguồn có cùng ID
	Source      string    `json:"source"` // "gdelt" | "rss"
	Feed        string    `json:"feed"`   // tên feed, ví dụ "bbc-world"; với GDELT là "gdelt"
	URL         string    `json:"url"`
	Title       string    `json:"title"`
	Summary     string    `json:"summary,omitempty"`
	Language    string    `json:"language,omitempty"`
	Domain      string    `json:"domain,omitempty"`
	Country     string    `json:"country,omitempty"`
	PublishedAt time.Time `json:"published_at"` // theo nguồn; GDELT là lúc GDELT thấy bài (bước 15 phút)
	IngestTime  time.Time `json:"ingest_time"`
}

// NewsID băm URL đã chuẩn hoá (bỏ fragment, tham số theo dõi utm_*/at_*, dấu / cuối, host viết thường).
func NewsID(raw string) string {
	sum := sha256.Sum256([]byte(CanonicalURL(raw)))
	return hex.EncodeToString(sum[:16])
}

func CanonicalURL(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" {
		return strings.TrimSpace(raw)
	}
	u.Fragment = ""
	u.Host = strings.ToLower(u.Host)
	u.Scheme = strings.ToLower(u.Scheme)
	q := u.Query()
	for k := range q {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, "utm_") || strings.HasPrefix(lk, "at_") {
			q.Del(k)
		}
	}
	u.RawQuery = q.Encode()
	if len(u.Path) > 1 {
		u.Path = strings.TrimSuffix(u.Path, "/")
	}
	return u.String()
}
