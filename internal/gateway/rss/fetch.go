package rss

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/phillong97254/news-market-pipeline/internal/domain"
)

func (i *impl) Fetch(ctx context.Context, feed Feed) ([]domain.NewsItem, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, feed.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("User-Agent", i.userAgent)
	req.Header.Set("Accept", "application/rss+xml, application/atom+xml, application/xml, text/xml")
	i.mu.Lock()
	ch := i.cache[feed.URL]
	i.mu.Unlock()
	if ch.etag != "" {
		req.Header.Set("If-None-Match", ch.etag)
	}
	if ch.lastModified != "" {
		req.Header.Set("If-Modified-Since", ch.lastModified)
	}

	resp, err := i.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", feed.Name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotModified {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch %s: status %d", feed.Name, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 5<<20))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", feed.Name, err)
	}

	items, err := i.parse(feed, body)
	if err != nil {
		return nil, err
	}
	// Chỉ nhớ ETag khi đã đọc được feed, để lần lỗi sau không bị 304 che mất dữ liệu.
	i.mu.Lock()
	i.cache[feed.URL] = cacheHeaders{etag: resp.Header.Get("ETag"), lastModified: resp.Header.Get("Last-Modified")}
	i.mu.Unlock()
	return items, nil
}

func (i *impl) parse(feed Feed, body []byte) ([]domain.NewsItem, error) {
	var doc document
	dec := xml.NewDecoder(bytes.NewReader(body))
	dec.Strict = false
	dec.CharsetReader = func(charset string, r io.Reader) (io.Reader, error) {
		// Feed không phải UTF-8 rất hiếm; đọc thẳng thay vì thêm thư viện mã hoá.
		return r, nil
	}
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("parse %s: %w", feed.Name, err)
	}
	if root := doc.XMLName.Local; root != "rss" && root != "feed" {
		return nil, fmt.Errorf("parse %s: not an RSS/Atom feed (root <%s>)", feed.Name, root)
	}

	now := i.now().UTC()
	host := ""
	if u, err := url.Parse(feed.URL); err == nil {
		host = strings.TrimPrefix(strings.ToLower(u.Hostname()), "www.")
	}
	base := domain.NewsItem{Source: "rss", Feed: feed.Name, IngestTime: now}

	var items []domain.NewsItem
	add := func(title, link, summary, date, lang string) {
		title = cleanText(title)
		link = strings.TrimSpace(link)
		if title == "" || !strings.HasPrefix(link, "http") {
			return
		}
		it := base
		it.ID = domain.NewsID(link)
		it.URL = link
		it.Title = title
		it.Summary = truncate(cleanText(summary), maxSummary)
		it.Language = lang
		it.Domain = linkHost(link, host)
		it.PublishedAt = parseDate(date, now)
		items = append(items, it)
	}

	for _, it := range doc.Channel.Items {
		link := it.Link
		if link == "" && strings.HasPrefix(it.GUID, "http") {
			link = it.GUID
		}
		date := it.PubDate
		if date == "" {
			date = it.DCDate
		}
		add(it.Title, link, it.Description, date, doc.Channel.Language)
	}
	for _, e := range doc.Entries {
		date := e.Published
		if date == "" {
			date = e.Updated
		}
		add(e.Title, atomHref(e.Links), e.Summary, date, doc.Lang)
	}
	return items, nil
}

func atomHref(links []atomLink) string {
	for _, l := range links {
		if l.Rel == "" || l.Rel == "alternate" {
			return l.Href
		}
	}
	if len(links) > 0 {
		return links[0].Href
	}
	return ""
}

var dateLayouts = []string{
	time.RFC1123Z, time.RFC1123, time.RFC3339, time.RFC3339Nano,
	"Mon, 2 Jan 2006 15:04:05 -0700", "Mon, 2 Jan 2006 15:04:05 MST", "Mon, 02 Jan 2006 15:04 -0700",
	"2006-01-02T15:04:05", "2006-01-02 15:04:05",
}

// parseDate: không đọc được thì dùng thời điểm hiện tại (bài vẫn được giữ).
func parseDate(s string, fallback time.Time) time.Time {
	s = strings.TrimSpace(s)
	for _, l := range dateLayouts {
		if t, err := time.Parse(l, s); err == nil {
			return t.UTC()
		}
	}
	return fallback
}

var (
	tagRE   = regexp.MustCompile(`<[^>]*>`)
	spaceRE = regexp.MustCompile(`\s+`)
)

// cleanText bỏ thẻ HTML (VnExpress nhét <a><img> vào description) và gộp khoảng trắng.
func cleanText(s string) string {
	s = html.UnescapeString(tagRE.ReplaceAllString(s, " "))
	return strings.TrimSpace(spaceRE.ReplaceAllString(s, " "))
}

func truncate(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	r := []rune(s)
	return strings.TrimSpace(string(r[:n])) + "…"
}

func linkHost(link, fallback string) string {
	if u, err := url.Parse(link); err == nil && u.Hostname() != "" {
		return strings.TrimPrefix(strings.ToLower(u.Hostname()), "www.")
	}
	return fallback
}
