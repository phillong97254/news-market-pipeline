package rss

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const rssBody = `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:dc="http://purl.org/dc/elements/1.1/"><channel>
<language><![CDATA[en-gb]]></language>
<item>
  <title><![CDATA[US announces new tariffs]]></title>
  <description><![CDATA[The new levies would hit <b>China</b> &amp; the EU.]]></description>
  <link>https://www.bbc.co.uk/news/articles/c89?at_medium=RSS&amp;at_campaign=rss</link>
  <pubDate>Thu, 26 Nov 2026 04:12:00 GMT</pubDate>
</item>
<item>
  <title>Bài có ảnh</title>
  <description><![CDATA[<a href="https://vnexpress.net/x.html"><img src="a.jpg"></a></br>Ngân hàng   trung ương giữ lãi suất.]]></description>
  <guid>https://vnexpress.net/x.html</guid>
  <pubDate>Thu, 26 Nov 2026 11:05:00 +0700</pubDate>
</item>
<item><title>No link</title></item>
</channel></rss>`

const atomBody = `<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xml:lang="en">
<entry>
  <title>Fed holds rates</title>
  <link rel="alternate" href="https://example.com/fed"/>
  <summary>Summary here</summary>
  <updated>2026-11-26T03:00:00Z</updated>
</entry>
</feed>`

func newTestGateway(fixed time.Time) *impl {
	g := New("test-agent").(*impl)
	g.now = func() time.Time { return fixed }
	return g
}

func TestParse_RSS(t *testing.T) {
	fixed := time.Date(2026, 11, 26, 5, 0, 0, 0, time.UTC)
	items, err := newTestGateway(fixed).parse(Feed{Name: "bbc-world", URL: "https://feeds.bbci.co.uk/news/world/rss.xml"}, []byte(rssBody))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("got %d items, want 2", len(items))
	}
	a := items[0]
	if a.Title != "US announces new tariffs" || a.Summary != "The new levies would hit China & the EU." ||
		a.Language != "en-gb" || a.Domain != "bbc.co.uk" || a.Feed != "bbc-world" || a.Source != "rss" {
		t.Fatalf("item 0 = %+v", a)
	}
	if !a.PublishedAt.Equal(time.Date(2026, 11, 26, 4, 12, 0, 0, time.UTC)) {
		t.Fatalf("published = %v", a.PublishedAt)
	}
	b := items[1]
	if b.URL != "https://vnexpress.net/x.html" || b.Summary != "Ngân hàng trung ương giữ lãi suất." {
		t.Fatalf("item 1 = %+v", b)
	}
	if !b.PublishedAt.Equal(time.Date(2026, 11, 26, 4, 5, 0, 0, time.UTC)) {
		t.Fatalf("timezone not converted: %v", b.PublishedAt)
	}
}

func TestParse_Atom(t *testing.T) {
	items, err := newTestGateway(time.Now()).parse(Feed{Name: "x", URL: "https://example.com/feed"}, []byte(atomBody))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].URL != "https://example.com/fed" || items[0].Language != "en" {
		t.Fatalf("items = %+v", items)
	}
}

func TestParse_Invalid(t *testing.T) {
	if _, err := newTestGateway(time.Now()).parse(Feed{Name: "x"}, []byte("<html><body>oops")); err == nil {
		t.Fatal("want parse error")
	}
}

func TestTruncate(t *testing.T) {
	if got := truncate(strings.Repeat("ă", 600), 500); len([]rune(got)) != 501 {
		t.Fatalf("rune length = %d", len([]rune(got)))
	}
}

func TestFetch_ConditionalGet(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("If-None-Match") == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		_, _ = w.Write([]byte(rssBody))
	}))
	defer srv.Close()

	g := newTestGateway(time.Now())
	feed := Feed{Name: "t", URL: srv.URL}
	first, err := g.Fetch(context.Background(), feed)
	if err != nil || len(first) != 2 {
		t.Fatalf("first fetch: %d items, err %v", len(first), err)
	}
	second, err := g.Fetch(context.Background(), feed)
	if err != nil || len(second) != 0 {
		t.Fatalf("second fetch: %d items, err %v", len(second), err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d", calls)
	}
}

func TestFetch_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	if _, err := newTestGateway(time.Now()).Fetch(context.Background(), Feed{Name: "t", URL: srv.URL}); err == nil {
		t.Fatal("want error on 503")
	}
}
