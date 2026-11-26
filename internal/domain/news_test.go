package domain

import "testing"

func TestCanonicalURL(t *testing.T) {
	tests := []struct{ in, want string }{
		{"https://www.bbc.co.uk/news/articles/c89?at_medium=RSS&at_campaign=rss", "https://www.bbc.co.uk/news/articles/c89"},
		{"https://Example.COM/a/b/?utm_source=x&id=7#top", "https://example.com/a/b?id=7"},
		{"  https://vnexpress.net/x.html  ", "https://vnexpress.net/x.html"},
		{"https://example.com/", "https://example.com/"},
		{"not a url", "not a url"},
	}
	for _, tt := range tests {
		if got := CanonicalURL(tt.in); got != tt.want {
			t.Errorf("CanonicalURL(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestNewsID_SameArticleSameID(t *testing.T) {
	a := NewsID("https://www.bbc.co.uk/news/articles/c89?at_medium=RSS")
	b := NewsID("https://www.bbc.co.uk/news/articles/c89")
	if a != b || len(a) != 32 {
		t.Fatalf("ids differ or bad length: %q %q", a, b)
	}
	if NewsID("https://www.bbc.co.uk/news/articles/other") == a {
		t.Fatal("different articles share an id")
	}
}
