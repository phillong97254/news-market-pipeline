package rss

import "encoding/xml"

// Một struct đọc được cả RSS 2.0 (<rss><channel><item>) và Atom (<feed><entry>).
type document struct {
	XMLName xml.Name // "rss" hoặc "feed"
	Channel struct {
		Language string    `xml:"language"`
		Items    []rssItem `xml:"item"`
	} `xml:"channel"`
	Lang    string      `xml:"lang,attr"` // Atom xml:lang
	Entries []atomEntry `xml:"entry"`
}

type rssItem struct {
	Title       string `xml:"title"`
	Link        string `xml:"link"`
	GUID        string `xml:"guid"`
	Description string `xml:"description"`
	PubDate     string `xml:"pubDate"`
	DCDate      string `xml:"http://purl.org/dc/elements/1.1/ date"`
}

type atomEntry struct {
	Title     string     `xml:"title"`
	Links     []atomLink `xml:"link"`
	Summary   string     `xml:"summary"`
	Published string     `xml:"published"`
	Updated   string     `xml:"updated"`
}

type atomLink struct {
	Href string `xml:"href,attr"`
	Rel  string `xml:"rel,attr"`
}
