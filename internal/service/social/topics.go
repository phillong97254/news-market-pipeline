package social

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// Topic là một chủ đề và các cụm từ khoá (chữ thường). Khớp nguyên từ: "eth" không khớp "ethics".
type Topic struct {
	Name     string
	Keywords []string
}

// DefaultTopics: chủ đề gần với các nhãn của nlp-worker, có cả tiếng Việt.
var DefaultTopics = []Topic{
	{"tariff", []string{"tariff", "tariffs", "trade war", "thuế quan"}},
	{"fed", []string{"federal reserve", "fomc", "powell", "rate cut", "rate cuts", "rate hike", "rate hikes", "interest rate", "interest rates", "lãi suất"}},
	{"inflation", []string{"inflation", "cpi", "lạm phát"}},
	{"recession", []string{"recession", "suy thoái"}},
	{"sanctions", []string{"sanction", "sanctions", "trừng phạt"}},
	{"stocks", []string{"stock market", "s&p 500", "nasdaq", "dow jones", "wall street", "chứng khoán", "vn-index", "vnindex"}},
	{"bitcoin", []string{"bitcoin", "btc"}},
	{"ethereum", []string{"ethereum", "eth"}},
	{"crypto", []string{"crypto", "cryptocurrency", "stablecoin", "stablecoins", "tiền điện tử", "tiền mã hóa", "tiền số"}},
	{"gold", []string{"gold price", "price of gold", "giá vàng"}},
	{"oil", []string{"oil price", "oil prices", "crude oil", "opec", "giá dầu"}},
}

// match trả các chủ đề có trong text (mỗi chủ đề tối đa 1 lần).
func match(topics []Topic, text string) []string {
	lower := strings.ToLower(text)
	var out []string
	for _, t := range topics {
		for _, kw := range t.Keywords {
			if containsWord(lower, kw) {
				out = append(out, t.Name)
				break
			}
		}
	}
	return out
}

func containsWord(s, kw string) bool {
	for start := 0; ; {
		k := strings.Index(s[start:], kw)
		if k < 0 {
			return false
		}
		k += start
		end := k + len(kw)
		if boundaryBefore(s, k) && boundaryAfter(s, end) {
			return true
		}
		start = k + 1
	}
}

func boundaryBefore(s string, k int) bool {
	if k == 0 {
		return true
	}
	r, _ := utf8.DecodeLastRuneInString(s[:k])
	return !isWordRune(r)
}

func boundaryAfter(s string, end int) bool {
	if end >= len(s) {
		return true
	}
	r, _ := utf8.DecodeRuneInString(s[end:])
	return !isWordRune(r)
}

func isWordRune(r rune) bool { return unicode.IsLetter(r) || unicode.IsDigit(r) }
