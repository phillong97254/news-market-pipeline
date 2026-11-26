package alert

import (
	"fmt"
	"html"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/phillong97254/news-market-pipeline/internal/domain"
	"github.com/phillong97254/news-market-pipeline/internal/gateway/clickhouse"
)

const disclaimer = "<i>Chỉ là trùng thời gian, không phải nguyên nhân hay khuyến nghị đầu tư.</i>"

func escape(s string) string { return html.EscapeString(s) }

// safeURL chỉ cho link http(s) vào thẻ <a>: URL đến từ RSS/GDELT (bên ngoài), không tin được.
func safeURL(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return ""
	}
	return u.String()
}

func formatNews(n domain.EnrichedNews, prices []clickhouse.PriceChange, loc *time.Location) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s <b>Tin ảnh hưởng %s</b> · %s\n", sentimentIcon(n.Sentiment), impactLabel(n.Impact), html.EscapeString(n.Category))
	title := n.TitleEN
	if title == "" {
		title = n.Title
	}
	fmt.Fprintf(&b, "%s\n", html.EscapeString(title))
	if n.Title != "" && n.Title != title {
		fmt.Fprintf(&b, "<i>%s</i>\n", html.EscapeString(n.Title))
	}
	fmt.Fprintf(&b, "sentiment %+.2f", n.Sentiment)
	if len(n.Assets) > 0 {
		fmt.Fprintf(&b, " · %s", html.EscapeString(strings.Join(n.Assets, ", ")))
	}
	when := n.PublishedAt
	if when.IsZero() {
		when = n.ProcessedAt
	}
	fmt.Fprintf(&b, " · %s, %s\n", html.EscapeString(n.Feed), when.In(loc).Format("15:04 02/01"))
	for _, p := range prices {
		fmt.Fprintf(&b, "%s %s (1h %+.2f%%)\n", html.EscapeString(p.Symbol), formatPrice2(p.ToPrice), p.Pct())
	}
	if u := safeURL(n.URL); u != "" {
		fmt.Fprintf(&b, "<a href=\"%s\">Mở bài</a>", html.EscapeString(u))
	}
	return strings.TrimRight(b.String(), "\n")
}

func formatPrice(c clickhouse.PriceChange, window, lookback time.Duration, news []clickhouse.NewsSummary, loc *time.Location) string {
	var b strings.Builder
	icon := "📈"
	if c.Pct() < 0 {
		icon = "📉"
	}
	fmt.Fprintf(&b, "%s <b>%s %+.2f%% trong %s</b>\n", icon, html.EscapeString(c.Symbol), c.Pct(), minutes(window))
	fmt.Fprintf(&b, "%s → %s (%s–%s)\n", formatPrice2(c.FromPrice), formatPrice2(c.ToPrice),
		c.FromTime.Add(time.Minute).In(loc).Format("15:04"), c.ToTime.Add(time.Minute).In(loc).Format("15:04"))
	if len(news) == 0 {
		fmt.Fprintf(&b, "Không có tin liên quan nào trong %s trước đó.\n", minutes(lookback))
	} else {
		fmt.Fprintf(&b, "Tin liên quan trong %s trước đó:\n", minutes(lookback))
		for _, n := range news {
			fmt.Fprintf(&b, "• [%d] %s (%s, %+.2f, %s)\n", n.Impact, linkOrText(n.URL, n.Title),
				html.EscapeString(n.Category), n.Sentiment, n.PublishedAt.In(loc).Format("15:04"))
		}
	}
	b.WriteString(disclaimer)
	return b.String()
}

// linkOrText: <a> nếu URL hợp lệ, không thì chỉ tiêu đề.
func linkOrText(rawURL, title string) string {
	if u := safeURL(rawURL); u != "" {
		return fmt.Sprintf("<a href=\"%s\">%s</a>", html.EscapeString(u), html.EscapeString(title))
	}
	return html.EscapeString(title)
}

func sentimentIcon(s float64) string {
	switch {
	case s <= -0.2:
		return "🔴"
	case s >= 0.2:
		return "🟢"
	default:
		return "⚪"
	}
}

func impactLabel(impact int) string {
	switch impact {
	case 3:
		return "cao"
	case 2:
		return "vừa"
	default:
		return "thấp"
	}
}

func minutes(d time.Duration) string {
	if d%time.Hour == 0 {
		return fmt.Sprintf("%d giờ", int(d/time.Hour))
	}
	return fmt.Sprintf("%d phút", int(d/time.Minute))
}

// formatPrice2: 2 chữ số thập phân, phân cách hàng nghìn bằng dấu phẩy (60,123.45).
func formatPrice2(p float64) string {
	s := strconv.FormatFloat(math.Abs(p), 'f', 2, 64)
	intPart, frac := s[:len(s)-3], s[len(s)-3:]
	var b strings.Builder
	if p < 0 {
		b.WriteByte('-')
	}
	for k, r := range intPart {
		if k > 0 && (len(intPart)-k)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(r)
	}
	return b.String() + frac
}
