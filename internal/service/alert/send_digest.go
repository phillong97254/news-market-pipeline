package alert

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/phillong97254/news-market-pipeline/internal/gateway/clickhouse"
	"github.com/phillong97254/news-market-pipeline/internal/gateway/telegram"
)

// MaybeSendDigest gửi tóm tắt 24 giờ một lần mỗi ngày, ngay khi giờ địa phương qua DigestAt.
// Ngày đã gửi được nhớ trong Redis (key alert:news:digest:<ngày>) nên khởi động lại không gửi lại.
func (i *impl) MaybeSendDigest(ctx context.Context) error {
	if i.cfg.DigestAt < 0 {
		return nil
	}
	now := i.now().In(i.cfg.Location)
	at := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, i.cfg.Location).Add(i.cfg.DigestAt)
	if now.Before(at) || now.Sub(at) > 3*time.Hour { // quá 3 giờ (máy tắt cả tối) thì bỏ ngày đó
		return nil
	}
	key := "digest:" + at.Format("2006-01-02")
	i.mu.Lock()
	sent := i.digestSent == key
	i.mu.Unlock()
	if sent {
		return nil
	}
	if done, err := i.sent.Done(ctx, key); err == nil && done {
		i.mu.Lock()
		i.digestSent = key
		i.mu.Unlock()
		return nil
	}

	text := i.buildDigest(ctx, at.Add(-24*time.Hour), at)
	if err := i.tg.Send(ctx, telegram.SendInput{Text: text}); err != nil {
		return fmt.Errorf("send digest: %w", err)
	}
	i.mu.Lock()
	i.digestSent = key
	i.mu.Unlock()
	if err := i.sent.MarkDone(ctx, key); err != nil {
		i.logger.Warn("digest mark failed", "err", err)
	}
	i.logger.Info("digest sent", "day", key)
	return nil
}

// buildDigest: phần nào lỗi thì bỏ phần đó, vẫn gửi phần còn lại.
func (i *impl) buildDigest(ctx context.Context, from, to time.Time) string {
	loc := i.cfg.Location
	r := clickhouse.RangeInput{From: from, To: to}
	var b strings.Builder
	fmt.Fprintf(&b, "🗞 <b>Tóm tắt 24 giờ</b> (đến %s)\n", to.In(loc).Format("15:04 02/01"))

	if prices, err := i.ch.PriceSummary(ctx, r); err != nil {
		i.logger.Warn("digest prices", "err", err)
	} else if len(prices) > 0 {
		b.WriteString("\n<b>Giá</b>\n")
		for _, p := range prices {
			fmt.Fprintf(&b, "%s %s (%+.2f%%, thấp %s, cao %s)\n", escape(p.Symbol), formatPrice2(p.Close), p.Pct(),
				formatPrice2(p.Low), formatPrice2(p.High))
		}
	}

	if st, err := i.ch.NewsStats(ctx, r); err != nil {
		i.logger.Warn("digest news stats", "err", err)
	} else {
		fmt.Fprintf(&b, "\n<b>Tin</b>: %d tin, %d liên quan, sentiment TB %+.2f\n", st.Total, st.Relevant, st.AvgSentiment)
		var parts []string
		for k, c := range st.ByCategory {
			if k == 4 {
				break
			}
			parts = append(parts, fmt.Sprintf("%s %d", escape(c.Category), c.Count))
		}
		if len(parts) > 0 {
			fmt.Fprintf(&b, "Nhiều nhất: %s\n", strings.Join(parts, ", "))
		}
	}

	if news, err := i.ch.RecentNews(ctx, clickhouse.RecentNewsInput{From: from, To: to, MinImpact: 2, Limit: 5}); err != nil {
		i.logger.Warn("digest top news", "err", err)
	} else if len(news) > 0 {
		b.WriteString("\n<b>Tin đáng chú ý</b>\n")
		for _, n := range news {
			fmt.Fprintf(&b, "• [%d] <a href=\"%s\">%s</a> (%s, %+.2f)\n", n.Impact, escape(n.URL), escape(n.Title), escape(n.Category), n.Sentiment)
		}
	}

	if topics, err := i.ch.SocialTopics(ctx, r); err != nil {
		i.logger.Warn("digest social", "err", err)
	} else if len(topics) > 0 && topics[0].Posts > 0 {
		b.WriteString("\n<b>Bluesky</b> (số bài, so với TB 7 ngày)\n")
		var parts []string
		for _, t := range topics {
			if t.Posts == 0 {
				continue
			}
			s := fmt.Sprintf("%s %s", escape(t.Topic), formatInt(t.Posts))
			if t.Baseline > 0 {
				s += fmt.Sprintf(" (x%.1f)", float64(t.Posts)/t.Baseline)
			}
			parts = append(parts, s)
		}
		b.WriteString(strings.Join(parts, " · ") + "\n")
	}

	b.WriteString("\n" + disclaimer)
	return b.String()
}

func formatInt(n int) string {
	s := formatPrice2(float64(n))
	return s[:len(s)-3]
}
