package alert

import (
	"context"
	"fmt"
	"time"

	"github.com/phillong97254/news-market-pipeline/internal/gateway/telegram"
)

// Ngưỡng coi một tầng là tắc. RSS lấy 10 phút/lần nên 45 phút không có tin mới là bất thường.
const (
	tickStale       = 5 * time.Minute
	newsStale       = 45 * time.Minute
	enrichStale     = 30 * time.Minute // còn tin chờ mà nlp-worker không ghi gì
	backlogMax      = 300              // khoảng 1 giờ làm việc của nlp-worker
	clickhouseFails = 5                // số phút liên tiếp ClickHouse lỗi
	healthRemind    = 6 * time.Hour    // sự cố kéo dài thì nhắc lại
)

type problem struct {
	key, text string
}

func (i *impl) CheckHealth(ctx context.Context) error {
	now := i.now()
	var active []problem

	h, err := i.ch.PipelineHealth(ctx)
	if err != nil {
		i.mu.Lock()
		i.chFails++
		fails := i.chFails
		i.mu.Unlock()
		if fails >= clickhouseFails {
			active = append(active, problem{"clickhouse", fmt.Sprintf("ClickHouse không trả lời %d phút liền: %v", fails, err)})
		}
		// Các tầng khác không kiểm được khi ClickHouse lỗi: giữ nguyên trạng thái của chúng.
		return i.applyHealth(ctx, now, active, map[string]bool{"clickhouse": true})
	}
	i.mu.Lock()
	i.chFails = 0
	i.mu.Unlock()

	if h.LastTick.IsZero() || now.Sub(h.LastTick) > tickStale {
		active = append(active, problem{"prices", "Không có giá mới " + ago(now, h.LastTick) + " (price-producer, Coinbase hoặc Kafka engine)."})
	}
	if h.LastNews.IsZero() || now.Sub(h.LastNews) > newsStale {
		active = append(active, problem{"news", "Không có tin mới " + ago(now, h.LastNews) + " (news-producer hoặc các nguồn RSS/GDELT)."})
	}
	switch {
	case h.Backlog > 0 && (h.LastEnriched.IsZero() || now.Sub(h.LastEnriched) > enrichStale):
		active = append(active, problem{"nlp", fmt.Sprintf("nlp-worker không gán nhãn tin nào %s, còn %d tin chờ (Ollama?).", ago(now, h.LastEnriched), h.Backlog)})
	case h.Backlog > backlogMax:
		active = append(active, problem{"nlp", fmt.Sprintf("Tồn %d tin chờ gán nhãn, nlp-worker không theo kịp.", h.Backlog)})
	}
	return i.applyHealth(ctx, now, active, map[string]bool{"clickhouse": true, "prices": true, "news": true, "nlp": true})
}

// applyHealth báo sự cố mới, nhắc sự cố kéo dài và báo khi một sự cố trong checked đã hết.
func (i *impl) applyHealth(ctx context.Context, now time.Time, active []problem, checked map[string]bool) error {
	isActive := map[string]bool{}
	var msgs []string
	i.mu.Lock()
	for _, p := range active {
		isActive[p.key] = true
		since, was := i.issues[p.key]
		if !was {
			i.issues[p.key] = now
			i.issueNotified[p.key] = now
			msgs = append(msgs, "⚠️ <b>Pipeline có sự cố</b>\n"+escape(p.text))
		} else if now.Sub(i.issueNotified[p.key]) >= healthRemind {
			i.issueNotified[p.key] = now
			msgs = append(msgs, fmt.Sprintf("⚠️ <b>Sự cố vẫn còn</b> (từ %s)\n%s", since.In(i.cfg.Location).Format("15:04 02/01"), escape(p.text)))
		}
	}
	for key, since := range i.issues {
		if checked[key] && !isActive[key] {
			delete(i.issues, key)
			delete(i.issueNotified, key)
			msgs = append(msgs, fmt.Sprintf("✅ <b>Đã ổn lại</b>: %s (sự cố kéo dài %s)", issueName(key), now.Sub(since).Round(time.Minute)))
		}
	}
	i.mu.Unlock()

	// Tin về sức khoẻ pipeline không tính vào hạn mức MaxPerHour.
	for _, m := range msgs {
		if err := i.tg.Send(ctx, telegram.SendInput{Text: m}); err != nil {
			return fmt.Errorf("send health alert: %w", err)
		}
		i.logger.Info("health alert sent", "text", m)
	}
	return nil
}

func issueName(key string) string {
	switch key {
	case "prices":
		return "giá"
	case "news":
		return "thu thập tin"
	case "nlp":
		return "gán nhãn tin"
	default:
		return key
	}
}

func ago(now, t time.Time) string {
	if t.IsZero() {
		return "trong nhiều giờ"
	}
	return "trong " + now.Sub(t).Round(time.Minute).String()
}
