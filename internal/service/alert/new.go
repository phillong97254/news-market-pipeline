package alert

import (
	"log/slog"
	"sync"
	"time"

	"github.com/phillong97254/news-market-pipeline/internal/cache/processed"
	"github.com/phillong97254/news-market-pipeline/internal/gateway/clickhouse"
	"github.com/phillong97254/news-market-pipeline/internal/gateway/telegram"
)

type impl struct {
	logger *slog.Logger
	tg     telegram.Gateway
	ch     clickhouse.Gateway
	sent   processed.Store // id tin đã báo (Redis), để khởi động lại không báo lại
	cfg    Config
	now    func() time.Time

	mu        sync.Mutex
	lastPrice map[string]time.Time // lần cuối báo giá của mỗi cặp (trong RAM: khởi động lại thì có thể báo lại 1 lần)
	sentAt    []time.Time          // thời điểm các tin nhắn trong 1 giờ qua
}

func New(logger *slog.Logger, tg telegram.Gateway, ch clickhouse.Gateway, sent processed.Store, cfg Config) Service {
	return &impl{
		logger:    logger.With("service", "alert"),
		tg:        tg,
		ch:        ch,
		sent:      sent,
		cfg:       cfg.withDefaults(),
		now:       time.Now,
		lastPrice: map[string]time.Time{},
	}
}

// allowed: còn chỗ trong hạn mức MaxPerHour không.
func (i *impl) allowed() bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	cutoff := i.now().Add(-time.Hour)
	keep := i.sentAt[:0]
	for _, t := range i.sentAt {
		if t.After(cutoff) {
			keep = append(keep, t)
		}
	}
	i.sentAt = keep
	return len(i.sentAt) < i.cfg.MaxPerHour
}

func (i *impl) recordSent() {
	i.mu.Lock()
	i.sentAt = append(i.sentAt, i.now())
	i.mu.Unlock()
}
