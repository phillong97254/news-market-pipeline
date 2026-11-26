package processed

import (
	"time"

	"github.com/redis/go-redis/v9"
)

type impl struct {
	client *redis.Client
	prefix string
	ttl    time.Duration
}

// New: addr rỗng thì tắt (luôn coi là chưa xử lý). Redis dùng chung với dự án khác nên mọi key có prefix,
// timeout ngắn để Redis chậm không làm chậm pipeline.
func New(addr, password, prefix string, ttl time.Duration) Store {
	if addr == "" {
		return nop{}
	}
	return &impl{
		client: redis.NewClient(&redis.Options{
			Addr:         addr,
			Password:     password,
			DialTimeout:  time.Second,
			ReadTimeout:  500 * time.Millisecond,
			WriteTimeout: 500 * time.Millisecond,
			MaxRetries:   1,
		}),
		prefix: prefix,
		ttl:    ttl,
	}
}

func (i *impl) key(id string) string { return i.prefix + id }
