package news

import (
	"sync"
	"time"
)

// seenSet nhớ ID đã gửi, dùng chung giữa các poller (cùng bài từ GDELT và RSS chỉ gửi 1 lần).
// Chỉ nằm trong RAM: khởi động lại có thể gửi lại vài bài, tầng sau (ClickHouse) loại trùng theo ID.
type seenSet struct {
	mu  sync.Mutex
	ids map[string]time.Time
}

func newSeenSet() *seenSet { return &seenSet{ids: map[string]time.Time{}} }

func (s *seenSet) has(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.ids[id]
	return ok
}

// add trả false nếu ID đã có (một poller khác vừa gửi).
func (s *seenSet) add(id string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.ids[id]; ok {
		return false
	}
	s.ids[id] = now
	return true
}

func (s *seenSet) remove(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.ids, id)
}

func (s *seenSet) prune(before time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for id, t := range s.ids {
		if t.Before(before) {
			delete(s.ids, id)
			n++
		}
	}
	return n
}

func (s *seenSet) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.ids)
}
