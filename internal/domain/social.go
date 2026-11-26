package domain

import "time"

// SocialMention: số bài mạng xã hội nhắc tới một chủ đề trong 1 phút. Không lưu nội dung hay tác giả bài.
// Topic "_all" là tổng số bài đọc được trong phút đó (để tính tỉ lệ).
type SocialMention struct {
	Source     string    `json:"source"` // bluesky
	Topic      string    `json:"topic"`
	Minute     time.Time `json:"minute"`
	Posts      int       `json:"posts"`
	IngestTime time.Time `json:"ingest_time"`
}

const SocialAllTopic = "_all"
