package clickhouse

import (
	"context"
	"encoding/json"
	"strconv"
)

func rangeParams(in RangeInput) map[string]string {
	return map[string]string{
		"from": strconv.FormatInt(in.From.UnixMilli(), 10),
		"to":   strconv.FormatInt(in.To.UnixMilli(), 10),
	}
}

const priceSummarySQL = `
SELECT symbol,
       argMin(open, minute) AS open,
       max(high)            AS high,
       min(low)             AS low,
       argMax(close, minute) AS close
FROM market.candles_1m
WHERE minute >= fromUnixTimestamp64Milli({from:Int64}, 'UTC')
  AND minute <  fromUnixTimestamp64Milli({to:Int64}, 'UTC')
GROUP BY symbol
ORDER BY symbol
FORMAT JSONEachRow`

type priceRangeRow struct {
	Symbol string  `json:"symbol"`
	Open   float64 `json:"open"`
	High   float64 `json:"high"`
	Low    float64 `json:"low"`
	Close  float64 `json:"close"`
}

func (i *impl) PriceSummary(ctx context.Context, input RangeInput) ([]PriceRange, error) {
	var out []PriceRange
	err := i.query(ctx, priceSummarySQL, rangeParams(input), func(dec *json.Decoder) error {
		var r priceRangeRow
		if err := dec.Decode(&r); err != nil {
			return err
		}
		out = append(out, PriceRange(r))
		return nil
	})
	return out, err
}

// Alias không trùng tên cột (relevant), nếu không countIf(relevant) sẽ tự tham chiếu.
const newsStatsSQL = `
SELECT category, count() AS n, countIf(relevant) AS n_relevant, sumIf(sentiment, relevant) AS sentiment_sum
FROM market.news_enriched FINAL
WHERE published_at >= fromUnixTimestamp64Milli({from:Int64}, 'UTC')
  AND published_at <  fromUnixTimestamp64Milli({to:Int64}, 'UTC')
GROUP BY category
ORDER BY n_relevant DESC, n DESC
FORMAT JSONEachRow`

type newsStatsRow struct {
	Category     string  `json:"category"`
	N            int     `json:"n"`
	Relevant     int     `json:"n_relevant"`
	SentimentSum float64 `json:"sentiment_sum"`
}

func (i *impl) NewsStats(ctx context.Context, input RangeInput) (NewsStats, error) {
	var st NewsStats
	var sum float64
	err := i.query(ctx, newsStatsSQL, rangeParams(input), func(dec *json.Decoder) error {
		var r newsStatsRow
		if err := dec.Decode(&r); err != nil {
			return err
		}
		st.Total += r.N
		st.Relevant += r.Relevant
		sum += r.SentimentSum
		if r.Relevant > 0 {
			st.ByCategory = append(st.ByCategory, CategoryCount{Category: r.Category, Count: r.Relevant})
		}
		return nil
	})
	if st.Relevant > 0 {
		st.AvgSentiment = sum / float64(st.Relevant)
	}
	return st, err
}

// Alias n_posts (không phải posts) để sumIf(posts) không tự tham chiếu.
// Baseline: tổng 7 ngày trước From chia cho số khoảng cùng độ dài mà social-producer thật sự có dữ liệu
// (đếm theo phút có bản ghi), để mấy ngày đầu chưa đủ dữ liệu không làm hệ số bị thổi phồng. NULL -> 0.
const socialTopicsSQL = `
WITH fromUnixTimestamp64Milli({from:Int64}, 'UTC') AS f,
     fromUnixTimestamp64Milli({to:Int64}, 'UTC')   AS t,
     greatest(1, dateDiff('second', f, t))          AS len_s
SELECT topic,
       sumIf(posts, minute >= f AND minute < t) AS n_posts,
       sumIf(posts, minute >= f - INTERVAL 7 DAY AND minute < f)
           / nullIf(uniqExactIf(minute, minute >= f - INTERVAL 7 DAY AND minute < f) * 60 / len_s, 0) AS baseline
FROM market.social_mentions FINAL
WHERE minute >= f - INTERVAL 7 DAY AND minute < t AND topic != '_all'
GROUP BY topic
ORDER BY n_posts DESC
FORMAT JSONEachRow`

type socialTopicRow struct {
	Topic    string   `json:"topic"`
	Posts    int      `json:"n_posts"`
	Baseline *float64 `json:"baseline"`
}

func (i *impl) SocialTopics(ctx context.Context, input RangeInput) ([]SocialTopic, error) {
	var out []SocialTopic
	err := i.query(ctx, socialTopicsSQL, rangeParams(input), func(dec *json.Decoder) error {
		var r socialTopicRow
		if err := dec.Decode(&r); err != nil {
			return err
		}
		t := SocialTopic{Topic: r.Topic, Posts: r.Posts}
		if r.Baseline != nil {
			t.Baseline = *r.Baseline
		}
		out = append(out, t)
		return nil
	})
	return out, err
}
