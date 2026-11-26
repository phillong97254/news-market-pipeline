package clickhouse

import (
	"context"
	"encoding/json"
	"time"
)

// Mỗi max() trên bảng rỗng trả 1970-01-01, coi như chưa từng có dữ liệu.
const pipelineHealthSQL = `
SELECT
    (SELECT max(event_time)   FROM market.prices_tick   WHERE event_time   > now() - INTERVAL 1 DAY) AS last_tick,
    (SELECT max(ingest_time)  FROM market.news          WHERE ingest_time  > now() - INTERVAL 7 DAY) AS last_news,
    (SELECT max(processed_at) FROM market.news_enriched WHERE processed_at > now() - INTERVAL 7 DAY) AS last_enriched,
    (SELECT count() FROM
        (SELECT DISTINCT id FROM market.news WHERE ingest_time > now() - INTERVAL 1 DAY)
        WHERE id NOT IN (SELECT id FROM market.news_enriched WHERE processed_at > now() - INTERVAL 8 DAY)) AS backlog
FORMAT JSONEachRow`

type healthRow struct {
	LastTick     time.Time `json:"last_tick"`
	LastNews     time.Time `json:"last_news"`
	LastEnriched time.Time `json:"last_enriched"`
	Backlog      int       `json:"backlog"`
}

func (i *impl) PipelineHealth(ctx context.Context) (Health, error) {
	var h Health
	err := i.query(ctx, pipelineHealthSQL, nil, func(dec *json.Decoder) error {
		var r healthRow
		if err := dec.Decode(&r); err != nil {
			return err
		}
		h = Health{LastTick: zeroEpoch(r.LastTick), LastNews: zeroEpoch(r.LastNews), LastEnriched: zeroEpoch(r.LastEnriched), Backlog: r.Backlog}
		return nil
	})
	return h, err
}

func zeroEpoch(t time.Time) time.Time {
	if t.Unix() <= 0 {
		return time.Time{}
	}
	return t
}
