package clickhouse

import (
	"context"
	"encoding/json"
	"strconv"
)

const recentNewsSQL = `
SELECT id, published_at, if(title_en != '', title_en, title) AS title, category, sentiment, impact, feed, url
FROM market.news_enriched FINAL
WHERE relevant
  AND impact >= {min_impact:UInt8}
  AND published_at >= fromUnixTimestamp64Milli({from:Int64}, 'UTC')
  AND published_at <  fromUnixTimestamp64Milli({to:Int64}, 'UTC')
ORDER BY impact DESC, published_at DESC
LIMIT {limit:UInt32}
FORMAT JSONEachRow`

func (i *impl) RecentNews(ctx context.Context, input RecentNewsInput) ([]NewsSummary, error) {
	params := map[string]string{
		"min_impact": strconv.Itoa(max(0, input.MinImpact)),
		"from":       strconv.FormatInt(input.From.UnixMilli(), 10),
		"to":         strconv.FormatInt(input.To.UnixMilli(), 10),
		"limit":      strconv.Itoa(max(1, input.Limit)),
	}
	var out []NewsSummary
	err := i.query(ctx, recentNewsSQL, params, func(dec *json.Decoder) error {
		var r newsRow
		if err := dec.Decode(&r); err != nil {
			return err
		}
		out = append(out, NewsSummary(r))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
