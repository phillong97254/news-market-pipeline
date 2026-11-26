package clickhouse

import (
	"context"
	"encoding/json"
	"strconv"
	"time"
)

// Nến của phút hiện tại chưa đóng nên bỏ; cửa sổ W phút cần W+1 nến (đầu và cuối).
const priceChangesSQL = `
SELECT symbol,
       argMin(close, minute) AS from_price,
       argMax(close, minute) AS to_price,
       min(minute)           AS from_time,
       max(minute)           AS to_time
FROM market.candles_1m
WHERE minute >= toStartOfMinute(now()) - toIntervalMinute({w:UInt32} + 1)
  AND minute <  toStartOfMinute(now())
GROUP BY symbol
ORDER BY symbol
FORMAT JSONEachRow`

func (i *impl) PriceChanges(ctx context.Context, input PriceChangesInput) ([]PriceChange, error) {
	w := max(1, int(input.Window/time.Minute))
	var out []PriceChange
	err := i.query(ctx, priceChangesSQL, map[string]string{"w": strconv.Itoa(w)}, func(dec *json.Decoder) error {
		var r priceChangeRow
		if err := dec.Decode(&r); err != nil {
			return err
		}
		out = append(out, PriceChange(r))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
