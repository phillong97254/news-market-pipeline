#!/usr/bin/env python3
"""Sinh grafana/dashboards/news-market.json. Sửa file này rồi chạy: python3 grafana/gen_dashboard.py

Lưu ý SQL: trong ClickHouse, alias trùng tên cột nguồn sẽ che cột đó (avg(x) AS x lồng aggregate),
nên cột kết quả đặt tên khác (r_1h thay cho ret_1h...)."""
import json
import pathlib

DS = {"type": "grafana-clickhouse-datasource", "uid": "clickhouse"}
TIMESERIES, TABLE = 0, 1  # Format enum của plugin ClickHouse v4

panels = []
_next_id = [1]


def target(sql, fmt):
    return {
        "refId": "A",
        "datasource": DS,
        "editorType": "sql",
        "rawSql": sql.strip(),
        "format": fmt,
        "queryType": "timeseries" if fmt == TIMESERIES else "table",
    }


def panel(kind, title, sql, x, y, w, h, fmt=TABLE, desc="", field=None, options=None, overrides=None, transformations=None):
    p = {
        "id": _next_id[0],
        "type": kind,
        "title": title,
        "description": desc,
        "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [target(sql, fmt)],
        "fieldConfig": {"defaults": field or {}, "overrides": overrides or []},
        "options": options or {},
    }
    if transformations:
        p["transformations"] = transformations
    _next_id[0] += 1
    panels.append(p)
    return p


def stat(title, sql, x, unit="none", decimals=0, desc=""):
    panel(
        "stat", title, sql, x, 0, 4, 4, desc=desc,
        field={"unit": unit, "decimals": decimals, "color": {"mode": "fixed", "fixedColor": "text"}},
        options={"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                 "colorMode": "none", "graphMode": "none", "textMode": "value"},
    )


# ── Hàng 1: tình trạng pipeline ──
stat("BTC-USD", "SELECT close FROM market.candles_1m WHERE symbol = 'BTC-USD' AND minute >= now() - INTERVAL 10 MINUTE ORDER BY minute DESC LIMIT 1", 0, "currencyUSD", 2)
stat("Vàng (PAXG-USD)", "SELECT close FROM market.candles_1m WHERE symbol = 'PAXG-USD' AND minute >= now() - INTERVAL 10 MINUTE ORDER BY minute DESC LIMIT 1", 4, "currencyUSD", 2)
stat("Tick 5 phút qua", "SELECT count() FROM market.prices_tick WHERE event_time >= now() - INTERVAL 5 MINUTE", 8,
     desc="0 nghĩa là price-producer hoặc Kafka engine đang dừng.")
stat("Tin mới 1 giờ qua", "SELECT uniqExact(id) FROM market.news WHERE ingest_time >= now() - INTERVAL 1 HOUR", 12)
stat("Gán nhãn 1 giờ qua", "SELECT uniqExact(id) FROM market.news_enriched WHERE processed_at >= now() - INTERVAL 1 HOUR", 16)
stat("Chờ gán nhãn", "SELECT (SELECT uniqExact(id) FROM market.news) - (SELECT uniqExact(id) FROM market.news_enriched)", 20,
     desc="Số tin có trong news.raw nhưng chưa có trong news.enriched (nlp-worker xử lý khoảng 12 giây/tin).")

# ── Hàng 2: giá, đánh dấu tin impact >= 2 ──
panel(
    "timeseries", "$symbol: giá đóng cửa 1 phút",
    """SELECT minute AS time, close AS "$symbol"
FROM market.candles_1m
WHERE symbol = '$symbol' AND $__timeFilter(minute)
ORDER BY time""",
    0, 4, 12, 9, fmt=TIMESERIES,
    desc="Đổi cặp ở biến 'Cặp' phía trên. Đường dọc là tin có impact >= 2 (bật/tắt ở mục Annotations).",
    field={"unit": "currencyUSD", "decimals": 2, "custom": {"lineWidth": 1, "fillOpacity": 0, "showPoints": "never"}},
    options={"legend": {"showLegend": False}, "tooltip": {"mode": "single"}},
)
panel(
    "timeseries", "% thay đổi từ đầu khoảng thời gian, mọi tài sản",
    """SELECT time, symbol, (close / first_value(close) OVER (PARTITION BY symbol ORDER BY time) - 1) * 100 AS pct
FROM
(
    SELECT toStartOfInterval(minute, INTERVAL greatest(1, intDiv(dateDiff('minute', $__fromTime, $__toTime), 600)) MINUTE) AS time,
           symbol, argMax(close, minute) AS close
    FROM market.candles_1m
    WHERE $__timeFilter(minute)
    GROUP BY symbol, time
)
ORDER BY time""",
    12, 4, 12, 9, fmt=TIMESERIES,
    desc="Cùng một thang % để so coin với vàng (PAXG = token 1 ounce vàng). Gộp tối đa khoảng 600 điểm mỗi đường.",
    field={"unit": "percent", "decimals": 2, "displayName": "${__field.labels.symbol}",
           "custom": {"lineWidth": 1, "fillOpacity": 0, "showPoints": "never"}},
    options={"legend": {"showLegend": True, "displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
)

# ── Hàng 3: dòng tin ──
panel(
    "timeseries", "Tin liên quan theo giờ, theo loại",
    """SELECT toStartOfHour(published_at) AS time, category, count() AS news
FROM market.news_enriched FINAL
WHERE relevant AND $__timeFilter(published_at)
GROUP BY time, category
ORDER BY time""",
    0, 13, 12, 8, fmt=TIMESERIES,
    field={"displayName": "${__field.labels.category}",
           "custom": {"drawStyle": "bars", "stacking": {"mode": "normal"}, "fillOpacity": 80, "lineWidth": 0}},
    options={"legend": {"showLegend": True, "displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
)
panel(
    "timeseries", "Sentiment trung bình theo giờ (tin liên quan)",
    """SELECT toStartOfHour(published_at) AS time,
       avg(sentiment) AS "sentiment TB",
       avg(sentiment * impact) / 3 AS "sentiment x impact"
FROM market.news_enriched FINAL
WHERE relevant AND $__timeFilter(published_at)
GROUP BY time
ORDER BY time""",
    12, 13, 12, 8, fmt=TIMESERIES,
    desc="-1 xấu cho tài sản rủi ro, 1 tốt. Nhãn do model 7B gán, chỉ xem xu hướng.",
    field={"min": -1, "max": 1, "decimals": 2, "custom": {"showPoints": "always", "lineWidth": 1}},
    options={"legend": {"showLegend": True, "displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
)

# ── Hàng 4: tương quan ──
PCT = {"unit": "percent", "decimals": 3}
panel(
    "table", "Lợi nhuận TB sau tin, theo sentiment ($symbol)",
    """SELECT '0 mọi thời điểm' AS nhom,
       count() AS mau,
       round(avg(nullIf(c15, 0) / close - 1) * 100, 4)  AS ret_15m,
       round(avg(nullIf(c60, 0) / close - 1) * 100, 4)  AS ret_1h,
       round(avg(nullIf(c240, 0) / close - 1) * 100, 4) AS ret_4h,
       round(countIf(c60 > close) / countIf(c60 > 0) * 100, 1) AS tang_1h
FROM
(
    SELECT close,
           leadInFrame(close, 15)  OVER w AS c15,
           leadInFrame(close, 60)  OVER w AS c60,
           leadInFrame(close, 240) OVER w AS c240
    FROM market.candles_1m
    WHERE symbol = '$symbol' AND $__timeFilter(minute)
    WINDOW w AS (ORDER BY minute ROWS BETWEEN CURRENT ROW AND 240 FOLLOWING)
)
UNION ALL
SELECT multiIf(sentiment <= -0.2, '1 tin xấu', sentiment >= 0.2, '3 tin tốt', '2 trung tính') AS nhom,
       count() AS mau,
       round(avg(ret_15m) * 100, 4),
       round(avg(ret_1h) * 100, 4),
       round(avg(ret_4h) * 100, 4),
       round(countIf(ret_1h > 0) / count(ret_1h) * 100, 1)
FROM market.news_reaction
WHERE symbol = '$symbol' AND $__timeFilter(published_at)
GROUP BY nhom
ORDER BY nhom""",
    0, 21, 12, 8,
    desc=("So với dòng 'mọi thời điểm' (lợi nhuận TB của mọi phút trong khoảng thời gian đang xem). "
          "Chênh lệch nhỏ hoặc ít mẫu thì không có ý nghĩa thống kê. Tương quan, không phải nhân quả, không phải khuyến nghị."),
    field={"decimals": 3},
    overrides=[
        {"matcher": {"id": "byRegexp", "options": "ret_.*|tang_1h"}, "properties": [{"id": "unit", "value": "percent"}]},
        {"matcher": {"id": "byName", "options": "tang_1h"}, "properties": [{"id": "displayName", "value": "% lần tăng sau 1h"}, {"id": "decimals", "value": 1}]},
        {"matcher": {"id": "byName", "options": "mau"}, "properties": [{"id": "displayName", "value": "số mẫu"}, {"id": "decimals", "value": 0}]},
        {"matcher": {"id": "byName", "options": "nhom"}, "properties": [{"id": "displayName", "value": "nhóm"}]},
    ],
    options={"showHeader": True, "cellHeight": "sm"},
)
panel(
    "table", "Lợi nhuận TB sau tin, theo loại tin ($symbol)",
    """SELECT category,
       count() AS mau,
       round(avg(sentiment), 2) AS sentiment_tb,
       round(avg(ret_15m) * 100, 4) AS r_15m,
       round(avg(ret_1h) * 100, 4) AS r_1h,
       round(avg(ret_4h) * 100, 4) AS r_4h,
       round(avg(abs(ret_1h)) * 100, 4) AS bien_dong_1h
FROM market.news_reaction
WHERE symbol = '$symbol' AND $__timeFilter(published_at)
GROUP BY category
ORDER BY mau DESC""",
    12, 21, 12, 8,
    desc="bien_dong_1h = trung bình |lợi nhuận 1h|: loại tin nào đi kèm giá dao động mạnh, bất kể chiều.",
    field={"decimals": 3},
    overrides=[
        {"matcher": {"id": "byRegexp", "options": "r_.*|bien_dong_1h"}, "properties": [{"id": "unit", "value": "percent"}]},
        {"matcher": {"id": "byName", "options": "bien_dong_1h"}, "properties": [{"id": "displayName", "value": "|biến động 1h|"}]},
        {"matcher": {"id": "byName", "options": "mau"}, "properties": [{"id": "displayName", "value": "số mẫu"}, {"id": "decimals", "value": 0}]},
        {"matcher": {"id": "byName", "options": "sentiment_tb"}, "properties": [{"id": "decimals", "value": 2}, {"id": "displayName", "value": "sentiment TB"}]},
    ],
    options={"showHeader": True, "cellHeight": "sm"},
)

LINK_TITLE = {"matcher": {"id": "byName", "options": "title"},
              "properties": [{"id": "links", "value": [{"title": "Mở bài báo", "url": "${__data.fields.url}", "targetBlank": True}]},
                             {"id": "custom.minWidth", "value": 380}]}
HIDE_URL = [{"id": "organize", "options": {"excludeByName": {"url": True}}}]

# ── Hàng 5: từng tin ──
panel(
    "table", "Giá sau từng tin liên quan ($symbol)",
    """SELECT published_at, feed, category, sentiment, impact, title,
       round(ret_15m * 100, 3) AS r_15m, round(ret_1h * 100, 3) AS r_1h, round(ret_4h * 100, 3) AS r_4h, url
FROM market.news_reaction
WHERE symbol = '$symbol' AND $__timeFilter(published_at)
ORDER BY published_at DESC
LIMIT 200""",
    0, 29, 24, 10,
    desc="Mốc là giá đóng cửa phút trước khi tin ra. Ô trống: chưa đủ thời gian. Bấm tiêu đề để mở bài.",
    field={"custom": {"align": "auto"}},
    overrides=[
        LINK_TITLE,
        {"matcher": {"id": "byRegexp", "options": "r_.*"},
         "properties": [{"id": "unit", "value": "percent"}, {"id": "decimals", "value": 3},
                        {"id": "custom.cellOptions", "value": {"type": "color-text"}},
                        {"id": "color", "value": {"mode": "thresholds"}},
                        {"id": "thresholds", "value": {"mode": "absolute", "steps": [{"color": "red", "value": None}, {"color": "green", "value": 0}]}}]},
        {"matcher": {"id": "byName", "options": "sentiment"}, "properties": [{"id": "decimals", "value": 2}]},
    ],
    options={"showHeader": True, "cellHeight": "sm"},
    transformations=HIDE_URL,
)
panel(
    "table", "Tin mới gán nhãn",
    """SELECT published_at, feed, relevant, category, sentiment, impact,
       arrayStringConcat(assets, ', ') AS assets, title_en AS title, latency_ms, url
FROM market.news_enriched FINAL
WHERE $__timeFilter(published_at)
ORDER BY published_at DESC
LIMIT 200""",
    0, 39, 24, 10,
    field={"custom": {"align": "auto"}},
    overrides=[LINK_TITLE,
               {"matcher": {"id": "byName", "options": "sentiment"}, "properties": [{"id": "decimals", "value": 2}]},
               {"matcher": {"id": "byName", "options": "latency_ms"}, "properties": [{"id": "unit", "value": "ms"}]}],
    options={"showHeader": True, "cellHeight": "sm"},
    transformations=HIDE_URL,
)

# ── Hàng 6: Bluesky ──
SOCIAL_FIELD = {"displayName": "${__field.labels.topic}",
                "custom": {"lineWidth": 1, "fillOpacity": 0, "showPoints": "never"}}
SOCIAL_OPTS = {"legend": {"showLegend": True, "displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi", "sort": "desc"}}
panel(
    "timeseries", "Bluesky: số bài nhắc tới chủ đề, mỗi giờ",
    """SELECT toStartOfHour(minute) AS time, topic, sum(posts) AS posts
FROM market.social_mentions FINAL
WHERE topic != '_all' AND $__timeFilter(minute)
GROUP BY time, topic
ORDER BY time""",
    0, 49, 12, 9, fmt=TIMESERIES,
    desc="Đếm theo từ khoá trên toàn bộ bài công khai mới (Jetstream). Không lưu nội dung bài. 'eth', 'btc' khớp nguyên từ.",
    field=SOCIAL_FIELD, options=SOCIAL_OPTS,
)
panel(
    "timeseries", "Bluesky: số bài trên mỗi 10.000 bài, mỗi giờ",
    """SELECT time, topic, posts * 10000 / nullIf(total, 0) AS per_10k
FROM
(
    SELECT toStartOfHour(minute) AS time, topic, sum(posts) AS posts
    FROM market.social_mentions FINAL
    WHERE topic != '_all' AND $__timeFilter(minute)
    GROUP BY time, topic
) AS t
INNER JOIN
(
    SELECT toStartOfHour(minute) AS time, sum(posts) AS total
    FROM market.social_mentions FINAL
    WHERE topic = '_all' AND $__timeFilter(minute)
    GROUP BY time
) AS a USING time
ORDER BY time""",
    12, 49, 12, 9, fmt=TIMESERIES,
    desc="Chia cho tổng số bài để bỏ ảnh hưởng của giờ cao điểm/thấp điểm trên Bluesky.",
    field={**SOCIAL_FIELD, "decimals": 1}, options=SOCIAL_OPTS,
)

dashboard = {
    "uid": "news-market",
    "title": "News x Market",
    "description": "Tin chính trị/vĩ mô và giá coin. Phân tích tương quan, không phải dự đoán hay khuyến nghị đầu tư.",
    "tags": ["news-market-pipeline"],
    "timezone": "browser",
    "schemaVersion": 39,
    "version": 1,
    "editable": False,
    "refresh": "1m",
    "time": {"from": "now-24h", "to": "now"},
    "templating": {"list": [{
        "name": "symbol", "label": "Cặp", "type": "query", "datasource": DS,
        "query": "SELECT DISTINCT symbol FROM market.prices_1m ORDER BY symbol",
        "definition": "SELECT DISTINCT symbol FROM market.prices_1m ORDER BY symbol",
        "refresh": 1,  # lấy lại danh sách khi mở dashboard: thêm cặp mới không cần sửa file này
        "current": {"text": "BTC-USD", "value": "BTC-USD"},
    }]},
    "annotations": {"list": [
        {"builtIn": 1, "datasource": {"type": "grafana", "uid": "-- Grafana --"}, "enable": True, "hide": True,
         "iconColor": "rgba(0, 211, 255, 1)", "name": "Annotations & Alerts", "type": "dashboard"},
        {"name": "Tin impact >= 2", "enable": True, "iconColor": "orange", "datasource": DS,
         "target": target("""SELECT published_at AS time,
       concat(title_en, ' [', category, ', sentiment ', toString(round(sentiment, 2)), ']') AS text,
       arrayStringConcat(arrayConcat([category], assets), ',') AS tags
FROM market.news_enriched FINAL
WHERE relevant AND impact >= 2 AND $__timeFilter(published_at)
ORDER BY time""", TABLE)},
    ]},
    "panels": panels,
}

out = pathlib.Path(__file__).parent / "dashboards" / "news-market.json"
out.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2) + "\n")
print(f"wrote {out} ({len(panels)} panels)")
