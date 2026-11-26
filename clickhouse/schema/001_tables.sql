-- Schema giai đoạn 4. Chạy lại được nhiều lần (IF NOT EXISTS / OR REPLACE).
-- Luồng: topic Redpanda ──► bảng Kafka engine (*_queue) ──► materialized view ──► bảng MergeTree.
-- Kafka engine giao at-least-once, producer cũng có thể gửi lại, nên các bảng chính dùng ReplacingMergeTree
-- và truy vấn cần FINAL (hoặc GROUP BY khoá) để không đếm trùng.

CREATE DATABASE IF NOT EXISTS market;

-- Bản ghi JSON hỏng của mọi topic (kafka_handle_error_mode='stream'), giữ 7 ngày để gỡ lỗi.
CREATE TABLE IF NOT EXISTS market.kafka_errors
(
    topic        LowCardinality(String),
    partition    UInt64,
    offset       UInt64,
    error        String,
    raw_message  String,
    seen_at      DateTime('UTC') DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (topic, seen_at)
TTL seen_at + INTERVAL 7 DAY;

-- ───────────────────────────── prices.tick ─────────────────────────────

CREATE TABLE IF NOT EXISTS market.prices_tick
(
    symbol       LowCardinality(String),
    source       LowCardinality(String),
    price        Decimal(18, 8),
    size         Decimal(18, 8),
    side         LowCardinality(String),
    trade_id     UInt64,
    event_time   DateTime64(6, 'UTC'),
    ingest_time  DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (symbol, event_time, trade_id)
TTL toDateTime(event_time) + INTERVAL 90 DAY;

-- Giữ thời gian ở dạng chuỗi rồi tự parse: Go ghi RFC3339 có nano giây.
-- Trong các MV, cột nguồn viết q.<cột> vì alias đầu ra trùng tên sẽ che mất cột gốc.
CREATE TABLE IF NOT EXISTS market.prices_tick_queue
(
    source       String,
    symbol       String,
    price        String,
    size         String,
    side         String,
    trade_id     UInt64,
    event_time   String,
    ingest_time  String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:9092',
         kafka_topic_list = 'prices.tick',
         kafka_group_name = 'clickhouse-prices',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1,
         kafka_handle_error_mode = 'stream';

CREATE MATERIALIZED VIEW IF NOT EXISTS market.prices_tick_mv TO market.prices_tick AS
SELECT
    symbol,
    source,
    toDecimal64OrZero(q.price, 8)                       AS price,
    toDecimal64OrZero(q.size, 8)                        AS size,
    side,
    trade_id,
    parseDateTime64BestEffort(q.event_time, 6, 'UTC')   AS event_time,
    parseDateTime64BestEffort(q.ingest_time, 6, 'UTC')  AS ingest_time
FROM market.prices_tick_queue AS q
WHERE length(_error) = 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS market.prices_tick_errors_mv TO market.kafka_errors AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset, _error AS error, _raw_message AS raw_message
FROM market.prices_tick_queue
WHERE length(_error) > 0;

-- Nến 1 phút, gộp dần khi ghi. Tick bị gửi trùng (hiếm) sẽ làm volume/trades lệch nhẹ; open/close/high/low không đổi.
CREATE TABLE IF NOT EXISTS market.prices_1m
(
    symbol   LowCardinality(String),
    minute   DateTime('UTC'),
    open     AggregateFunction(argMin, Decimal(18, 8), DateTime64(6, 'UTC')),
    high     SimpleAggregateFunction(max, Decimal(18, 8)),
    low      SimpleAggregateFunction(min, Decimal(18, 8)),
    close    AggregateFunction(argMax, Decimal(18, 8), DateTime64(6, 'UTC')),
    volume   SimpleAggregateFunction(sum, Decimal(38, 8)),
    trades   SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (symbol, minute)
TTL minute + INTERVAL 2 YEAR;

CREATE MATERIALIZED VIEW IF NOT EXISTS market.prices_1m_mv TO market.prices_1m AS
SELECT
    symbol,
    toStartOfMinute(toDateTime(event_time, 'UTC')) AS minute,
    argMinState(price, event_time)                 AS open,
    max(price)                                     AS high,
    min(price)                                     AS low,
    argMaxState(price, event_time)                 AS close,
    sum(size)                                      AS volume,
    count()                                        AS trades
FROM market.prices_tick
GROUP BY symbol, minute;

-- Dùng view này thay vì đọc thẳng prices_1m (cột *State phải merge).
CREATE OR REPLACE VIEW market.candles_1m AS
SELECT
    symbol,
    minute,
    toFloat64(argMinMerge(open))  AS open,
    toFloat64(max(high))          AS high,
    toFloat64(min(low))           AS low,
    toFloat64(argMaxMerge(close)) AS close,
    toFloat64(sum(volume))        AS volume,
    sum(trades)                   AS trades
FROM market.prices_1m
GROUP BY symbol, minute;

-- ───────────────────────────── news.raw ─────────────────────────────

CREATE TABLE IF NOT EXISTS market.news
(
    id            String,
    source        LowCardinality(String),
    feed          LowCardinality(String),
    url           String,
    title         String,
    summary       String,
    language      LowCardinality(String),
    domain        LowCardinality(String),
    country       LowCardinality(String),
    published_at  DateTime64(3, 'UTC'),
    ingest_time   DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree
ORDER BY id;

CREATE TABLE IF NOT EXISTS market.news_queue
(
    id            String,
    source        String,
    feed          String,
    url           String,
    title         String,
    summary       String,
    language      String,
    domain        String,
    country       String,
    published_at  String,
    ingest_time   String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:9092',
         kafka_topic_list = 'news.raw',
         kafka_group_name = 'clickhouse-news',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1,
         kafka_handle_error_mode = 'stream';

-- published_at thiếu hoặc là thời điểm 0 của Go thì lấy ingest_time.
CREATE MATERIALIZED VIEW IF NOT EXISTS market.news_mv TO market.news AS
SELECT
    id, source, feed, url, title, summary, language, domain, country,
    parseDateTime64BestEffort(q.ingest_time, 6, 'UTC') AS ingest_time,
    if(parseDateTime64BestEffortOrZero(q.published_at, 3, 'UTC') < toDateTime64('2000-01-01', 3, 'UTC'),
       parseDateTime64BestEffort(q.ingest_time, 3, 'UTC'),
       parseDateTime64BestEffortOrZero(q.published_at, 3, 'UTC')) AS published_at
FROM market.news_queue AS q
WHERE length(_error) = 0 AND id != '';

CREATE MATERIALIZED VIEW IF NOT EXISTS market.news_errors_mv TO market.kafka_errors AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset, _error AS error, _raw_message AS raw_message
FROM market.news_queue
WHERE length(_error) > 0;

-- ───────────────────────────── news.enriched ─────────────────────────────

-- Cùng id được gán nhãn lại thì giữ bản processed_at mới nhất.
CREATE TABLE IF NOT EXISTS market.news_enriched
(
    id            String,
    source        LowCardinality(String),
    feed          LowCardinality(String),
    url           String,
    title         String,
    title_en      String,
    language      LowCardinality(String),
    domain        LowCardinality(String),
    country       LowCardinality(String),
    published_at  DateTime64(3, 'UTC'),
    category      LowCardinality(String),
    relevant      Bool,
    sentiment     Float32,
    impact        UInt8,
    assets        Array(LowCardinality(String)),
    model         LowCardinality(String),
    processed_at  DateTime64(6, 'UTC'),
    latency_ms    UInt32
)
ENGINE = ReplacingMergeTree(processed_at)
ORDER BY id;

CREATE TABLE IF NOT EXISTS market.news_enriched_queue
(
    id            String,
    source        String,
    feed          String,
    url           String,
    title         String,
    title_en      String,
    language      String,
    domain        String,
    country       String,
    published_at  String,
    ingest_time   String,
    category      String,
    relevant      Bool,
    sentiment     Float64,
    impact        Int64,
    assets        Array(String),
    model         String,
    processed_at  String,
    latency_ms    Int64
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:9092',
         kafka_topic_list = 'news.enriched',
         kafka_group_name = 'clickhouse-news-enriched',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1,
         kafka_handle_error_mode = 'stream';

CREATE MATERIALIZED VIEW IF NOT EXISTS market.news_enriched_mv TO market.news_enriched AS
SELECT
    id, source, feed, url, title, title_en, language, domain, country,
    if(parseDateTime64BestEffortOrZero(q.published_at, 3, 'UTC') < toDateTime64('2000-01-01', 3, 'UTC'),
       parseDateTime64BestEffortOrZero(q.ingest_time, 3, 'UTC'),
       parseDateTime64BestEffortOrZero(q.published_at, 3, 'UTC')) AS published_at,
    category,
    relevant,
    toFloat32(q.sentiment)                               AS sentiment,
    toUInt8(greatest(0, least(3, q.impact)))             AS impact,
    assets,
    model,
    parseDateTime64BestEffort(q.processed_at, 6, 'UTC')  AS processed_at,
    toUInt32(greatest(0, q.latency_ms))                  AS latency_ms
FROM market.news_enriched_queue AS q
WHERE length(_error) = 0 AND id != '';

CREATE MATERIALIZED VIEW IF NOT EXISTS market.news_enriched_errors_mv TO market.kafka_errors AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset, _error AS error, _raw_message AS raw_message
FROM market.news_enriched_queue
WHERE length(_error) > 0;

-- ───────────────────────────── phân tích ─────────────────────────────

-- Giá phản ứng thế nào sau mỗi tin liên quan (impact >= 1): giá đóng cửa phút trước khi tin ra làm mốc,
-- so với giá đóng cửa 15 phút, 1 giờ, 4 giờ sau. Chỉ là tương quan theo thời gian, không chứng minh nhân quả.
-- Cột ret_* là NULL khi chưa đủ thời gian hoặc thiếu nến.
CREATE OR REPLACE VIEW market.news_reaction AS
SELECT
    n.id                          AS id,
    any(n.published_at)           AS published_at,
    any(n.title_en)               AS title,
    any(n.feed)                   AS feed,
    any(n.url)                    AS url,
    any(n.category)               AS category,
    any(n.sentiment)              AS sentiment,
    any(n.impact)                 AS impact,
    any(n.assets)                 AS assets,
    p.symbol                      AS symbol,
    anyIf(p.close, n.off = -1)    AS price_before,
    if(price_before = 0 OR anyIf(p.close, n.off = 15) = 0,  NULL, anyIf(p.close, n.off = 15)  / price_before - 1) AS ret_15m,
    if(price_before = 0 OR anyIf(p.close, n.off = 60) = 0,  NULL, anyIf(p.close, n.off = 60)  / price_before - 1) AS ret_1h,
    if(price_before = 0 OR anyIf(p.close, n.off = 240) = 0, NULL, anyIf(p.close, n.off = 240) / price_before - 1) AS ret_4h
FROM
(
    SELECT
        id, published_at, title_en, feed, url, category, sentiment, impact, assets,
        arrayJoin([-1, 15, 60, 240]) AS off,
        toStartOfMinute(toDateTime(published_at, 'UTC')) + toIntervalMinute(off) AS m
    FROM market.news_enriched FINAL
    WHERE relevant AND impact >= 1
) AS n
INNER JOIN market.candles_1m AS p ON p.minute = n.m
GROUP BY n.id, p.symbol;
