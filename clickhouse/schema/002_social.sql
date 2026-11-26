-- Bluesky: số bài Bluesky theo chủ đề mỗi phút (topic social.mentions).
-- Khởi động lại social-producer thì phút đang dở được ghi 2 lần (2 phần); ReplacingMergeTree(posts) giữ phần lớn hơn,
-- nên con số của phút đó có thể hụt một ít, nhưng không bao giờ đếm trùng.

CREATE TABLE IF NOT EXISTS market.social_mentions
(
    source       LowCardinality(String),
    topic        LowCardinality(String),
    minute       DateTime('UTC'),
    posts        UInt32,
    ingest_time  DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(posts)
PARTITION BY toYYYYMM(minute)
ORDER BY (source, topic, minute)
TTL minute + INTERVAL 1 YEAR;

CREATE TABLE IF NOT EXISTS market.social_mentions_queue
(
    source       String,
    topic        String,
    minute       String,
    posts        UInt32,
    ingest_time  String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:9092',
         kafka_topic_list = 'social.mentions',
         kafka_group_name = 'clickhouse-social',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1,
         kafka_handle_error_mode = 'stream';

CREATE MATERIALIZED VIEW IF NOT EXISTS market.social_mentions_mv TO market.social_mentions AS
SELECT
    source,
    topic,
    toDateTime(parseDateTime64BestEffort(q.minute, 0, 'UTC'), 'UTC') AS minute,
    posts,
    parseDateTime64BestEffort(q.ingest_time, 6, 'UTC')            AS ingest_time
FROM market.social_mentions_queue AS q
WHERE length(_error) = 0 AND topic != '';

CREATE MATERIALIZED VIEW IF NOT EXISTS market.social_mentions_errors_mv TO market.kafka_errors AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset, _error AS error, _raw_message AS raw_message
FROM market.social_mentions_queue
WHERE length(_error) > 0;
