# news-market-pipeline

Pipeline thời gian thực: tin chính trị và biến động giá coin/chứng khoán.
Mục tiêu là phân tích tương quan, **không** phải dự đoán giá hay khuyến nghị đầu tư.

```
Coinbase WebSocket ──► price-producer (Go) ──► Redpanda: prices.tick            ← Giai đoạn 1 (xong)
GDELT + RSS ──► news-producer (Go) ──► Redpanda: news.raw                       ← Giai đoạn 2 (xong)
news.raw ──► nlp-worker (Go + Ollama qwen2.5:7b) ──► news.enriched             ← Giai đoạn 3 (xong)
3 topic ──► ClickHouse (Kafka engine + materialized view) ──► Grafana            ← Giai đoạn 4 (xong)
news.enriched + ClickHouse ──► alerter (Go) ──► Telegram                         ← Giai đoạn 5 (xong)
```

## Chạy
Code nằm ở Kali, chạy trên Oracle (arm64, build tại chỗ):
```bash
./deploy.sh                  # đồng bộ + build + chạy tất cả
./deploy.sh news-producer    # chỉ build lại 1 service
```

Lần đầu, tạo mật khẩu ClickHouse/Grafana trên Oracle (file `.env` không nằm trong repo, `deploy.sh` không ghi đè nó):
```bash
ssh oracle 'cd ~/news-market-pipeline && umask 077 && printf "CLICKHOUSE_PASSWORD=%s\nCH_GRAFANA_PASSWORD=%s\nGF_ADMIN_PASSWORD=%s\n" $(openssl rand -hex 24) $(openssl rand -hex 24) $(openssl rand -hex 16) > .env'
```
Tên biến xem ở `.env.example`.

Mọi cổng chỉ nghe trên `127.0.0.1` của Oracle. Xem từ Kali qua SSH tunnel:
```bash
ssh -N -L 8090:127.0.0.1:8090 -L 3000:127.0.0.1:3000 oracle   # Console: http://localhost:8090, Grafana: http://localhost:3000
```
Đăng nhập Grafana bằng user `admin`, mật khẩu là `GF_ADMIN_PASSWORD` trong `.env` trên Oracle.

Kiểm tra nhanh trên Oracle:
```bash
docker exec redpanda rpk topic describe news.raw -p        # số bản ghi mỗi partition
docker exec redpanda rpk topic consume news.raw -n 5       # xem 5 bản ghi
docker logs -f news-producer                               # mỗi lần lấy: fetched/new/dup/too_old
```

## Topic `prices.tick`
- Key: symbol (`BTC-USD`, `ETH-USD`, `PAXG-USD`), nên mỗi cặp giữ đúng thứ tự trong 1 partition.
- `PAXG-USD` là token 1 ounce vàng trên Coinbase, dùng làm giá vàng (24/7, không cần key). Giao dịch thưa
  (vài lệnh mỗi phút), nên nến 1 phút có chỗ trống và `news_reaction` có thể ra NULL cho vàng.
- Giữ 7 ngày hoặc tối đa 2GB.
- Value (JSON): `source, symbol, price, size, side, trade_id, event_time, ingest_time`.
  `price`/`size` là chuỗi thập phân để không mất độ chính xác.

## Topic `news.raw`
- Key: `id` = băm URL đã chuẩn hoá (bỏ `utm_*`, `at_*`, fragment). Cùng bài từ GDELT và RSS có cùng id.
- Giữ 30 ngày.
- Value (JSON): `id, source, feed, url, title, summary, language, domain, country, published_at, ingest_time`.
- Chỉ lưu tiêu đề, tóm tắt ngắn (tối đa 500 ký tự) và link, không lưu nội dung bài báo.
- Bỏ bài cũ hơn 48 giờ.
- Lọc trùng nằm trong RAM: khởi động lại producer sẽ gửi lại các bài còn trong feed.
  Tầng sau (ClickHouse) phải loại trùng theo `id`.

| Nguồn | Chu kỳ | Ghi chú |
|---|---|---|
| GDELT DOC API | 15 phút, nhìn lại 1 giờ | Giới hạn 1 request/5 giây, bị 429 thì đợi lần sau. Bắt tay TLS chậm (~10s) |
| BBC World, BBC Business | 10 phút | Điều khoản dùng lại metadata/RSS của BBC |
| CoinDesk | 10 phút | |
| VnExpress Thế giới, Kinh doanh | 10 phút | Feed không ghi ngôn ngữ; đôi lúc trả 503 |

Đổi nguồn qua env `RSS_FEEDS="tên=url,tên=url"`, `GDELT_QUERY` (đặt `off` để tắt), `GDELT_EVERY`, `RSS_EVERY`.
Feed dùng conditional GET (ETag / Last-Modified) để không tải lại khi chưa đổi.

## Topic `news.enriched` (giai đoạn 3)
`nlp-worker` đọc `news.raw` (consumer group `nlp-worker`), gọi Ollama trên chính Oracle, ghi kết quả vào `news.enriched`
(key = `id`, giữ 90 ngày). Value là mọi trường của `news.raw` cộng thêm:

| Trường | Ý nghĩa |
|---|---|
| `title_en` | tiêu đề tiếng Anh (dịch nếu cần) |
| `category` | trade, monetary_policy, fiscal, election, geopolitics, sanctions, crypto_regulation, corporate, other |
| `relevant` | thị trường có thể phản ứng với tin này không |
| `sentiment` | -1 (xấu cho tài sản rủi ro) đến 1 (tốt) |
| `impact` | 0 không, 1 thấp, 2 vừa, 3 cao |
| `assets` | BTC, ETH, CRYPTO, US_STOCKS, VN_STOCKS, USD, GOLD, OIL |
| `model`, `processed_at`, `latency_ms` | model dùng, thời điểm và thời gian xử lý |

- Tốc độ: khoảng 12 giây/tin trên CPU ARM (khoảng 300 tin/giờ). Lần đầu nạp model mất thêm khoảng 50 giây.
- At-least-once: chỉ commit offset sau khi broker xác nhận đã ghi `news.enriched`. Ollama chết thì worker
  dừng lại chờ (backoff 2s đến 1 phút), không bỏ tin. Model trả sai schema 3 lần liên tiếp thì bỏ tin đó.
- Bỏ qua tin đã gán nhãn: sau khi ghi thành công, worker đặt key `nlp:done:<id>` (giữ 30 ngày) vào Redis dùng chung
  (`~/infra/redis`). Mật khẩu đọc từ `../infra/redis/.env` qua `env_file`, không nằm trong repo.
  Redis lỗi thì chỉ log và xử lý bình thường (tệ nhất là hỏi LLM lại một tin).
- Schema giới hạn `assets` tối đa 4 phần tử và câu trả lời tối đa 200 token: không có giới hạn này, model có lúc lặp
  mãi một phần tử và kẹt hàng phút. Thử lại sau đầu ra hỏng thì đổi seed và tăng nhẹ temperature.
- Container chạy `network_mode: host` để gọi Ollama ở `127.0.0.1:11434` và Redis ở `127.0.0.1:6379`; không mở cổng nào.
- Nhãn do model 7B gán, có sai sót (ví dụ gắn `USD` cho tin doanh nghiệp Anh). Dùng để thống kê xu hướng, không dùng từng nhãn riêng lẻ.

Xem độ trễ xử lý (lag) và kết quả:
```bash
docker exec redpanda rpk group describe nlp-worker
docker logs -f nlp-worker
```

## ClickHouse và Grafana (giai đoạn 4)
ClickHouse tự đọc 3 topic bằng bảng Kafka engine (`*_queue`, consumer group `clickhouse-*`); materialized view chuyển
sang bảng lưu trữ. Không có service Go nào ở giữa. Schema ở `clickhouse/schema/`, service `clickhouse-schema` áp nó mỗi lần deploy
(chạy lại không sao). Consumer group mới đọc từ đầu topic, nên bảng có lại mọi dữ liệu Redpanda còn giữ.

| Bảng / view (database `market`) | Nội dung |
|---|---|
| `prices_tick` | mọi tick, giữ 90 ngày; `ReplacingMergeTree` theo `(symbol, event_time, trade_id)` |
| `prices_1m` → view `candles_1m` | nến 1 phút (open/high/low/close/volume/trades), giữ 2 năm. Đọc qua `candles_1m` |
| `news` | `news.raw`, mỗi `id` một dòng sau khi merge |
| `news_enriched` | `news.enriched`, giữ bản `processed_at` mới nhất của mỗi `id` |
| view `news_reaction` | mỗi tin liên quan (impact ≥ 1) × mỗi cặp: giá đóng cửa phút trước tin, lợi nhuận sau 15 phút / 1 giờ / 4 giờ |
| `kafka_errors` | bản ghi JSON hỏng của mọi topic, giữ 7 ngày |

- Kafka engine giao at-least-once và producer có thể gửi lại, nên bảng chính là `ReplacingMergeTree`: dòng trùng chỉ biến mất
  khi merge chạy. Đếm thì dùng `FINAL` hoặc `uniqExact(id)`.
- Nến 1 phút tính lúc ghi: tick trùng (hiếm) làm `volume`/`trades` lệch nhẹ, không ảnh hưởng giá.
- Trong materialized view, cột nguồn viết `q.<cột>` vì alias trùng tên che mất cột gốc. Dashboard cũng tránh đặt alias trùng tên cột.
- Đổi cột của bảng đã có thì `CREATE ... IF NOT EXISTS` không áp lại: cần `ALTER` hoặc xoá và tạo lại view/bảng.
- RAM: ClickHouse khoảng 250MB (giới hạn 3GB), Grafana khoảng 120MB (giới hạn 512MB). Đĩa: khoảng 35MB cho mỗi 2 triệu tick.

Grafana (plugin `grafana-clickhouse-datasource`, user ClickHouse chỉ đọc `grafana`) có sẵn dashboard **News x Market**:
giá BTC/ETH có vạch đánh dấu tin impact ≥ 2, số tin theo loại, sentiment theo giờ, lợi nhuận trung bình sau tin theo sentiment
và theo loại tin (so với lợi nhuận trung bình của mọi thời điểm), bảng từng tin. Dashboard sinh từ `grafana/gen_dashboard.py`:
sửa file đó rồi chạy `python3 grafana/gen_dashboard.py && ./deploy.sh grafana`.

Đây là tương quan theo thời gian trên ít mẫu và nhãn của model 7B, không chứng minh tin làm giá chạy, không phải khuyến nghị đầu tư.

Truy vấn tay trên Oracle:
```bash
docker exec -it clickhouse sh -c 'clickhouse-client -u pipeline --password "$CLICKHOUSE_PASSWORD"'
# SELECT * FROM market.news_reaction WHERE symbol = 'BTC-USD' ORDER BY published_at DESC LIMIT 10;
```

## Cảnh báo Telegram (giai đoạn 5)
`alerter` gửi qua bot riêng cho cảnh báo (tách khỏi bot Vietlott). Token và chat id nằm trong `.env.alerter` trên Oracle
(`TG_TOKEN`, `TG_CHAT_ID`, quyền 600) qua `env_file`, không nằm trong repo, `deploy.sh` không ghi đè, và không bao giờ được log (lỗi HTTP đã bỏ URL chứa token).

| Loại | Khi nào | Kèm theo |
|---|---|---|
| Tin | bản ghi `news.enriched` liên quan, `impact >= ALERT_MIN_IMPACT` (mặc định 3, khoảng 1-2 tin/giờ) | giá BTC/ETH hiện tại và % thay đổi 1 giờ |
| Giá | mỗi phút: giá đóng cửa đổi `>= PRICE_MOVE_PCT` (1,5%) trong `PRICE_WINDOW` (15 phút) | tối đa 3 tin liên quan trong 1 giờ trước đó |

- Tin cũ hơn `ALERT_MAX_AGE` (6 giờ) thì bỏ, nên lần đầu chạy (consumer group `alerter` đọc từ đầu topic) không báo lại tồn đọng.
- Không báo trùng: key `alert:news:<id>` trong Redis dùng chung, giữ 7 ngày. Cảnh báo giá mỗi cặp nghỉ `PRICE_COOLDOWN` (1 giờ),
  nhớ trong RAM nên khởi động lại có thể báo lại 1 lần.
- Tối đa `ALERT_MAX_PER_HOUR` (10) tin nhắn mỗi giờ; vượt thì bỏ và log cảnh báo.
- Telegram tạm lỗi (mạng, 5xx, 429) thì consumer chờ rồi gửi lại; Telegram từ chối (sai chat, bị chặn) thì bỏ tin đó.
- Ngưỡng 1,5%/15 phút xấp xỉ phân vị 99 của BTC trong dữ liệu ngày đầu; đổi trong `docker-compose.yml` rồi `./deploy.sh alerter`.
- Giờ hiển thị theo `ALERT_UTC_OFFSET` (7 = giờ Việt Nam).
- Đọc ClickHouse bằng user chỉ đọc `grafana`.

```bash
docker logs -f alerter     # news alert sent / price alert sent
```

## Cấu trúc
```
cmd/price-producer/          giá coin
cmd/news-producer/           tin tức: tạo poller cho GDELT và từng feed RSS
cmd/nlp-worker/              gán nhãn tin bằng Ollama
cmd/alerter/                 cảnh báo Telegram: tin impact cao, giá biến động mạnh
internal/domain/             Tick, NewsItem, NewsAnalysis/EnrichedNews, NewsID/CanonicalURL
internal/gateway/coinbase/   WebSocket Coinbase, read timeout 30s
internal/gateway/gdelt/      GDELT DOC 2.0 (artlist JSON)
internal/gateway/rss/        RSS 2.0 + Atom, bỏ thẻ HTML, conditional GET
internal/gateway/ollama/     /api/chat với JSON schema, chuẩn hoá nhãn
internal/gateway/telegram/   sendMessage, lỗi không chứa token
internal/gateway/clickhouse/ HTTP interface, query có tham số, đọc biến động giá và tin gần đây
internal/consumer/kafka/     consumer group, xử lý từng bản ghi, commit sau khi xong, thử lại có backoff
internal/publisher/kafka/    franz-go, idempotent, gom lô 50ms, nén zstd, dùng chung mọi topic
internal/service/ingest/     giá: tự kết nối lại, backoff 1s → 30s
internal/service/news/       tin: poller song song, lọc trùng, bỏ bài quá cũ
internal/service/enrich/     gán nhãn 1 tin, phân biệt lỗi tạm thời và lỗi vĩnh viễn
internal/service/alert/      chọn tin/biến động cần báo, chống trùng và spam, soạn tin nhắn
clickhouse/schema/           bảng, Kafka engine, materialized view, view phân tích
clickhouse/config.d|users.d/ cấu hình RAM/log, user chỉ đọc cho Grafana
grafana/                     datasource, dashboard (sinh bằng gen_dashboard.py)
```

## Test
```bash
go test ./...
```
