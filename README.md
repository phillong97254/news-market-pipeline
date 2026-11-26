# news-market-pipeline

Pipeline thời gian thực: tin chính trị và biến động giá coin/chứng khoán.
Mục tiêu là phân tích tương quan, **không** phải dự đoán giá hay khuyến nghị đầu tư.

```
Coinbase WebSocket ──► price-producer (Go) ──► Redpanda: prices.tick            ← Giai đoạn 1 (xong)
GDELT + RSS ──► news-producer (Go) ──► Redpanda: news.raw                       ← Giai đoạn 2 (xong)
news.raw ──► nlp-worker (Go + Ollama qwen2.5:7b) ──► news.enriched             ← Giai đoạn 3 (xong)
Redpanda ──► ClickHouse ──► Grafana / cảnh báo Telegram                         ← Giai đoạn 4-5
```

## Chạy
Code nằm ở Kali, chạy trên Oracle (arm64, build tại chỗ):
```bash
./deploy.sh                  # đồng bộ + build + chạy tất cả
./deploy.sh news-producer    # chỉ build lại 1 service
```

Mọi cổng chỉ nghe trên `127.0.0.1` của Oracle. Xem Redpanda Console từ Kali:
```bash
ssh -N -L 8090:127.0.0.1:8090 oracle   # rồi mở http://localhost:8090
```

Kiểm tra nhanh trên Oracle:
```bash
docker exec redpanda rpk topic describe news.raw -p        # số bản ghi mỗi partition
docker exec redpanda rpk topic consume news.raw -n 5       # xem 5 bản ghi
docker logs -f news-producer                               # mỗi lần lấy: fetched/new/dup/too_old
```

## Topic `prices.tick`
- Key: symbol (`BTC-USD`, `ETH-USD`), nên mỗi cặp giữ đúng thứ tự trong 1 partition.
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

## Cấu trúc
```
cmd/price-producer/          giá coin
cmd/news-producer/           tin tức: tạo poller cho GDELT và từng feed RSS
cmd/nlp-worker/              gán nhãn tin bằng Ollama
internal/domain/             Tick, NewsItem, NewsAnalysis/EnrichedNews, NewsID/CanonicalURL
internal/gateway/coinbase/   WebSocket Coinbase, read timeout 30s
internal/gateway/gdelt/      GDELT DOC 2.0 (artlist JSON)
internal/gateway/rss/        RSS 2.0 + Atom, bỏ thẻ HTML, conditional GET
internal/gateway/ollama/     /api/chat với JSON schema, chuẩn hoá nhãn
internal/consumer/kafka/     consumer group, xử lý từng bản ghi, commit sau khi xong, thử lại có backoff
internal/publisher/kafka/    franz-go, idempotent, gom lô 50ms, nén zstd, dùng chung mọi topic
internal/service/ingest/     giá: tự kết nối lại, backoff 1s → 30s
internal/service/news/       tin: poller song song, lọc trùng, bỏ bài quá cũ
internal/service/enrich/     gán nhãn 1 tin, phân biệt lỗi tạm thời và lỗi vĩnh viễn
```

## Test
```bash
go test ./...
```
