# 00 · Cài đặt và chạy

## Yêu cầu

| Thành phần | Phiên bản | Ghi chú |
|---|---|---|
| Docker + Compose | ≥ 24 / v2 | Chạy Kafka, Postgres, Grafana và pipeline live |
| Python | 3.11 – 3.14 | Đã kiểm: PySpark 4.0.4 + pandas 3 chạy được trên 3.14 (21/21 test xanh, 13/10); WSL Ubuntu + Python 3.12 + Java 17: 25/25 (08/11). Image Docker dùng 3.11 |
| Java | 17 hoặc 21 | Spark 4.0 cần Java ≥ 17 (OpenJDK 21 qua Homebrew là được) |
| RAM | ≥ 12 GB | Mỗi job Spark của E3 ~1 GB; `GROUP` điều chỉnh số job song song (mặc định 2) |
| Ổ đĩa | ≈ 40–60 GB | 5 cặp × 4 tháng aggTrades (~185M dòng): CSV + parquet + Kafka |

## Lần đầu

```bash
cd bigdata
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
set -a && source .env && set +a     # Kafka localhost:9094, Postgres localhost:5433
export PYTHONPATH=src

make up                             # kafka, kafka-init (tạo topic), postgres (chạy sql/init.sql), grafana
make test                           # hoặc: make test-docker
```

- Postgres: `localhost:5433` (cổng 5433 để tránh đụng Postgres có sẵn trên máy), db/user/pass đều là `bdi`
- Grafana: http://localhost:3000 (admin/admin; xem ẩn danh cũng được), hai dashboard:
  - **BDI · Binance batch + streaming** (`/d/bdi-live`): luồng live, sự cố, cảnh báo
  - **BDI · Kết quả thí nghiệm** (`/d/bdi-results`): E1–E7, C5. Nạp lại sau khi có kết quả mới bằng `make results`
- Service `official-poller` tự lấy nến official qua REST mỗi phút; luồng live chạy phát hiện bất thường (`--anomaly --anomaly-history official`)
- Kafka từ host: `localhost:9094`; từ container: `kafka:9092`

## Bật luồng live (ưu tiên số 1 theo checkpoint 13/10)

```bash
make up-all            # build image + chạy ws-producer, raw-archive, stream-candles
make logs              # xem log
make live-status       # sau ~2 phút phải thấy nến source='stream' cho cả 5 cặp
```

Luồng live phải **chạy liên tục ít nhất 24h** vì E3, E4, E5 và C5 đều dùng dữ liệu này. Để máy không ngủ (macOS: `caffeinate -dims &`).

Nếu `stream.binance.com` bị chặn: sửa lệnh của service `ws-producer` trong `docker-compose.yml` thành `["python","-m","bdi.ingest.ws_producer","--fallback"]`. Khi đó producer dùng `wss://data-stream.binance.vision`.

## Dữ liệu lịch sử (tầng batch)

```bash
python -m bdi.ingest.download                 # 5 cặp × 4 tháng × {aggTrades, klines}, kiểm sha256
python -m bdi.batch.load_klines               # official -> data/lake/gold + Postgres
python -m bdi.batch.build_batch               # aggTrades -> silver parquet -> nến batch -> Postgres
```

Thử nhỏ trước: `--symbols BTCUSDT --months 2026-08` (download) và `--symbols BTCUSDT --periods 2026-08` (batch).

Đối soát stream **ngay trong ngày** (không phải chờ file T+1): lấy klines chính thức qua REST

```bash
python -m bdi.ingest.rest_klines --minutes 120        # ghi source='official', file_checksum='rest:/api/v3/klines'
python -m bdi.reconcile.compare --left stream --run-id live_check --inner
```

Cho ngày chạy live (bản đầy đủ, sau khi Binance công bố file ngày, thường là T+1):

```bash
python -m bdi.ingest.download --days 2026-10-14
python -m bdi.batch.load_klines --periods 2026-10-14
python -m bdi.batch.build_batch --periods 2026-10-14
```

## Sự cố thường gặp

| Triệu chứng | Cách xử lý |
|---|---|
| Postgres báo `role "bdi" does not exist` | Đang kết nối nhầm vào Postgres cài sẵn trên máy (cổng 5432). Project dùng cổng **5433** |
| Nến lệch đúng 7 giờ | Đã xử lý trong `sink._utc_strings`. Nếu tự ghi timestamp từ Spark sang Python, luôn định dạng chuỗi UTC trước |
| `Java gateway process exited` | Chưa có Java 17+ hoặc `JAVA_HOME` sai |
| Producer báo `UnknownTopicOrPart` | `kafka-init` chưa chạy: `docker compose up kafka-init` |
| Stream không ra nến | Watermark chưa vượt qua phút đó. Chờ thêm ≥ 1 phút + watermark, rồi xem `results/progress/live.jsonl` |
| Đổi code stream rồi job lỗi khi khởi động | Checkpoint cũ không tương thích. Xóa `data/checkpoints/stream_candles_<run_id>` hoặc đổi `--run-id` |
| HTTP 418/429 khi backfill | Binance rate limit; code đã tự chờ theo `Retry-After` |
