# Tích hợp batch và streaming dữ liệu giao dịch Binance

**IT5427 · Tích hợp và xử lý dữ liệu lớn · Nhóm 3**: Vũ Tuấn Minh, Đỗ Xuân Thanh, Đào Quang Huy, Trần Anh Vũ

Kế hoạch gốc nằm ở [`plan.md`](plan.md). Repo này là phần hiện thực của kế hoạch đó: mỗi đóng góp (C1–C6) có code tương ứng, và mỗi thí nghiệm (E1–E7) có script chạy ra bảng và hình cho báo cáo.

## Đọc gì trước

> **Thành viên mới clone repo:** đọc [docs/07-huong-dan-thanh-vien.md](docs/07-huong-dan-thanh-vien.md) trước tiên.


| Bước | Tài liệu | Nội dung |
|---|---|---|
| 1 | [docs/00-setup.md](docs/00-setup.md) | Cài đặt, bật hạ tầng, chạy test, kiểm tra luồng live |
| 2 | [docs/01-architecture.md](docs/01-architecture.md) | Kiến trúc, lược đồ chuẩn, bảng DB, các quyết định thiết kế |
| 3 | [docs/02-roadmap.md](docs/02-roadmap.md) | **Việc cần làm**: checklist theo tuần và theo mảng, kèm người phụ trách |
| 4 | [docs/03-pitfalls.md](docs/03-pitfalls.md) | Các bẫy kỹ thuật và chỗ code đã xử lý |
| 5 | [docs/experiments/](docs/experiments/) | Hướng dẫn từng thí nghiệm E1–E7 và C5 |
| 6 | [docs/04-demo.md](docs/04-demo.md) | Kịch bản demo trước hội đồng |
| 7 | [docs/05-ket-qua.md](docs/05-ket-qua.md) | **Tổng hợp toàn bộ số liệu** E1–E7, C5 |
| 8 | [docs/06-dan-y-slide.md](docs/06-dan-y-slide.md) | **Dàn ý slide** chi tiết + câu hỏi hội đồng |
| 9 | [docs/07-huong-dan-thanh-vien.md](docs/07-huong-dan-thanh-vien.md) | **Việc của từng thành viên**: lệnh chạy, tiêu chí đạt, lịch |

## Kiến trúc tóm tắt

```
                         ┌──────────────── tầng BATCH ────────────────┐
data.binance.vision ──►  download (sha256) ─► CSV ─► silver parquet ──┐│
  aggTrades / klines                                                  ▼│
                                         build_candles(df)  ◄── CÙNG MỘT HÀM (C2)
Binance WebSocket  ──►  ws_producer ─► Kafka ─► stream_candles ───────┘│
  + REST backfill       (state aggId)   │        (watermark, dedup)    │
                                        └─► raw_archive (bronze) ──► replay (E3, E7)
                                                                       │
          Postgres: candles(symbol, minute, source) ◄── upsert idempotent (C6)
                    ws_gaps, chaos_events, reconcile_results, anomalies
                          │
                          ├─► reconcile/compare + taxonomy (C3) ─► results/
                          └─► Grafana dashboard (http://localhost:3000)
```

## Bắt đầu nhanh

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env && set -a && source .env && set +a
export PYTHONPATH=src

make up                       # Kafka + Postgres + Grafana
make test                     # hoặc: make test-docker
make up-all                   # bật thêm pipeline live (WS -> Kafka -> Spark -> Postgres)
make live-status              # sau khoảng 2 phút sẽ thấy nến source='stream'
```

## Bản đồ code

| Mã | Đóng góp | Code chính | Thí nghiệm |
|---|---|---|---|
| C1 | Lược đồ chuẩn nhận biết schema drift | `src/bdi/timeutil.py`, `src/bdi/schema.py`, `tests/test_timeutil.py` | `experiments/e1_drift_ablation.py` |
| C2 | Viết một lần, chạy hai tầng | `src/bdi/candles.py`, `tests/test_candles.py` | E2, E3 |
| C3 | Đối soát 3 nguồn + nhãn nguyên nhân | `src/bdi/reconcile/compare.py`, `src/bdi/reconcile/taxonomy.py` | E2, E3, E4; bảng gộp `experiments/c3_causes.py` |
| C4 | Record linkage mờ | `src/bdi/linkage/fuzzy.py` | `experiments/e5_linkage.py` |
| C5 | Chọn nguồn theo độ hội tụ | `src/bdi/metrics.py` | `experiments/c5_source_policy.py`, E3, E4 |
| C6 | Lineage + ghi idempotent | `src/bdi/lineage.py`, `src/bdi/sink.py`, `sql/init.sql` | `experiments/e4_chaos/chaos.sh batch_rerun`, `experiments/c6_lineage.py` |

```
src/bdi/
  config.py  spark.py  schema.py  timeutil.py  candles.py  lineage.py  sink.py  metrics.py
  ingest/    download.py · ws_producer.py · backfill.py · replay.py · rest_klines.py
  batch/     build_batch.py · load_klines.py
  stream/    stream_candles.py · raw_archive.py
  reconcile/ compare.py · taxonomy.py
  linkage/   fuzzy.py
  anomaly/   mzscore.py
experiments/ e1 … e7, c3, c5, c6, e4_chaos/chaos.sh
tests/       contract test C1, test C2 (batch = streaming, nhiều micro-batch), C3, C4, E7
sql/init.sql · docker-compose.yml · docker/Dockerfile · grafana/
data/        (gitignored) raw, lake, checkpoints, state
results/     bảng CSV + hình PNG cho báo cáo
```

> Không đẩy `data/` lên GitHub. `.gitignore` đã chặn thư mục này, đúng cam kết trong đề xuất.
