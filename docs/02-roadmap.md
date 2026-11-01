# 02 · Lộ trình và việc cần làm

Đánh `[x]` khi xong. Mỗi mục có **đầu ra kiểm chứng được**: một file trong `results/`, một bảng trong DB, hoặc một test xanh.

> **Hôm nay (13/10/2026) là checkpoint.** Nếu luồng streaming chưa chạy trọn vẹn cho ít nhất 1 cặp, làm mục **Tuần 0** trước mọi thứ khác.

## Phân công (điền tên)

| Mảng | Phụ trách | Phạm vi | Thí nghiệm |
|---|---|---|---|
| 1 | _______ | Ingestion batch, checksum, lineage | E1, E2 |
| 2 | _______ | Streaming, Kafka, chaos test | E3, E4 |
| 3 | _______ | Record linkage, truy vấn xấp xỉ | E5, E6 |
| 4 | _______ | Thống kê mô tả, phát hiện bất thường, Grafana | E7 |

---

## Tuần 0: Checkpoint streaming (làm ngay)

- [x] `make up` chạy được, Postgres có đủ bảng (`\dt` trong `make psql`)
- [x] `make test` xanh: 21/21 test (13/10)
- [x] `make up-all`, sau 2 phút `make live-status` thấy nến `stream` của cả 5 cặp
- [x] Grafana hiển thị panel "Giá đóng cửa theo nguồn" (đã kiểm qua API)
- [x] `data/lake/bronze/raw_aggtrades/` có file mới sau mỗi 30 giây (offset liên tục, không thiếu/trùng)
- [ ] **Để luồng live chạy liên tục ≥ 24h** (`caffeinate`). Đây là dữ liệu cho E3, E4, E5 và C5
- [x] Ghi lại ngày giờ bắt đầu chạy vào đây: `2026-10-13 15:07 UTC` (container `ws-producer`, `stream-candles`, `raw-archive`)
- [x] Kiểm tra nhanh stream vs official qua REST (`bdi.ingest.rest_klines`): 10/11 phút khớp tuyệt đối mọi trường; phút lệch duy nhất là `CAPTURE_START` (producer khởi động giữa phút). Các lần restart producer đều được backfill và khớp 100%

## Tuần 1: Nền tảng batch + E1 + E2 (Mảng 1)

- [x] `python -m bdi.ingest.download`: tải 5 cặp × 4 tháng, 40/40 file `[ok]` checksum (39 GB)
- [x] Ghi lại tổng số dòng aggTrades: **418.156.831 dòng** (gấp khoảng 2,3 lần ước tính 185M trong plan, cần sửa con số trong báo cáo và E6)
- [x] `python -m bdi.batch.load_klines`: 20 file, mỗi tháng đủ 43 200 hoặc 44 640 phút
- [x] `python -m bdi.batch.build_batch`: 418M dòng, khoảng 70 phút
- [x] **E1** (SOL + XRP): [kết quả](experiments/E1-drift-ablation.md) → `results/e1/ablation.csv`. Naive hỏng 100% ở 2026, drift-aware đúng 100%
- [ ] E1 cho đủ 5 cặp × 4 tháng
- [x] E7 kiểm tra lại trên LUNA 5/2022, giữ nguyên tham số: 9–12/5 là top-4 ngày (01/11)
- [x] E6 đo lại thời gian trên máy rảnh: HLL nhanh 24,7 lần, phân vị nhanh dần theo relErr (01/11)
- [x] **E2**: [kết quả](experiments/E2-batch-vs-official.md). **878 400/878 400 nến khớp tuyệt đối**
- [x] Kiểm tra câu hỏi mở của C3: official của 5 cặp × 4 tháng **không có phút nào `trades = 0`**, và mọi tháng đủ phút. Với các cặp thanh khoản cao này, `EMPTY_MINUTE` không xảy ra (cần ghi rõ phạm vi khi kết luận)
- [ ] Viết nháp mục báo cáo C1 và C2: ba điểm kỹ thuật (min_by/max_by, Decimal, số trades), dẫn chứng bằng `tests/test_candles.py`

## Tuần 2: E3 + E4 + Grafana (Mảng 2, Mảng 4)

- [x] Tải official + aggTrades ngày 13/10 (BTC, ETH); các khoảng khác dùng `rest_klines`
- [x] Đối soát live (93/94, 514/514 phút khớp; xem E4): `python -m bdi.reconcile.compare --left stream --start <ngày> --end <ngày+1> --run-id live_d1 --with-late 5`
- [x] **E3** bản đầu (30 phút dữ liệu thật): [kết quả](experiments/E3-watermark-tradeoff.md). Luồng thật gần như không có bản tin trễ (tối đa 0,78 s), 100% khớp ở mọi watermark, độ trễ p95 tăng 11,9 → 36,1 s
- [x] E3 với tiêm độ trễ (5% bản tin trễ ≤ 20 s): % khớp 2% ở W = 0 s, 100% ở W = 30 s. Hình đầy đủ ở `results/e3/tradeoff.png`
- [ ] (tùy chọn) Chạy lại E3 trên cửa sổ ≥ 2 giờ sôi động
- [x] E4 · sự cố tự nhiên (máy ngủ 5,7 giờ): [kết quả](experiments/E4-fault-injection.md). Backfill 260 nghìn bản ghi, 514/514 phút × 5 cặp khớp official
- [x] **E4** chủ động: [kết quả](experiments/E4-fault-injection.md). 4 kịch bản streaming 0 nến thiếu/thừa/lệch, phục hồi 5,7–25,6 s
- [x] E4 · idempotent: `chaos.sh batch_rerun` in ra `[ok] idempotent` (175 680 dòng, md5 không đổi)
- [ ] E4 · ngắt tự nhiên sau 24h: bảng `ws_gaps` có dòng với `reason` tương ứng
- [ ] Grafana bản đầu: thêm panel theo ý nhóm, rồi export JSON vào `grafana/dashboards/`

## Tuần 3: E5 + E6 + E7 (Mảng 3, Mảng 4)

- [x] **E5**: [kết quả](experiments/E5-record-linkage.md). Ghép giữ thứ tự đạt F1 = 0,995 ở δ = 1 s; blocking giảm số cặp 250–2 750 lần
- [x] **E6**: [kết quả](experiments/E6-approx-query.md). HLL++ nhanh hơn 12,7 lần (sai 0,76%) trên agg_id; phân vị chính xác OOM, nên phải dùng xấp xỉ
- [x] **E7** batch: [kết quả](experiments/E7-anomaly-ftx.md). Sau khi sửa phương pháp (W = 1 ngày, log-volume, sàn 0,5%), 8–9/11/2022 là top-2 ngày
- [x] **E7** replay FTX 100× trên stream: báo động stream trùng batch 111/111; nến khớp official 5 758/5 758
- [x] E7 throughput sơ bộ: 13,9k–25,5k bản tin/s (khoảng 750–1 370 lần live)
- [ ] **E7** stress test theo bậc (`e7_stress.sh stress`) trên máy rảnh → `results/e7/stress.png`
- [x] Thống kê mô tả: `experiments/descriptive_stats.py` → `results/desc/`; khớp klines 100% (01/11)

## Tuần 4: Tổng hợp + báo cáo + demo

- [ ] **C3**: bảng phân bố nguyên nhân lệch gộp từ E2, E3, E4
- [x] **C5** sơ bộ: [kết quả](experiments/C5-source-policy.md). TTC p50 = 13 s, 100% nến hội tụ; bảng quy tắc chọn nguồn đã có số liệu
- [ ] C5: đo lại trên 24h live khi máy rảnh, thêm cột batch sau T+1
- [ ] **C6**: mô tả lineage, dẫn chứng bằng `SELECT source, file_checksum, watermark_config, pipeline_version FROM candles LIMIT 5`
- [ ] Chốt toàn bộ hình trong `results/` và đưa vào báo cáo
- [ ] Tập demo theo [04-demo.md](04-demo.md) ít nhất 2 lần

---

## Thứ tự cắt nếu trễ tiến độ

1. E6: chỉ giữ 1 cấu hình (`rsd=0.05`, `relErr=0.01`)
2. E5: chỉ giữ δ ∈ {100ms, 1s}, sửa `DELTAS_MS` trong `experiments/e5_linkage.py`
3. Replay 100× và stress test

**Không được cắt:** E2, E3, E4.
