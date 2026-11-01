# Mảng 4 · Phát hiện bất thường, stress test, Grafana

> Phụ trách: Trần Anh Vũ. Soạn 01/11/2026. Lịch chung: code freeze 5/11, thuyết trình 8/11.

| Việc | Bắt buộc? | Chạy ở đâu | Trạng thái |
|---|---|---|---|
| B4.1 E7 trên LUNA 5/2022, giữ nguyên tham số | Có, quan trọng nhất | Máy có dữ liệu thô | **Xong** 01/11 trên máy Minh: 9–12/5 là 4 ngày hạng 1–4 |
| B4.3 Stress test theo bậc tốc độ | Nên làm | Máy dữ liệu (Kafka + Postgres), lúc rảnh | Chờ, mục 2 |
| B4.4 Panel Grafana cho demo | Có | Máy dữ liệu | Chờ, mục 3 |
| Báo cáo E7 | Có | | Có nháp, gồm cả LUNA (giữ ngoài repo, Minh gửi riêng) |
| Slide 14, 15, 17 | Có | | Chưa làm |

## 1. E7 trên LUNA

Chạy trên máy Minh (Windows, không có Postgres) bằng hai thay đổi nhỏ, không đụng `src/`:
- `experiments/lake_windows.py candles`: dựng nến bằng `build_candles` (cùng hàm với `build_batch`) rồi ghi parquet bằng pyarrow, vì Spark trên Windows thiếu winutils nên không ghi được parquet.
- `experiments/e7_anomaly.py --lake`: đọc nến từ `data/lake/gold/candles_batch` thay vì bảng `candles` trong Postgres.

```powershell
python -m bdi.ingest.download --kind aggTrades klines --months 2022-05     # ~7 GB, 2,5 phút
python experiments\lake_windows.py candles --periods 2022-05 2022-11 2026-06 2026-07 2026-08
python experiments\e7_anomaly.py --lake --month 2022-05 `
    --event-days 2022-05-09 2022-05-10 2022-05-11 2022-05-12 `
    --windows 1440 --thresholds 3.5 --floors 0.005 --tag luna
```

Kiểm tra trước: chạy `e7_anomaly.py --lake` với cấu hình mặc định (tháng 11/2022) phải tái lập đúng `results/e7/sweep.csv` cũ (dựng từ Postgres).

Trên máy dữ liệu (Linux, có Postgres) dùng đúng lệnh trong `docs/07-huong-dan-thanh-vien.md` B4.1, không cần `--lake`.

## 2. Stress test (máy dữ liệu, khi rảnh)

```bash
docker compose stop stream-candles-update raw-archive        # tắt việc nặng; KHÔNG làm trong ngày D đo C5
SPEEDS="1 10 50 100 200 500 1000" experiments/e7_stress.sh stress
docker compose start stream-candles-update raw-archive
```

- Mỗi bậc phát lại 20 phút sôi động nhất của FTX (9/11/2022 14:00–14:20), dừng khi độ trễ p95 > 10 s.
- Nếu job Spark tụt lại xa hoặc OOM: thêm `--max-offsets-per-trigger 500000` vào lệnh stream trong `e7_stress.sh` (bẫy P14).
- **Đầu ra:** `results/e7/stress.csv`, `results/e7/stress.png`.
- **Mốc so sánh** (từ thống kê mô tả, `results/desc/`): live trung bình 18,6 bản tin/s; phút đông nhất 4 tháng lịch sử 1 980 bản tin/s (22/8/2026 05:10, 5 cặp). Báo cáo throughput tối đa theo bội số của hai mốc này.

## 3. Grafana cho demo

**Dashboard kết quả đã bổ sung (01/11, trên máy Minh):** `publish_results.py` nạp thêm `exp_desc`, `exp_c2`, `exp_e6` (đo lại khi rảnh; số cũ ở `exp_e6_busy`), `exp_e7_luna`, `exp_e7_luna_daily`; `build_results_dashboard.py` thêm hàng "Dữ liệu", hàng "C2 · Phản chứng" và hai panel LUNA (28 panel). SQL đã chạy thử trên SQLite với cùng dữ liệu, **chưa thử trên Grafana thật**. Trên máy dữ liệu chỉ cần:

```bash
make results     # nạp CSV vào Postgres + dựng lại bdi_results.json
```

rồi mở `/d/bdi-results` kiểm từng panel mới hiện đúng.

- Dashboard kết quả: sửa `grafana/build_results_dashboard.py` rồi `make results`; không sửa tay JSON sinh ra.
- Dashboard live: sửa trên giao diện, Export JSON ghi đè `grafana/dashboards/bdi.json`.
- Panel nên có khi demo (slide 11): độ tươi nến stream, % khớp official 24 giờ, cảnh báo bất thường theo nguồn (`anomalies.source`), khoảng trống WS + backfill. Phần lớn đã có trong `bdi.json`; việc chính là sắp lại thứ tự cho khớp kịch bản demo trong `docs/04-demo.md` và thử với Thanh.

## 4. Sau khi có số

Cập nhật `docs/experiments/E7-anomaly-ftx.md` (bỏ dòng "cần kiểm lại trên LUNA") và mục E7 trong `docs/05-ket-qua.md`, **cùng commit** với `results/e7/`.
