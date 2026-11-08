# Tiến độ ngày 08/11/2026

> Nối tiếp [tien-do-01-11.md](tien-do-01-11.md). Lịch mới: thuyết trình **15/11**. Lượt chạy 24 giờ cho C5 chuyển sang máy Minh (WSL2 + Docker), ngày D dự kiến 9/11 (UTC), dự phòng 11/11 hoặc 12/11.
> Nhánh này (`tong-hop/truoc-c5`) gộp `minh` (E1 đủ 5 cặp, sửa C1, phản chứng C2) với `tong-hop/ket-qua-01-11`, rồi làm các việc **không cần kết quả C5**. Chưa đổi gì trong `src/`. Theo quy tắc Mảng 1, image dùng cho lượt chạy phải giữ nguyên tới khi đo xong C5.

## Đã làm từ 01/11

| Việc | Mảng | Kết quả |
|---|---|---|
| Hạ tầng trên máy Minh | 2 | WSL2 Ubuntu + Docker + Java 17 + venv PySpark 4.0.4; Kafka, Postgres, Grafana và 4 service live chạy thông suốt (đã thử end-to-end) |
| Test batch = streaming nhiều micro-batch | 1 | 3 test mới trong `tests/test_candles.py`: update mode (phút 0 cập nhật qua 3 đợt, bản cuối khớp batch); append + `dropDuplicatesWithinWatermark` (bản tin gửi lại không bị cộng 2 lần); bản tin trễ quá watermark bị bỏ đúng 1 dòng (`numRowsDroppedByWatermark = 1`). **25/25 test xanh** |
| Bảng gộp C3 | 1, 2 | `experiments/c3_causes.py` → `results/c3/causes_by_experiment.csv`: 21 lượt, 889 328 nến, 548 lệch. Gồm 5 `CAPTURE_START` (live); 543 nến của 4 lượt E3 jitter chưa có nhãn |
| Truy vết C6 | 1 | `experiments/c6_lineage.py`: % nến truy được theo nguồn (sha256 → URL file Binance; topic Kafka; endpoint REST), `--trace SYMBOL MINUTE` cho demo. Đã chạy thử trên Postgres WSL |
| `make results` trên Grafana thật | 4 | Dashboard "Kết quả thí nghiệm": 19/20 panel có truy vấn ra dữ liệu (kiểm qua `/api/ds/query`). Panel "E4 · Khoảng trống WS (gần nhất)" đọc `ws_gaps` live, rỗng khi luồng live chưa chạy |
| Sửa lời | chung | E2 "khớp mọi trường" thành "cả 6 trường được so (O, H, L, C, V, trades)", kèm ghi chú `quote_volume` không nằm trong phép so; số test 21 → 25 |

## Còn lại

**Chờ lượt 24 giờ (ngày D, rồi D+1):**
- C5 đủ 24 giờ, thêm cột batch; đối soát `c5_batch_<D>`; E4 `proactive_24h`.
- C6 số chính thức: `python experiments/c6_lineage.py` sau khi đã nạp batch và official ngày D.
- E3 cửa sổ 2 giờ từ raw archive của lượt chạy (qua đêm), rồi `e3_late_labels.py`. Sau đó chạy lại `c3_causes.py` để cột "chưa gắn nhãn" về 0.
- E5 biến thể event time trên raw archive ngày D.

**Chỉ làm sau khi đo xong C5 (đổi `src/`):**
- Sửa `quote_volume` trong `src/bdi/candles.py` (`decimal(18,8) × decimal(18,8)`), thêm test. Hoặc giữ nguyên và ghi vào hạn chế.

**Sau ngày D, khi máy rảnh:** stress test E7 theo bậc (Mảng 4); E6 trên 2 worker (mở rộng, cắt đầu tiên nếu thiếu giờ).

**Chung:** điền số vào nháp báo cáo; chốt hình; tập demo 2 lần, quay video 60 giây; gộp nhánh này vào `main` khi nhóm đồng ý.
