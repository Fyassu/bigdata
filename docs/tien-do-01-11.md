# Tiến độ ngày 01/11/2026

> Ghi lại phần đã làm và chưa làm của 4 mảng tính đến 01/11/2026. Mốc còn lại: ngày D đo C5 là 2/11 (UTC), code freeze 5/11, thuyết trình 8/11.
> Các việc "đã làm" dưới đây chạy trên máy Minh (Windows, có 39 GB dữ liệu thô, không có Kafka, Postgres hay Docker). Việc nào cần luồng live, Kafka, raw archive hoặc Postgres thì còn chờ máy dữ liệu.

## Tóm tắt

| Mảng | Người | Đã làm | Chưa làm (bắt buộc) |
|---|---|---|---|
| 1 · Batch, lineage | Vũ Tuấn Minh | E1 đủ 5 cặp (nhánh `minh`), phản chứng C2 | Truy vấn lineage C6 trên Postgres |
| 2 · Streaming, chịu lỗi | Đỗ Xuân Thanh | Hướng dẫn chạy, script gắn nhãn trễ E3 | **C5 đủ 24 giờ**, E3 cửa sổ 2 giờ, kiểm E4, **demo + video** |
| 3 · Ghép mờ, xấp xỉ, thống kê | Đào Quang Huy | **Thống kê mô tả**, **E6 đo lại khi máy rảnh** | E5 biến thể event time (nên làm) |
| 4 · Bất thường, Grafana | Trần Anh Vũ | **E7 trên LUNA**, panel Grafana kết quả mới | Stress test, chạy `make results` kiểm panel |

## Mảng 1 · Vũ Tuấn Minh

**Đã làm**
- E1 đủ 5 cặp × 4 tháng, có hình: commit `13ebca5` và sửa C1 chia nano-giây `f153efd` trên nhánh `minh` (fork), **chưa gộp vào `main`**.
- Phản chứng C2 trên đủ 878 400 nến (`experiments/c2_counterfactual.py`, `results/c2/counterfactual.csv`): `first`/`last` còn 13,3% / 14,4%; cộng `double` còn 47,8%; đếm dòng aggTrade còn 0,47%.
- Phát hiện lỗi `quote_volume`: Spark làm tròn tích `decimal(24,8) × decimal(24,8)` về 6 chữ số lẻ, BTCUSDT chỉ khớp official 0,19%. Cách sửa `decimal(18,8) × decimal(18,8)` khớp 100%. Chưa áp vào `src/bdi/candles.py`.

**Chưa làm**
- Truy vấn lineage C6 trên Postgres máy dữ liệu (A2, A4).
- Test batch = streaming với **nhiều micro-batch** (không chạy được trên Windows vì thiếu winutils).
- Quyết định sửa `quote_volume` trong `build_candles` hay chỉ ghi vào hạn chế. Theo quy tắc của Mảng 1: không đổi `src/` trước khi đo xong C5.
- Gộp nhánh `minh` vào `main`.

## Mảng 2 · Đỗ Xuân Thanh

**Đã làm** (trên máy Minh, chưa chạy trên dữ liệu thật)
- Hướng dẫn chạy từng bước: [mang2/README.md](mang2/README.md); truy vấn kiểm tra máy dữ liệu: [mang2/kiem-tra.sql](mang2/kiem-tra.sql).
- `experiments/e3_late_labels.py`: gắn nhãn `LATE_BEYOND_WATERMARK` cho lượt E3 có tiêm độ trễ, dựng lại thứ tự đến từ cấu hình replay. Đã kiểm bằng dữ liệu giả lập, **chưa chạy trên dữ liệu thật**.

**Chưa làm**
- Kiểm `ws_gaps` có dòng `proactive_24h` (E4 ngắt 24 giờ).
- **C5 đủ 24 giờ** ngày D = 2/11, thêm cột batch ngày 3/11 (bắt buộc).
- E3 cửa sổ 2 giờ (chạy qua đêm), rồi chạy `e3_late_labels.py`.
- **Demo và video dự phòng 60 giây** (bắt buộc).

## Mảng 3 · Đào Quang Huy

**Đã làm**
- Thống kê mô tả (`experiments/descriptive_stats.py`, `results/desc/`): 418 156 831 aggTrade, 1,05 tỷ trade lẻ; số trade, quote volume và khối lượng mua chủ động khớp klines 100% ở 878 400 phút; phút đông nhất 1 980 bản tin/s.
- E6 đo lại khi máy rảnh (`results/e6/approx_idle_minh.csv`): **đạt tiêu chí** (phân vị nhanh dần theo relErr); HyperLogLog++ nhanh hơn 24,7 lần, sai 0,80%. Đo trên máy Minh, khác máy lần đo cũ.
- `experiments/lake_windows.py`: dựng silver và nến bằng pyarrow trên Windows không có winutils. Đã kiểm silver giống hệt cách Spark đọc CSV (0 dòng khác biệt).

**Chưa làm**
- E5 biến thể `--stream-time event` (cần raw archive 13/10 trên máy dữ liệu), xem [mang3/README.md](mang3/README.md) mục 2.
- E6 trên 2 worker (mở rộng, cắt đầu tiên nếu trễ).
- Ghép báo cáo cuối khi Mảng 2 có số.

## Mảng 4 · Trần Anh Vũ

**Đã làm**
- E7 trên LUNA 5/2022, giữ nguyên tham số (`results/e7/sweep_luna.csv`, `alerts_per_day_luna.*`): **9–12/5 là 4 ngày hạng 1–4 của tháng**. Chạy bằng `e7_anomaly.py --lake` (đọc nến từ parquet); với 11/2022 cách đọc này tái lập đúng `sweep.csv` cũ.
- Dashboard kết quả: `publish_results.py` và `build_results_dashboard.py` thêm thống kê mô tả, phản chứng C2, E6 đo lại, LUNA (28 panel). SQL đã chạy thử trên SQLite, **chưa thử trên Grafana thật**.

**Chưa làm**
- Stress test theo bậc tốc độ trên máy dữ liệu khi rảnh: [mang4/README.md](mang4/README.md) mục 2.
- `make results` trên máy dữ liệu và kiểm từng panel mới.
- Sắp lại dashboard live cho kịch bản demo.

## Việc chung

**Đã làm:** slide nháp 18 trang (artifact riêng tư của Minh); nháp báo cáo theo khung 5 mục cho mọi phần, kể cả mở đầu, kiến trúc, kết luận.

**Chưa làm:** điền 37 chỗ còn chờ trong nháp báo cáo (phần lớn chờ C5 24 giờ và E3 2 giờ); tập demo 2 lần; chia sẻ slide cho nhóm.

## Không đẩy lên repo

Nháp báo cáo, khung và script ghép báo cáo không thuộc phần chạy project nên giữ ngoài repo (khai báo trong `.git/info/exclude` trên máy Minh), Minh gửi riêng cho nhóm:
`docs/mang1/`, `docs/mang2/bao-cao/`, `docs/mang3/bao-cao/`, `docs/mang3/ghep-bao-cao.md`, `docs/mang3/ghep_bao_cao.py`, `docs/mang3/bao-cao-tong-hop.md`, `docs/mang4/bao-cao/`. Dữ liệu `data/` (silver 9,7 GB, nến parquet) đã nằm trong `.gitignore`.
