# Mảng 3 · Ghép mờ, truy vấn xấp xỉ, thống kê mô tả, ghép báo cáo

> Phụ trách: Đào Quang Huy. Soạn 01/11/2026. Lịch chung: code freeze 5/11, thuyết trình 8/11.

| Việc | Bắt buộc? | Chạy ở đâu | Trạng thái |
|---|---|---|---|
| B4.2 Thống kê mô tả | Có | Máy bất kỳ có `data/raw` | **Xong** trên máy Minh, xem mục 1 |
| B3.2 E6 đo lại khi máy rảnh | Có | Máy có silver 4 tháng | **Xong** 01/11 trên máy Minh (phương án B), đạt tiêu chí |
| B3.1 E5 biến thể event time | Nên làm | Máy dữ liệu (raw archive 13/10) | Chờ, mục 2 |
| B3.3 E6 trên 2 worker | Mở rộng, cắt đầu tiên | Máy dữ liệu (Docker) | Bỏ nếu trễ |
| Báo cáo C4, E6, thống kê mô tả | Có | | Có nháp (giữ ngoài repo, Minh gửi riêng) |
| Ghép báo cáo cuối | Có | | Có khung và script ghép (giữ ngoài repo, Minh gửi riêng) |

## 1. Thống kê mô tả (đã chạy)

Script mới `experiments/descriptive_stats.py` đọc thẳng CSV aggTrades, không cần silver, Kafka hay Postgres. Mỗi file quét một lần; cùng lượt quét, số trade, quote volume và khối lượng mua chủ động được so tuyệt đối với klines official.

```bash
python experiments/descriptive_stats.py --symbols BTCUSDT ETHUSDT BNBUSDT SOLUSDT XRPUSDT \
    --periods 2022-11 2026-06 2026-07 2026-08
python experiments/descriptive_stats.py --plots-only     # chỉnh hình, không chạy Spark
```

Trên Windows cần thêm biến môi trường (xem docstring của script):

```powershell
$env:PYTHONUTF8 = "1"; $env:BDI__PATHS__RAW = "D:\bdi-data\raw"
$env:SPARK_DRIVER_MEMORY = "12g"; $env:SPARK_LOCAL_DIRS = "D:\spark-tmp"
```

Đầu ra trong `results/desc/`: `pair_month.csv`, `trades_per_minute.png`, `daily_volume.png`, `taker_buy.png`. Bảng phút trung gian `data/lake/gold/desc_minutes.parquet` không commit.

Khác với mô tả gốc ở B4.2 (`docs/07-huong-dan-thanh-vien.md`): script **không** đọc bảng `candles` trong Postgres mà tự gộp theo phút. Lý do: chạy được trên máy không có Postgres, và tỉ lệ mua chủ động tính được theo **số trade** (klines chỉ cho theo khối lượng).

## 2. E5 biến thể event time (máy dữ liệu, khoảng 5 phút)

Mục đích: cho thấy nếu dùng thời điểm giao dịch thay vì thời điểm nhận thì bài toán ghép trở nên tầm thường. Đó là bằng chứng vì sao E5 phải dùng `recv_ts_us`.

`e5_linkage.py` luôn ghi đè `results/e5/linkage.csv` và hai hình, nên **phải sao lưu trước**:

```bash
cd results/e5
mkdir -p _recv && cp linkage.csv pr_curve.png blocking_cost.png _recv/
cd ../..
python experiments/e5_linkage.py --date 2026-10-13 --start 2026-10-13T15:15 --minutes 30 --stream-time event
cd results/e5
mv linkage.csv linkage_event.csv && mv pr_curve.png pr_curve_event.png && mv blocking_cost.png blocking_cost_event.png
cp _recv/* . && rm -r _recv
git status results/e5     # chỉ được có 3 file *_event mới; linkage.csv không đổi
```

Kỳ vọng: P = R = 1 ở mọi δ với cách ghép giữ thứ tự. Nếu P < 1 thì kiểm các bản ghi trùng hệt cả `(price, qty, ts_us)`: khi trùng mốc thời gian, thứ tự sắp xếp trong nhóm không xác định nên vẫn có thể ghép chéo. Đó vẫn là kết quả đáng ghi (số cặp như vậy là bao nhiêu) chứ không phải lỗi script.

## 3. E6 đo lại khi máy rảnh

Số đo cũ ghi lúc máy tải cao (load 25–30) nên thứ tự thời gian bị ngược: relErr 0,01 và 0,05 chậm hơn 0,001. **Đạt khi** relErr nhanh dần theo thứ tự 0,001 → 0,01 → 0,05.

Dùng tag mới để giữ file cũ làm đối chứng:

**Phương án A, máy dữ liệu** (đúng như B3.2, có silver 419,8 triệu dòng):

```bash
docker compose stop stream-candles-update raw-archive     # KHÔNG làm trong ngày D đo C5 (docs/mang2/README.md)
SPARK_DRIVER_MEMORY=6g python experiments/e6_approx.py --repeats 3 --tag idle
docker compose start stream-candles-update raw-archive    # nhớ bật lại
```

**Phương án B, máy Minh** (rảnh hẳn, không đụng luồng live và lịch C5). Trên Windows không có winutils, `build_batch` hỏng ở bước ghi parquet, nên silver dựng bằng pyarrow (8,5 phút, 9,7 GB). Đã kiểm silver này giống hệt cách Spark đọc CSV (0 dòng khác biệt, tổng 418 156 831 dòng). `e6_approx.py` trên Windows tự đọc silver qua danh sách file.

```powershell
$env:PYTHONPATH = "e:\cao_hoc\big_data\bigdata\src"; $env:SPARK_DRIVER_MEMORY = "6g"
D:\venvs\bdi\Scripts\python.exe experiments\lake_windows.py silver --symbols BTCUSDT ETHUSDT BNBUSDT SOLUSDT XRPUSDT `
    --periods 2022-11 2026-06 2026-07 2026-08
D:\venvs\bdi\Scripts\python.exe experiments\e6_approx.py --repeats 3 --tag idle_minh
```

Silver ở phương án B có 418 156 831 dòng (chỉ 4 tháng), ít hơn 419 835 967 của lần đo cũ (có thêm file theo ngày). Khi so phải ghi rõ khác máy và khác số dòng; so thứ tự thời gian giữa các cấu hình trong cùng một lần đo thì vẫn hợp lệ.

Sau khi chạy: điền số vào nháp báo cáo E6, cập nhật `docs/experiments/E6-approx-query.md` và `docs/05-ket-qua.md` **trong cùng commit** với `results/e6/approx_<tag>.csv`.

## 4. Ghép báo cáo

Mỗi mảng gửi phần của mình theo khung 5 mục (câu hỏi, thiết kế, kết quả, diễn giải, hạn chế). Danh sách phần, người viết, file nháp và trạng thái nằm trong khung ghép báo cáo (giữ ngoài repo).
