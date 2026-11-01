# E6 · Truy vấn xấp xỉ

**Câu hỏi:** Truy vấn xấp xỉ có đáng dùng không?
**Phụ trách:** Mảng 3 · **Cắt đầu tiên nếu trễ** (chỉ giữ 1 cấu hình)

## Thiết kế

Chạy trên toàn bộ silver trades (419,8M dòng; thiết kế đã sửa, xem kết quả):

| Phép tính | Xấp xỉ | Tham số |
|---|---|---|
| `count(DISTINCT price)` theo symbol | `approx_count_distinct(price, rsd)` | rsd ∈ {0.01, 0.05, 0.1} |
| `percentile(qty, [0.5, 0.95, 0.99])` | `approxQuantile(qty, …, relativeError)` | relErr ∈ {0.001, 0.01, 0.05} |

Mỗi phép chạy 3 lần, lấy thời gian tốt nhất.

## Chạy

```bash
python experiments/e6_approx.py                     # local[*]
# mở rộng: 1 worker vs 2 worker (cần Spark standalone)
SPARK_MASTER=spark://<host>:7077 python experiments/e6_approx.py --tag 2workers
```

## Đầu ra

- `results/e6/approx_<tag>.csv`: `query`, `param`, `rel_error_pct`, `seconds`
- `results/e6/error_vs_time_<tag>.png`

## Kết quả (14/10/2026, 419 835 967 dòng, local[*], máy đang tải cao: load 25–30)

Lần chạy đầu theo đúng plan (đếm phân biệt `price`, phân vị `qty` gộp 5 cặp) cho kết quả gây hiểu nhầm:
- `price` có ít giá trị phân biệt nên đếm chính xác vốn rẻ;
- `qty` của các cặp khác đơn vị (BTC với XRP) thì không nên gộp;
- `relativeError` là sai số **thứ hạng**, không phải giá trị.

File cũ giữ lại ở `results/e6/approx_local_v1_price_qty.csv`. Thiết kế đã sửa nằm trong `experiments/e6_approx.py`.

**Đếm phân biệt theo symbol:**

| Cột | Số phân biệt | Chính xác | rsd = 0,01 | rsd = 0,05 | rsd = 0,1 |
|---|---|---|---|---|---|
| `price` (thấp) | 3,0 triệu | 90,0 s | 93,3 s · sai 1,6% | 62,8 s · 8,5% | 55,2 s · 14,6% |
| `agg_id` (cao) | 419,8 triệu | **234,7 s** | **18,4 s · sai 0,76%** | 10,6 s · 7,1% | 11,0 s · 11,0% |

Với cột nhiều giá trị phân biệt, HyperLogLog++ **nhanh hơn 12,7 lần** mà chỉ sai 0,76%. Với cột ít giá trị phân biệt thì không đáng dùng: `rsd = 0,01` còn chậm hơn bản chính xác. Sai số lớn nhất trên 5 cặp vượt `rsd` khoảng 1,5–1,7 lần, đúng tinh thần của độ lệch chuẩn (không phải cận cứng).

**Phân vị [0,5; 0,95; 0,99] của notional = price × qty (USDT), theo symbol:**

| Cấu hình | Thời gian | Sai số thứ hạng lớn nhất | Sai số giá trị so với relErr = 0,001 |
|---|---|---|---|
| `percentile` chính xác | **OutOfMemoryError** (heap 6 GB) | | |
| relErr = 0,001 | 101,8 s | 0,047% | (tham chiếu) |
| relErr = 0,01 | 316,9 s* | 1,00% | 59 042% |
| relErr = 0,05 | 334,2 s* | 5,00% | 203 712% |

- Phân vị chính xác **không chạy được** trên notional (hàng trăm triệu giá trị phân biệt mỗi cặp). Đây là lý do thực tế để cần xấp xỉ.
- Sai số thứ hạng **luôn nằm trong cam kết** `relErr` (đúng bằng cận khi dữ liệu có nhiều giá trị trùng).
- Sai số **giá trị** có thể rất lớn vì notional có đuôi cực dày (lệch 1% thứ hạng quanh p99 đổi giá trị nhiều bậc). Kết luận cho báo cáo: với dữ liệu đuôi dày, phải đặt `relErr` nhỏ (≤ 0,001) hoặc chỉ dùng xấp xỉ cho trung vị.
- (*) Thời gian relErr 0,01/0,05 dài hơn relErr 0,001 là do tải máy dao động mạnh lúc đo, không phản ánh chi phí thuật toán. Cần đo lại trên máy rảnh.

## Đo lại khi máy rảnh (01/11/2026)

Máy Minh (Windows, rảnh), 418 156 831 dòng (chỉ 4 tháng, không có file theo ngày), heap 6 GB, `--repeats 3`, `--tag idle_minh`. Silver dựng bằng `experiments/lake_windows.py silver` (Windows thiếu winutils nên Spark không ghi được parquet); đã kiểm giống hệt silver của Spark, 0 dòng khác biệt.

| Truy vấn | Chính xác | rsd/relErr 0,01 | 0,05 | 0,1 / — |
|---|---|---|---|---|
| Đếm phân biệt `price` (2,9M) | 17,9 s | 19,6 s · 1,6% | 15,7 s · 8,5% | 15,8 s · 14,6% |
| Đếm phân biệt `agg_id` (418M) | 238,2 s | **9,6 s · 0,80%** | 6,6 s · 7,1% | 6,5 s · 10,7% |
| Phân vị notional, relErr 0,001 / 0,01 / 0,05 | (OOM, lần cũ) | 37,5 s · thứ hạng 0,057% | 36,4 s · 1,00% | 36,3 s · 5,00% |

- **Đạt tiêu chí:** phân vị nhanh dần theo relErr 0,001 → 0,01 → 0,05, nhưng chỉ chênh 3%: chi phí nằm ở quét dữ liệu, không ở sketch.
- HyperLogLog++ trên `agg_id` nhanh hơn **24,7 lần** (lần đo tải cao: 12,7 lần).
- Kết quả: `results/e6/approx_idle_minh.csv`, `results/e6/error_vs_time_idle_minh.png`.

## Checklist

- [x] Bảng sai số và thời gian
- [x] Nhận xét: sai số thực tế so với `rsd` lý thuyết
- [x] Đo lại thời gian trên máy rảnh (01/11, máy Minh)
- [ ] (mở rộng) so sánh 1 worker và 2 worker
