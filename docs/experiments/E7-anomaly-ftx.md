# E7 · Phát hiện bất thường + replay FTX

**Câu hỏi:** Có phát hiện được sự kiện FTX không?
**Phụ trách:** Mảng 4

## Phương pháp

Modified z-score `M = 0.6745 · (x − median) / MAD`, báo động khi `|M| > 3.5` (Iglewicz–Hoaglin). Áp lên log-return và log(1 + khối lượng) của nến 1 phút. Median và MAD tính trên cửa sổ rolling W phút **trước** phút hiện tại (`anomaly/mzscore.py`). Mặc định W = 1440 phút và sàn |log-return| ≥ 0,5% (lý do ở phần kết quả).

## Phần 1: Batch

```bash
python experiments/e7_anomaly.py --source batch     # quét W ∈ {60,1440} × |M| ∈ {3.5,7,10} × sàn ∈ {0, 0,5%, 1%}
```

- `results/e7/sweep.csv`: `ftx_8`, `ftx_9`, `ftx_rank_8`, `ftx_rank_9`, `per_day_nov22`, `per_day_quiet26`, `volume_alerts_per_day_quiet26`
- `results/e7/ftx_btc.png`: giá BTC 11/2022 với các điểm bất thường, vùng tô là 8–9/11

Vì không có nhãn chuẩn, precision và recall chỉ mang tính tương đối. **Phải ghi rõ điều này trong báo cáo.** Dùng "số báo động/ngày giai đoạn yên ắng 6–8/2026" làm proxy cho tỷ lệ báo động giả.

### Kết quả (14/10/2026, nến batch 5 cặp)

Lần chạy đầu theo đúng plan (log-return và khối lượng thô, W = 60 phút, |M| > 3,5) **không đạt**:
- 700–900 báo động/ngày;
- 8–9/11 không nằm trong top 3 ngày;
- giai đoạn yên 2026 còn báo nhiều hơn tháng 11/2022.

Nguyên nhân và cách sửa (đã đưa vào `anomaly/mzscore.py` và `config/settings.yaml`):

| Vấn đề | Cách sửa |
|---|---|
| Khối lượng có đuôi rất dày | Dùng `log(1 + volume)` |
| W = 60 phút thích nghi quá nhanh: cả ngày FTX trở thành "bình thường" | W = 1440 phút (baseline 1 ngày) |
| M đo độ bất ngờ **tương đối**: thị trường yên thì MAD nhỏ, cú nhảy rất nhỏ cũng bị gắn cờ (BTC 6–8/2026: 29 báo động/ngày) | Thêm sàn tuyệt đối \|log-return\| ≥ 0,5% |
| Cửa sổ 7 ngày cho 0 báo động ở 8–9/11 | Không phải kết quả: dữ liệu bắt đầu 1/11 nên chưa đủ lịch sử. Cần tải thêm 10/2022 nếu muốn thử |

Bảng chọn lọc từ `results/e7/sweep.csv` (báo động log-return, 5 cặp cộng dồn):

| W | \|M\| > | Sàn | Ngày 8/11 | Ngày 9/11 | Hạng 8/11 | Hạng 9/11 | /ngày 11/2022 | /ngày yên 2026 |
|---|---|---|---|---|---|---|---|---|
| 60 | 3,5 | 0 | 197 | 86 | 4 | 29 | 144,2 | 201,8 |
| 1440 | 3,5 | 0 | 930 | 210 | 1 | 12 | 202,5 | 242,5 |
| **1440** | **3,5** | **0,5%** | **576** | **207** | **1** | **2** | **60,1** | **9,3** |
| 1440 | 3,5 | 1% | 235 | 116 | 1 | 2 | 18,3 | 0,9 |
| 1440 | 7 | 0,5% | 248 | 10 | 1 | 14 | 21,1 | 5,4 |

**Cấu hình chọn: W = 1 ngày, |M| > 3,5 (giữ đúng ngưỡng Iglewicz–Hoaglin), sàn 0,5%.**
- 8/11 và 9/11/2022 là **hai ngày nhiều báo động nhất tháng**; riêng 8/11 gấp khoảng 3 lần ngày thứ ba (14/11).
- Giai đoạn yên 2026 còn khoảng 9 báo động/ngày trên 5 cặp, tức khoảng 1,8/cặp/ngày.
- Cần ghi rõ trong báo cáo: sàn và cửa sổ được chọn sau khi xem kết quả trên chính tháng 11/2022, nên có rủi ro "khớp theo dữ liệu". Đã kiểm lại trên LUNA 5/2022, xem mục dưới.

### Kiểm tra ngoài mẫu trên LUNA 5/2022 (01/11/2026)

Giữ nguyên cấu hình chọn, không chỉnh sau khi xem kết quả:

```bash
python -m bdi.ingest.download --kind aggTrades klines --months 2022-05
python experiments/lake_windows.py candles --periods 2022-05 2026-06 2026-07 2026-08   # máy không có Postgres
python experiments/e7_anomaly.py --lake --month 2022-05 --event-days 2022-05-09 2022-05-10 2022-05-11 2022-05-12     --windows 1440 --thresholds 3.5 --floors 0.005 --tag luna
```

| Ngày | 9/5 | 10/5 | 11/5 | 12/5 | Hạng 5 (26/5) | Trung vị ngày |
|---|---|---|---|---|---|---|
| Báo động (5 cặp) | 237 | 82 | **384** | 106 | 80 | 23,5 |
| Hạng trong tháng | 2 | 4 | **1** | 3 | 5 | |

- Bốn ngày sự kiện là bốn hạng đầu của tháng. Ngày 10/5 chỉ hơn ngày hạng 5 hai báo động.
- `--lake` đọc nến từ `data/lake/gold/candles_batch`; với tháng 11/2022 cách đọc này tái lập **giống hệt** `sweep.csv` dựng từ Postgres.
- Kết quả: `results/e7/sweep_luna.csv`, `results/e7/alerts_per_day_luna.png`.

Hình: `results/e7/ftx_btc.png` (giá BTC + điểm bất thường) và `results/e7/alerts_per_day_nov22.png` (báo động theo ngày).

## Phần 2: Replay FTX qua streaming

```bash
experiments/e7_stress.sh ftx           # 6–10/11/2022 ở 100× (~58 phút; 6–7/11 là 2 ngày khởi động, vì MAD xấp xỉ cần 2W − 1 phút)
```

Cảnh báo được ghi vào bảng `anomalies` với `source='stream:e7_ftx_x100'` và hiện trên Grafana. So sánh với kết quả batch: có cùng phát hiện các mốc không, và phát hiện sau bao lâu.

### Kết quả replay FTX (14/10/2026, BTCUSDT 6–10/11/2022, 100×)

| Chỉ số | Giá trị |
|---|---|
| Bản tin phát lại | khoảng 41 triệu (2 đoạn: crash `BufferError` lúc 8/11 16:16, rồi phát tiếp từ aggId cuối đã vào Kafka) |
| **Nến stream so với official** | **5 758/5 758 phút khớp tuyệt đối** (6/11 00:00 → 9/11 23:58) |
| **Báo động stream so với batch** (cùng cấu hình, cùng khởi động từ 6/11) | **111/111 trùng tuyệt đối**: 0 chỉ có ở stream, 0 chỉ có ở batch |
| Báo động theo ngày (stream) | 8/11: 51 log-return + 14 khối lượng; 9/11: 46 log-return |

Hành trình chạy cũng là một bài kiểm tra chịu lỗi ngoài kế hoạch:
- producer replay chết vì `BufferError` (đã thêm backpressure, P14);
- job Spark không theo kịp 100× và bị dừng giữa chừng;
- lần chạy lại bị `OutOfMemoryError` vì lô 6,4 triệu bản tin, nên phải xóa file kế hoạch lô chưa commit (`offsets/215` + `.215.crc`) và chạy lại với `--max-offsets-per-trigger 500000`.

Dù vậy, kết quả cuối vẫn khớp tuyệt đối, nhờ checkpoint, `dropDuplicatesWithinWatermark` và ghi upsert.

**Throughput** (máy 16 GB, load 20–48, đang chạy song song luồng live, luồng C5 và một cụm khác):

| Chỉ số | Bản tin/giây |
|---|---|
| Lưu lượng live trung bình (5 cặp) | 18,6 (p95 = 58) |
| Tải replay đưa vào tối đa (100×, giờ cao điểm FTX) | 26 964 |
| Xử lý của Spark (lô 500k): trung vị / tối đa | **13 879 / 25 454** |

Pipeline xử lý được khoảng **750–1 370 lần** lưu lượng live trung bình. Ở 100× giờ cao điểm FTX, tải vượt khả năng xử lý trên máy này nên độ trễ tích lũy (job tụt lại phía sau). Stress test dưới đây nên chạy trên máy rảnh để có con số sạch.

## Phần 3: Stress test velocity

```bash
experiments/e7_stress.sh stress        # SPEEDS="1 10 50 100 200 500 1000", dừng khi p95 > 10s
```

- `results/e7/stress.csv` + `stress.png`: tải đưa vào, throughput xử lý tối đa, độ trễ p95 theo tốc độ
- Ghi lại **throughput tối đa (bản tin/giây)** và so với lưu lượng live trung bình (đọc từ `results/progress/live.jsonl`). Đây là con số chứng minh chữ V velocity.

## Checklist

- [x] Bảng quét tham số + hình FTX (cấu hình chọn: 8–9/11 là top-2 ngày)
- [x] Kiểm tra lại trên sự kiện khác (LUNA 5/2022): 9–12/5 là top-4 ngày của tháng
- [x] Replay 100× có cảnh báo trên stream: 111/111 trùng batch, nến 5 758/5 758 khớp official
- [x] Throughput sơ bộ: xử lý 13,9k (trung vị) – 25,5k (tối đa) bản tin/s, khoảng 750–1 370 lần lưu lượng live
- [ ] Stress test theo bậc tốc độ (`e7_stress.sh stress`) trên máy rảnh
- [ ] (nếu còn thời gian) Replay 1× cho 1 giờ quanh đỉnh 9/11
