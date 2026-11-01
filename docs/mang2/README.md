# Mảng 2 · Bộ lệnh chạy trên máy dữ liệu

**Người chạy:** Đỗ Xuân Thanh (máy đang giữ luồng live) · **Soạn:** 01/11/2026 · **Đóng băng code:** T5 5/11 · **Báo cáo:** CN 8/11

Mọi lệnh chạy từ thư mục gốc repo, sau khi đã `source .venv/bin/activate`, `set -a && source .env && set +a`, `export PYTHONPATH=src`. Các truy vấn SQL nằm trong [kiem-tra.sql](kiem-tra.sql) và chỉ đọc, chạy bằng `make psql`.

## Lịch đề xuất

| Khi nào | Việc | Thời gian máy | Ghi chú |
|---|---|---|---|
| **Ngay hôm nay (CN 1/11)** | Bước 0: kiểm tra trạng thái; bước 1: E4 ngắt 24 giờ | 5 phút | Luồng live chạy từ 13/10 nên có thể đã có dòng `proactive_24h` |
| CN 1/11, trước 24:00 UTC | Bước 2: bật `stream-candles-update` cho ngày D | | **D = T2 2/11 (UTC)** |
| Cả ngày D (2/11 UTC) | Để yên máy: không dừng service, không rebuild container, không chạy việc nặng | 24 giờ | E6 (Huy) và stress test (Vũ) **không** chạy trong ngày D |
| T3 3/11, sau khi Binance công bố file ngày 2/11 | Bước 3: C5 đủ 24 giờ + cột batch | ~30 phút | Bắt buộc |
| T3 3/11, tối → T4 4/11 sáng | Bước 4: E3 cửa sổ 2 giờ, chạy qua đêm | 6–7 giờ | Nên làm; cắt đầu tiên nếu thiếu giờ |
| Bất kỳ lúc nào máy rảnh | Bước 5: số nhãn C3 cho lượt E3 jitter | vài phút | Cần cho bảng C3 |
| T4 4/11 | E6 của Huy, stress test của Vũ (lần lượt, không chồng) | | Sau khi bước 3 xong |
| T5 5/11 – T6 6/11 | Bước 6: tập demo 2 lần, quay video dự phòng 60 s | | Bắt buộc |

Không chạy hai việc nặng cùng lúc (bẫy P13, P16). Giữ máy không ngủ khi chạy: `caffeinate -dims &` (bẫy P12).

---

## Bước 0 · Kiểm tra trạng thái (5 phút)

```bash
docker compose ps          # ws-producer, raw-archive, stream-candles, official-poller: "Up"
make live-status           # last_minute của cả 5 cặp phải cách hiện tại < 2 phút
ls data/lake/bronze/raw_aggtrades/ | tail      # các ngày đã có trong raw archive
```

Chạy các truy vấn **K1–K3** trong `kiem-tra.sql`.

## Bước 1 · E4: ngắt tự nhiên sau 24 giờ

Chạy truy vấn **K2**.

- **Có dòng `proactive_24h`:** đối soát quanh thời điểm đó (thay `<T>` bằng `disconnected_at`, lấy ±30 phút):
  ```bash
  python -m bdi.reconcile.compare --left stream --start <T-30m> --end <T+30m> --run-id e4_proactive --with-late 5
  ```
  **Đạt khi:** `results/reconcile/e4_proactive_summary.csv` có `all_match = 100` ở cả 5 cặp, `missing_left = 0`.
  Cập nhật: tích mục cuối trong checklist của `docs/experiments/E4-fault-injection.md`, thêm một dòng vào bảng sự cố.
- **Không có dòng nào:** producer bị khởi động lại trước mốc 24 giờ (mỗi lần restart đặt lại đồng hồ). Để nguyên producer qua ngày D là sẽ có; kiểm lại ở bước 3.

## Bước 2 · Chuẩn bị ngày D cho C5

```bash
docker compose --profile c5 up -d stream-candles-update     # update mode, W = 30 s, ghi candle_versions
docker compose ps stream-candles-update                       # "Up"
```

Sau 5 phút, chạy truy vấn **K3**: phải thấy `stream:update` có nến mới.

Trong suốt ngày D: không `docker compose down`, không rebuild image (thay đổi `src/bdi/timeutil.py` của Mảng 1 chỉ được merge sau khi C5 xong).

## Bước 3 · C5 đủ 24 giờ + cột batch (bắt buộc)

Chạy khi file ngày 2/11 đã có trên `data.binance.vision` (thường trong buổi sáng T+1 giờ Việt Nam).

```bash
python -m bdi.ingest.download --days 2026-11-02            # aggTrades + klines của 5 cặp, kiểm sha256
python -m bdi.batch.load_klines --periods 2026-11-02
python -m bdi.batch.build_batch --periods 2026-11-02
python experiments/c5_source_policy.py --start 2026-11-02 --end 2026-11-03 --sources stream stream:update batch
```

**Đạt khi:**
- `results/c5/source_quality.csv` có đủ 3 dòng `stream`, `stream:update`, `batch`;
- completeness của cả 3 nguồn là 100%, hoặc phần thiếu giải thích được bằng `ws_gaps` (truy vấn K2);
- dòng log `[c5] hội tụ …%` in ra p50 và p95.

**Cập nhật cùng một commit:** `results/c5/*`, bảng kết quả trong `docs/experiments/C5-source-policy.md` (bỏ chữ "sơ bộ", thêm cột batch), mục C5 trong `docs/05-ket-qua.md` (bỏ ý 5 ở phần Hạn chế).

## Bước 4 · E3 trên cửa sổ 2 giờ (nên làm)

### 4a. Cất kết quả 30 phút trước khi chạy (bắt buộc nếu chạy)

Lượt mới dùng lại đúng các run-id cũ (`e3_wm0`, `e3_wm0_j5x20`, …), nên sẽ:
- ghi đè `results/e3/configs/*.json` và `results/replay/e3_*.json`;
- **ghi nối thêm** vào `results/progress/e3_*.jsonl`. Nếu không cất file cũ đi, cột `late_rows_dropped` sẽ cộng cả hai lượt và cho số sai.

```bash
mkdir -p results/e3_30m/configs results/e3_30m/replay results/e3_30m/progress
mv results/e3/configs/*.json      results/e3_30m/configs/
cp results/replay/e3_*.json       results/e3_30m/replay/
mv results/progress/e3_*.jsonl    results/e3_30m/progress/
cp results/e3/tradeoff.csv results/e3/tradeoff.png results/e3_30m/
```

### 4b. Chạy

```bash
python -m bdi.ingest.rest_klines --start 2026-11-02T13:30 --end 2026-11-02T15:30    # official cho khung này (bỏ qua nếu bước 3 đã nạp klines ngày 2/11)
GROUP=2 experiments/e3_watermark_sweep.sh 2026-11-02 2026-11-02T13:30 2026-11-02T15:30
JITTER_PCT=5 JITTER_MAX=20 GROUP=3 experiments/e3_watermark_sweep.sh 2026-11-02 2026-11-02T13:30 2026-11-02T15:30
python experiments/e3_analyze.py
```

**Đạt khi:**
- trong `results/e3/logs/` và log replay, dòng `trễ so với nhịp mục tiêu` luôn < 1 s;
- `results/e3/tradeoff.csv` có `n_official` khoảng 118 × 5 = 590 mỗi cấu hình (2 giờ trừ phút đầu và phút cuối);
- dữ liệu thật: % khớp ≈ 100% ở mọi W; jitter: % khớp thấp ở W nhỏ và 100% ở W = 30 s.

**Cập nhật:** bảng trong `docs/experiments/E3-watermark-tradeoff.md` (giữ bảng 30 phút làm phụ lục), mục E3 trong `docs/05-ket-qua.md` (bỏ ý 2 ở phần Hạn chế).

## Bước 5 · Số nhãn C3 cho lượt E3 jitter

Bảng C3 hiện chưa có nến nào mang nhãn `LATE_BEYOND_WATERMARK`, vì chưa ai chạy đối soát trên nguồn jitter.

**Không dùng `compare --with-late` cho việc này.** Lệnh đó mô phỏng bản tin trễ theo thứ tự đến *thật* trong raw archive. Độ trễ của lượt jitter được tiêm lúc replay nên lệnh không thấy, và nến lệch sẽ bị gắn `UNKNOWN`. `experiments/e3_late_labels.py` dựng lại đúng thứ tự đến của lượt jitter (cùng `jitter()`, cùng seed 42), rồi mới mô phỏng watermark và gắn nhãn. Truyền **đúng** ba tham số đã dùng cho `e3_watermark_sweep.sh`:

```bash
# nếu đã chạy bước 4 (cửa sổ 2 giờ):
python experiments/e3_late_labels.py 2026-11-02 2026-11-02T13:30 2026-11-02T15:30
# nếu chưa, dùng lượt 30 phút cũ (configs đang nằm trong results/e3/configs/):
python experiments/e3_late_labels.py 2026-10-13 2026-10-13T15:15 2026-10-13T15:45
```

**Đạt khi:**
- với mỗi W có một dòng `[e3_wm<W>_j5x20] W=…: <lệch>/<tổng> nến lệch`;
- số nến lệch gần khớp cột `pct_match` của `results/e3/tradeoff.csv` cho series jitter (ví dụ W = 0 s khớp 2,1% thì khoảng 98% nến lệch);
- gần như mọi nến lệch mang nhãn `LATE_BEYOND_WATERMARK`; `UNKNOWN` bằng 0, hoặc ghi rõ số lượng nếu khác 0.

Gửi các file `results/reconcile/e3_wm*_j5x20_late_causes.csv` cho người viết C3. Script đã được kiểm tra offline bằng dữ liệu giả (thứ tự đến tái lập được; W = 30 s không còn nến lệch), nhưng chưa chạy trên dữ liệu thật.

## Bước 6 · Demo

Theo [04-demo.md](../04-demo.md). Thêm:
- Quay video dự phòng 60 s: Grafana `/d/bdi-live`, chạy `experiments/e4_chaos/chaos.sh ws_netcut`, bảng "Khoảng trống WS + backfill" có dòng mới, đường stream vẫn trùng official.
- Hôm trước buổi bảo vệ, thử `curl -sI https://stream.binance.com:9443` từ mạng hội trường hoặc mạng điện thoại. Nếu bị chặn, đổi service `ws-producer` sang `--fallback`.

---

## Sau khi chạy xong

```bash
make results          # nạp results/*.csv vào Postgres, dựng lại dashboard "Kết quả thí nghiệm"
```

Mỗi bước một nhánh (`mang2/c5-24h`, `mang2/e3-2h`, …). Commit `results/` cùng tài liệu tương ứng trong một commit, rồi mở PR.

Bản nháp báo cáo C3, C5, E3, E4 giữ ngoài repo (không thuộc phần chạy project), Minh gửi riêng. Các chỗ ghi **⟨chờ bước N⟩** trong nháp sẽ điền số sau khi chạy bước tương ứng ở trên.
