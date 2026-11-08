# E2 · Đối soát batch vs official

**Câu hỏi:** Nến dựng từ aggTrades có khớp tuyệt đối với klines chính thức không?
**Chứng minh cho:** C2, C3 · **Phụ trách:** Mảng 1 · **Không được cắt**

## Chạy

```bash
python -m bdi.batch.load_klines
python -m bdi.batch.build_batch
python -m bdi.reconcile.compare --left batch --right official --run-id e2
python experiments/e2_batch_vs_official.py --run-id e2
# hoặc gộp:  make official batch e2
```

## Đầu ra

| File | Nội dung |
|---|---|
| `results/reconcile/e2_summary.csv` = `results/e2/match_by_field.csv` | % khớp từng trường O, H, L, C, V, trades và `all_match`, theo symbol × tháng |
| `results/e2/heatmap_all_match.png` | Heatmap cặp × tháng |
| `results/e2/causes.png` + `e2_causes.csv` | Phân bố nhãn nguyên nhân |
| `results/reconcile/e2_mismatches.csv.gz` | Danh sách từng nến lệch, để soi tay |
| Bảng `reconcile_results` (run_id='e2') | Truy vấn SQL tùy ý |

## Phân tích nến lệch

Đối với batch, các nhãn `LATE_*`, `DISCONNECT_*` không áp dụng. Lệch thường rơi vào:

- `EMPTY_MINUTE`: phút official có `trades = 0` nhưng batch không có nến (không có trade nào thì không có dòng để group). **Cần trả lời:** Binance có sinh nến volume 0 không? Truy vấn kiểm tra:
  ```sql
  SELECT symbol, count(*) FROM candles WHERE source='official' AND trades = 0 GROUP BY 1;
  ```
  Kết luận (13/10/2026): 5 cặp × 4 tháng official đều đủ phút và **không có phút `trades = 0`**. Binance có sinh nến cho phút trống hay không vẫn chưa kiểm được với các cặp này; muốn trả lời dứt điểm thì cần một cặp thanh khoản thấp.
- `UNKNOWN`: phải soi từng trường hợp. Nếu số lượng đáng kể, bổ sung luật vào `taxonomy.py`.

## Kết quả sơ bộ

| Cặp | Tháng | Số nến | % khớp tuyệt đối (O, H, L, C, V, trades) |
|---|---|---|---|
| 5 cặp (BNB, BTC, ETH, SOL, XRP) | 2022-11, 2026-06, 2026-07, 2026-08 | **878 400** | **100%** ở cả 6 trường được so (O, H, L, C, V, trades), 0 nến thiếu ở cả hai phía |

`quote_volume` không nằm trong phép so của `compare`. Đo riêng ở phản chứng C2 (`results/c2/counterfactual.csv`): BTCUSDT chỉ khớp 0,19% do Spark làm tròn tích Decimal, bốn cặp còn lại 100% (xem [05-ket-qua.md](../05-ket-qua.md) mục C2).

Batch dựng từ 418 156 831 aggTrades (~70 phút trên máy 16 GB, local[*]). Không có nến lệch nên không có nhãn nguyên nhân nào; hình `causes.png` không được tạo. Kết quả này chứng minh C2: `min_by`/`max_by`, Decimal và `sum(last − first + 1)` tái tạo chính xác klines của Binance.

## Checklist

- [x] Heatmap 5 cặp × 4 tháng (`results/e2/heatmap_all_match.png`). Vì mọi ô đều 100%, trong báo cáo nên trình bày bằng bảng thay vì heatmap
- [x] Bảng % khớp từng trường (`results/e2/match_by_field.csv`)
- [x] Trả lời câu hỏi về nến volume 0 (xem trên)
- [x] Mọi nến lệch đều có nhãn (không có nến lệch)
