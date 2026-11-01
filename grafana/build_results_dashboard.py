"""Sinh grafana/dashboards/bdi_results.json — dashboard "BDI · Kết quả thí nghiệm".

    python grafana/build_results_dashboard.py

Dữ liệu: các bảng exp_* do experiments/publish_results.py nạp vào Postgres.
Màu theo bảng màu cố định của dự án (xem experiments/_common.py).
"""

import json
from pathlib import Path

DS = {"type": "grafana-postgresql-datasource", "uid": "bdi-pg"}
BLUE, ORANGE, AQUA, YELLOW, PINK = "#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"
_id = iter(range(1, 100))


def target(sql, fmt="table"):
    return [{"refId": "A", "datasource": DS, "format": fmt, "rawQuery": True, "editorMode": "code", "rawSql": sql}]


def colors(mapping):
    return [{"matcher": {"id": "byName", "options": n},
             "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": c}}]} for n, c in mapping.items()]


def stat(title, sql, x, y, w=6, h=4, unit="none", desc="", color=BLUE, decimals=None):
    d = {"unit": unit, "color": {"mode": "fixed", "fixedColor": color}}
    if decimals is not None:
        d["decimals"] = decimals
    return {"id": next(_id), "type": "stat", "title": title, "description": desc, "datasource": DS,
            "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": target(sql),
            "fieldConfig": {"defaults": d, "overrides": []},
            "options": {"colorMode": "value", "graphMode": "none", "textMode": "value",
                        "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}}}


def bars(title, sql, x, y, w, h, unit="none", series=None, desc="", stacked=False, minv=None, maxv=None):
    d = {"unit": unit, "custom": {"fillOpacity": 90, "lineWidth": 0}}
    if minv is not None:
        d["min"] = minv
    if maxv is not None:
        d["max"] = maxv
    return {"id": next(_id), "type": "barchart", "title": title, "description": desc, "datasource": DS,
            "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": target(sql),
            "fieldConfig": {"defaults": d, "overrides": colors(series or {})},
            "options": {"xField": "x", "orientation": "vertical", "showValue": "auto",
                        "stacking": "normal" if stacked else "none", "groupWidth": 0.7, "barWidth": 0.9,
                        "xTickLabelRotation": 0, "legend": {"showLegend": True, "displayMode": "list",
                                                            "placement": "bottom"},
                        "tooltip": {"mode": "multi"}}}


def table(title, sql, x, y, w, h, desc=""):
    return {"id": next(_id), "type": "table", "title": title, "description": desc, "datasource": DS,
            "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": target(sql),
            "options": {"showHeader": True, "cellHeight": "sm"}, "fieldConfig": {"defaults": {}, "overrides": []}}


def text(md, x, y, w, h):
    return {"id": next(_id), "type": "text", "title": "", "gridPos": {"x": x, "y": y, "w": w, "h": h},
            "options": {"mode": "markdown", "content": md}}


def row(title, y):
    return {"id": next(_id), "type": "row", "title": title, "collapsed": False, "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}}


P = []
P.append(text(
    "### Kết quả thí nghiệm · Nhóm 3 · IT5427\n"
    "Số liệu từ `results/*.csv`, nạp bằng `python experiments/publish_results.py`. "
    "Luồng live: dashboard **[BDI · Binance batch + streaming](/d/bdi-live)**. "
    "Chi tiết: `docs/05-ket-qua.md`.", 0, 0, 24, 2))

# --- hàng số chính
P += [
    stat("E2 · nến batch khớp tuyệt đối official",
         "SELECT round(sum(all_match * n_minutes / 100)) AS v FROM exp_e2", 0, 2, desc="5 cặp × 4 tháng, mọi trường O/H/L/C/V + trades"),
    stat("E4 · nến thiếu / thừa / lệch qua sự cố",
         "SELECT sum(missing_minutes + extra_trades_minutes + wrong_minutes) AS v FROM exp_e4", 6, 2, color=AQUA,
         desc="4 kịch bản chủ động; thêm sự cố máy ngủ 5,7 giờ cũng 0"),
    stat("E5 · F1 tốt nhất (ghép mờ)", "SELECT max(f1) AS v FROM exp_e5", 12, 2, decimals=3,
         desc="Ghép giữ thứ tự, δ = 1 s"),
    stat("E7 · báo động stream trùng batch",
         'SELECT "both" AS v FROM exp_e7_sb', 18, 2, color=ORANGE,
         desc="Replay FTX 100×: 111/111 trùng batch"),
]

y = 6
P.append(row("Dữ liệu · thống kê mô tả (5 cặp × 4 tháng)", y)); y += 1
P.append(table("Quy mô theo cặp × tháng", """
SELECT symbol AS "Cặp", period AS "Tháng", n_aggtrades AS "aggTrade", n_trades AS "Trade lẻ",
       round(trades_per_agg::numeric, 2) AS "Trade/aggTrade",
       round((quote_usdt_per_day / 1e6)::numeric, 0) AS "Triệu USDT/ngày",
       trades_per_min_p50 AS "Trade/phút p50", trades_per_min_max AS "Trade/phút max",
       round(taker_buy_pct_trades::numeric, 1) AS "Mua chủ động % (trade)",
       round(taker_buy_pct_quote::numeric, 1) AS "Mua chủ động % (USDT)",
       pct_match_trades AS "Khớp klines: trades %", pct_match_quote AS "quote %", pct_match_taker_buy AS "mua chủ động %"
FROM exp_desc ORDER BY 1, 2""", 0, y, 24, 8,
    desc="experiments/descriptive_stats.py: số trade, quote volume, khối lượng mua chủ động so tuyệt đối với klines")); y += 8

P.append(row("E1 · Schema drift   ·   E2 · Batch vs official", y)); y += 1
P.append(bars("E1 · % nến đúng: naive vs drift-aware", """
SELECT symbol || ' ' || period AS x,
       max(CASE WHEN version = 'naive' THEN pct_candle_ok END) AS "Naive (luôn coi là ms)",
       max(CASE WHEN version = 'drift_aware' THEN pct_candle_ok END) AS "Drift-aware"
FROM exp_e1 GROUP BY 1 ORDER BY 1""", 0, y, 12, 8, unit="percent", minv=0, maxv=100,
    series={"Naive (luôn coi là ms)": ORANGE, "Drift-aware": BLUE},
    desc="Naive hỏng toàn bộ 2026 (timestamp µs), vẫn đúng 2022 (ms)"))
P.append(table("E2 · % khớp từng trường theo cặp × tháng", """
SELECT symbol AS "Cặp", month AS "Tháng", n_minutes AS "Số nến",
       match_open AS "O %", match_high AS "H %", match_low AS "L %", match_close AS "C %",
       match_volume AS "V %", match_trades AS "Trades %", all_match AS "Tất cả %"
FROM exp_e2 ORDER BY 1, 2""", 12, y, 12, 8)); y += 8

P.append(row("C2 · Phản chứng: bỏ từng điểm kỹ thuật của build_candles", y)); y += 1
P.append(bars("% phút còn khớp official khi làm sai (trade đã xáo thứ tự)", """
SELECT replace(symbol, 'USDT', '') AS x,
       round((sum(pct_ok_6 * n_official) / sum(n_official))::numeric, 1) AS "Đúng (build_candles)",
       round((sum(pct_open_first * n_official) / sum(n_official))::numeric, 1) AS "Open = first",
       round((sum(pct_close_last * n_official) / sum(n_official))::numeric, 1) AS "Close = last",
       round((sum(pct_vol_double * n_official) / sum(n_official))::numeric, 1) AS "Volume double",
       round((sum(pct_trades_rows * n_official) / sum(n_official))::numeric, 1) AS "trades = số dòng"
FROM exp_c2 GROUP BY symbol ORDER BY symbol""", 0, y, 16, 8, unit="percent", minv=0, maxv=100,
    series={"Đúng (build_candles)": BLUE, "Open = first": ORANGE, "Close = last": YELLOW,
            "Volume double": PINK, "trades = số dòng": AQUA},
    desc="experiments/c2_counterfactual.py trên 878 400 nến; trung bình có trọng số theo số nến"))
P.append(table("quote_volume: tích Decimal bị làm tròn", """
SELECT symbol AS "Cặp",
       round((sum(pct_quote_current * n_official) / sum(n_official))::numeric, 2) AS "Hiện tại %",
       round((sum(pct_quote_fixed * n_official) / sum(n_official))::numeric, 2) AS "decimal(18,8)² %"
FROM exp_c2 GROUP BY symbol ORDER BY symbol""", 16, y, 8, 8,
    desc="decimal(24,8) × decimal(24,8) bị làm tròn về 6 chữ số lẻ; ép về decimal(18,8) trước khi nhân thì chính xác")); y += 8

P.append(row("E3 · Trade-off watermark", y)); y += 1
P.append(bars("E3 · % nến khớp official theo watermark", """
SELECT watermark_s || ' s' AS x,
       max(CASE WHEN jitter_pct = 0 AND trigger_s = 2 THEN pct_match END) AS "Dữ liệu thật",
       max(CASE WHEN jitter_pct > 0 THEN pct_match END) AS "5% bản tin trễ ≤ 20 s"
FROM exp_e3 GROUP BY watermark_s ORDER BY watermark_s""", 0, y, 12, 8, unit="percent", minv=0, maxv=100,
    series={"Dữ liệu thật": BLUE, "5% bản tin trễ ≤ 20 s": ORANGE},
    desc="Dữ liệu thật lệch thứ tự tối đa 0,78 s nên mọi watermark đều 100%"))
P.append(bars("E3 · Độ trễ ghi nến p95 theo watermark", """
SELECT watermark_s || ' s' AS x,
       max(CASE WHEN jitter_pct = 0 AND trigger_s = 2 THEN latency_p95_s END) AS "Dữ liệu thật",
       max(CASE WHEN jitter_pct > 0 THEN latency_p95_s END) AS "5% bản tin trễ ≤ 20 s"
FROM exp_e3 GROUP BY watermark_s ORDER BY watermark_s""", 12, y, 12, 8, unit="s",
    series={"Dữ liệu thật": BLUE, "5% bản tin trễ ≤ 20 s": ORANGE})); y += 8

P.append(row("E4 · Fault injection", y)); y += 1
P.append(table("E4 · Sự cố chủ động", """
SELECT scenario AS "Kịch bản", round(down_s::numeric, 0) AS "Lỗi (s)", round(recovery_s::numeric, 1) AS "Phục hồi (s)",
       backfilled_records AS "Backfill", official_minutes AS "Phút official",
       missing_minutes AS "Thiếu", extra_trades_minutes AS "Thừa", wrong_minutes AS "Lệch"
FROM exp_e4 ORDER BY inject""", 0, y, 14, 6))
P.append(table("E4 · Khoảng trống WS đã backfill (gần nhất)", """
SELECT symbol AS "Cặp", disconnected_at AS "Ngắt lúc", reconnected_at AS "Nối lại", backfilled AS "Backfill", reason AS "Lý do"
FROM ws_gaps ORDER BY disconnected_at DESC LIMIT 20""", 14, y, 10, 6)); y += 6

P.append(row("E5 · Record linkage   ·   E6 · Truy vấn xấp xỉ", y)); y += 1
P.append(bars("E5 · F1 theo dung sai δ", """
SELECT delta_ms || ' ms' AS x,
       max(CASE WHEN method = 'greedy' AND strategy = 'second' THEN f1 END) AS "Tham lam theo |Δt|",
       max(CASE WHEN method = 'ordered' THEN f1 END) AS "Giữ thứ tự"
FROM exp_e5 GROUP BY delta_ms ORDER BY delta_ms""", 0, y, 12, 8, minv=0, maxv=1,
    series={"Tham lam theo |Δt|": ORANGE, "Giữ thứ tự": BLUE}))
P.append(table("E6 · Chính xác vs xấp xỉ (418,2M dòng, máy rảnh)", """
SELECT query AS "Phép tính", param AS "Cấu hình", round(seconds::numeric, 1) AS "Thời gian (s)",
       round(rel_error_pct::numeric, 2) AS "Sai số giá trị %", round(rank_error_pct::numeric, 3) AS "Sai số thứ hạng %"
FROM exp_e6 ORDER BY 1, 2""", 12, y, 12, 8,
    desc="Đo lại 01/11 khi máy rảnh, tốt nhất 3 lần. Lần đo cũ lúc máy tải cao: bảng exp_e6_busy. "
         "Phân vị chính xác trên notional: OutOfMemoryError (6 GB)")); y += 8

P.append(row("E7 · Phát hiện bất thường   ·   C5 · Chọn nguồn", y)); y += 1
P.append(bars("E7 · Báo động log-return theo ngày, 11/2022 (5 cặp)", """
SELECT day AS x, CASE WHEN ftx THEN alerts END AS "8–9/11 (FTX)", CASE WHEN NOT ftx THEN alerts END AS "Ngày khác"
FROM exp_e7_daily ORDER BY day""", 0, y, 16, 8, stacked=True,
    series={"8–9/11 (FTX)": ORANGE, "Ngày khác": BLUE},
    desc="Modified z-score, baseline 1 ngày, |M| > 3,5, sàn |log-return| ≥ 0,5%"))
P.append(table("C5 · Chất lượng nguồn", """
SELECT source AS "Nguồn", completeness_pct AS "Completeness %", consistency_pct AS "Consistency %",
       round(timeliness_p50_s::numeric, 1) AS "Timeliness p50 (s)", round(timeliness_p95_s::numeric, 1) AS "p95 (s)"
FROM exp_c5""", 16, y, 8, 8)); y += 8
P.append(table("E7 · Quét tham số", """
SELECT "window" AS "W (phút)", threshold AS "|M| >", min_abs_return AS "Sàn", ftx_8 AS "8/11", ftx_9 AS "9/11",
       ftx_rank_8 AS "Hạng 8/11", ftx_rank_9 AS "Hạng 9/11",
       round(per_day_nov22::numeric, 1) AS "/ngày 11/2022", round(per_day_quiet26::numeric, 1) AS "/ngày yên 2026"
FROM exp_e7_sweep ORDER BY 1, 2, 3""", 0, y, 24, 8)); y += 8
P.append(bars("E7 · Kiểm tra ngoài mẫu: báo động theo ngày, LUNA 5/2022 (giữ nguyên tham số)", """
SELECT day AS x, CASE WHEN event THEN alerts END AS "9–12/5 (LUNA)", CASE WHEN NOT event THEN alerts END AS "Ngày khác"
FROM exp_e7_luna_daily ORDER BY day""", 0, y, 16, 8, stacked=True,
    series={"9–12/5 (LUNA)": ORANGE, "Ngày khác": BLUE},
    desc="Cấu hình chọn trên FTX, không chỉnh: W = 1 ngày, |M| > 3,5, sàn 0,5%"))
P.append(table("E7 · LUNA: 6 ngày nhiều báo động nhất", """
SELECT day AS "Ngày", alerts AS "Báo động", rank() OVER (ORDER BY alerts DESC) AS "Hạng",
       CASE WHEN event THEN 'sự kiện' ELSE '' END AS " "
FROM exp_e7_luna_daily ORDER BY alerts DESC LIMIT 6""", 16, y, 8, 8))

dash = {"uid": "bdi-results", "title": "BDI · Kết quả thí nghiệm", "timezone": "utc", "schemaVersion": 39,
        "time": {"from": "now-6h", "to": "now"}, "refresh": "", "panels": P, "editable": True,
        "links": [{"title": "Luồng live", "type": "link", "url": "/d/bdi-live"}]}
out = Path(__file__).parent / "dashboards" / "bdi_results.json"
out.write_text(json.dumps(dash, ensure_ascii=False, indent=1))
print(f"[ok] {out} · {len(P)} panel")
