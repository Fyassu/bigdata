"""E3 + C3 — Gắn nhãn nguyên nhân cho nến lệch của các lượt E3 có tiêm độ trễ (jitter).

`compare --with-late W` mô phỏng bản tin trễ theo thứ tự đến THẬT trong raw archive, nên
không thấy độ trễ do replay tiêm vào: nến lệch của lượt jitter sẽ thành UNKNOWN. Script
này dựng lại đúng thứ tự đến của lượt jitter (cùng iter_archive, cùng jitter() với
seed cố định như replay), rồi mô phỏng watermark và gắn nhãn bằng taxonomy C3.

Chạy trên máy có raw archive + bảng candles, với ĐÚNG ngày/khung đã truyền cho
e3_watermark_sweep.sh:

    python experiments/e3_late_labels.py 2026-10-13 2026-10-13T15:15 2026-10-13T15:45

Đọc các cấu hình jitter trong results/e3/configs/*.json (jitter_pct > 0).
Đầu ra: results/reconcile/<run_id>_late_{summary,causes}.csv cho từng watermark.
Số bản tin trễ mô phỏng là cận trên: Spark chỉ đẩy watermark giữa các micro-batch.
"""

from __future__ import annotations

import argparse
import json

import _common  # noqa: F401  (đặt sys.path tới src/)
import pandas as pd

from bdi.config import path
from bdi.ingest.replay import iter_archive, jitter
from bdi.reconcile.compare import compare, count_duplicates, load_candles, simulate_late, summarize
from bdi.reconcile.taxonomy import label
from bdi.timeutil import to_micros


def arrival_frame(events) -> pd.DataFrame:
    """Bản tin theo thứ tự đến (sau jitter), đủ cột cho simulate_late và count_duplicates."""
    rows = [(m["s"], m["a"], to_micros(m["T"])) for _, m in events]
    df = pd.DataFrame(rows, columns=["symbol", "agg_id", "ts_us"])
    df["minute"] = pd.to_datetime(df["ts_us"] // 60_000_000 * 60, unit="s", utc=True)
    # simulate_late sắp theo thứ tự đến Kafka; ở đây thứ tự đến chính là thứ tự dòng
    df["kafka_ts"] = df.index
    df["kafka_partition"] = 0
    df["kafka_offset"] = df.index
    return df


def window(run_id: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Cùng khung phút với e3_analyze.analyze()."""
    meta = json.loads((path("results") / "replay" / f"{run_id}.json").read_text())
    lo = pd.to_datetime(meta["origin_us"], unit="us", utc=True).ceil("min")
    span_us = (meta["replay_end_us"] - meta["replay_start_us"]) * meta["speed"]
    hi = pd.to_datetime(meta["origin_us"] + span_us, unit="us", utc=True).floor("min") - pd.Timedelta(minutes=1)
    return lo, hi


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("date", help="ngày trong archive, như tham số 1 của e3_watermark_sweep.sh")
    ap.add_argument("start", help="như tham số 2 của e3_watermark_sweep.sh")
    ap.add_argument("end", help="như tham số 3 của e3_watermark_sweep.sh")
    a = ap.parse_args()

    cfgs = [json.loads(p.read_text()) for p in sorted((path("results") / "e3" / "configs").glob("*.json"))]
    cfgs = [c for c in cfgs if c["jitter_pct"] > 0]
    if not cfgs:
        raise SystemExit("Không có cấu hình jitter nào trong results/e3/configs/")
    arrivals = {}
    out = path("results") / "reconcile"
    out.mkdir(parents=True, exist_ok=True)
    for c in sorted(cfgs, key=lambda c: c["watermark_s"]):
        key = (c["jitter_pct"], c["jitter_max_s"])
        if key not in arrivals:
            arrivals[key] = arrival_frame(jitter(iter_archive(a.date, a.start, a.end), *key))
        raw = arrivals[key]
        lo, hi = window(c["run_id"])
        # nạp cả phút dở dang trước lo để luật CAPTURE_START (phút sớm nhất có nến) rơi vào
        # phút đó, không rơi vào phút đầy đủ đầu tiên của khung; nhãn xong mới cắt về [lo, hi)
        pre = (lo - pd.Timedelta(minutes=1)).isoformat()
        stream = load_candles(c["source"], start=pre, end=hi.isoformat())
        official = load_candles("official", start=pre, end=hi.isoformat())
        m = compare(stream, official, "right")
        m["cause"] = label(m, None, simulate_late(raw, c["watermark_s"]), count_duplicates(raw))
        m = m[m["minute"] >= lo]
        causes = m.loc[~m["all_match"], "cause"].value_counts().rename_axis("cause").reset_index(name="n")
        summarize(m).to_csv(out / f"{c['run_id']}_late_summary.csv", index=False)
        causes.to_csv(out / f"{c['run_id']}_late_causes.csv", index=False)
        n_bad = int((~m["all_match"]).sum())
        print(f"[{c['run_id']}] W={c['watermark_s']}s: {n_bad}/{len(m)} nến lệch")
        print(causes.to_string(index=False) if n_bad else "  (không có nến lệch)")


if __name__ == "__main__":
    main()
