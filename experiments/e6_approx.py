"""E6 — Truy vấn xấp xỉ vs chính xác trên toàn bộ silver trades (~420M dòng).

Hai phép tính, theo từng symbol:
  1. Đếm phân biệt ở 2 mức số giá trị phân biệt (cardinality):
       price  : thấp (vài chục nghìn/cặp)  — đếm chính xác vốn đã rẻ
       agg_id : cao (= số dòng, ~420M)     — nơi HyperLogLog++ phát huy tác dụng
     approx_count_distinct(col, rsd) với rsd ∈ {0.01, 0.05, 0.1}
  2. Phân vị [0.5, 0.95, 0.99] của giá trị giao dịch notional = price × qty (USDT):
       percentile_approx(accuracy = 1/relErr), relErr ∈ {0.001, 0.01, 0.05}
     Thước đo chính: sai số THỨ HẠNG |F(ước lượng) − q|, tính CHÍNH XÁC bằng một lượt
     đếm — đúng đại lượng mà relativeError của Spark cam kết. Sai số giá trị so với
     ước lượng mịn nhất (relErr = 0.001).
     percentile chính xác trên notional (hàng trăm triệu giá trị phân biệt mỗi cặp) bị
     OutOfMemoryError với heap 6 GB — chính là lý do cần xấp xỉ. Thử lại bằng --exact-quantile.

Không gộp qty của các cặp khác đơn vị (BTC vs XRP) — vô nghĩa.

    python experiments/e6_approx.py
    SPARK_MASTER=spark://<host>:7077 python experiments/e6_approx.py --tag 2workers   # mở rộng
"""

from __future__ import annotations

import argparse
import os
import time

import _common
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import functions as F

from bdi.config import path
from bdi.spark import get_spark

RSDS = [0.01, 0.05, 0.1]
REL_ERRS = [0.001, 0.01, 0.05]
QS = [0.5, 0.95, 0.99]


def timed(fn, repeats: int):
    best, out = None, None
    for _ in range(repeats):
        t = time.perf_counter()
        out = fn()
        dt = time.perf_counter() - t
        best = dt if best is None else min(best, dt)
    return out, best


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="local")
    ap.add_argument("--repeats", type=int, default=2)
    ap.add_argument("--exact-quantile", action="store_true", help="thử percentile chính xác (dễ OOM)")
    a = ap.parse_args()

    spark = get_spark("e6_approx")
    silver = path("lake") / "silver" / "trades"
    if os.name == "nt":
        # Windows không có winutils: Spark không liệt kê được thư mục, nên truyền danh sách file
        from lake_windows import lake_files
        reader = spark.read.option("basePath", str(silver)).parquet(*lake_files(silver))
    else:
        reader = spark.read.parquet(str(silver))
    df = (reader
          .select("symbol", "agg_id", "price", (F.col("price") * F.col("qty")).cast("double").alias("notional")))
    n = df.count()
    print(f"[e6] {n:,} dòng", flush=True)
    rows = []

    # ---- 1. đếm phân biệt ----
    for col in ("price", "agg_id"):
        def exact(col=col):
            return {r[0]: r[1] for r in df.groupBy("symbol").agg(F.countDistinct(col)).collect()}
        ex, t_ex = timed(exact, a.repeats)
        rows.append({"query": f"count_distinct({col})", "param": "exact", "rel_error_pct": 0.0,
                     "seconds": t_ex, "cardinality": sum(ex.values())})
        print(rows[-1], flush=True)
        for rsd in RSDS:
            def approx(col=col, rsd=rsd):
                return {r[0]: r[1] for r in df.groupBy("symbol").agg(F.approx_count_distinct(col, rsd)).collect()}
            est, t = timed(approx, a.repeats)
            err = max(abs(est[k] - ex[k]) / ex[k] for k in ex) * 100
            rows.append({"query": f"count_distinct({col})", "param": f"rsd={rsd}", "rel_error_pct": err,
                         "seconds": t, "cardinality": sum(ex.values())})
            print(rows[-1], flush=True)

    # ---- 2. phân vị notional theo symbol ----
    qs = F.array(*[F.lit(q) for q in QS])

    if a.exact_quantile:
        def exact_q():
            return {r[0]: r[1] for r in df.groupBy("symbol").agg(F.percentile("notional", qs)).collect()}
        try:
            _, t_ex = timed(exact_q, 1)
            rows.append({"query": "quantile(notional)", "param": "exact", "seconds": t_ex})
        except Exception as e:  # noqa: BLE001 — ghi nhận OOM như một kết quả
            rows.append({"query": "quantile(notional)", "param": "exact", "seconds": None,
                         "note": type(e).__name__ + ": OutOfMemory" if "OutOfMemory" in str(e) else str(e)[:80]})
        print(rows[-1], flush=True)
    ref = None
    for re_ in REL_ERRS:
        def approx_q(re_=re_):
            return {r[0]: r[1] for r in
                    df.groupBy("symbol").agg(F.percentile_approx("notional", qs, int(1 / re_))).collect()}
        est, t = timed(approx_q, a.repeats)
        ref = ref or est
        val_err = max(abs(e - x) / x for k in ref for e, x in zip(est[k], ref[k]) if x) * 100
        rows.append({"query": "quantile(notional)", "param": f"relErr={re_}", "rel_error_pct": val_err,
                     "rank_error_pct": _rank_error(df, est, QS) * 100, "seconds": t})
        print(rows[-1], flush=True)

    out = pd.DataFrame(rows).assign(n_rows=n, tag=a.tag)
    out.to_csv(_common.out_dir("e6") / f"approx_{a.tag}.csv", index=False)
    print(out.to_string(index=False))
    _plot(out, a.tag)


def _rank_error(df, est: dict, qs: list[float]) -> float:
    """max |F(ước lượng) − q| trên mọi symbol và q, với F là CDF thực nghiệm."""
    # mỗi (symbol, q) một cột; tính trong 1 lượt quét bằng điều kiện symbol
    exprs, idx = [], 0
    for k in sorted(est):
        for v in est[k]:
            exprs.append(F.avg(F.when(F.col("symbol") == k, (F.col("notional") <= F.lit(v)).cast("double")))
                         .alias(f"c{idx}"))
            idx += 1
    r = df.agg(*exprs).first()
    errs, idx = [], 0
    for k in sorted(est):
        for q in qs:
            errs.append(abs(r[f"c{idx}"] - q))
            idx += 1
    return max(errs)


def _plot(out: pd.DataFrame, tag: str) -> None:
    queries = list(dict.fromkeys(out["query"]))
    fig, axes = plt.subplots(1, len(queries), figsize=(5 * len(queries), 4))
    for ax, q in zip(axes, queries):
        d = out[out["query"] == q]
        ycol = "rank_error_pct" if q.startswith("quantile") else "rel_error_pct"
        d = d.dropna(subset=["seconds", ycol])
        ax.scatter(d["seconds"], d[ycol], color=_common.SERIES[0], s=40, zorder=3)
        for r in d.itertuples():
            ax.annotate(r.param, (r.seconds, getattr(r, ycol)), textcoords="offset points",
                        xytext=(6, 4), color=_common.TEXT_2, fontsize=9)
        ax.set_xlabel("thời gian (s, tốt nhất)")
        ax.set_ylabel("sai số thứ hạng lớn nhất (%)" if ycol == "rank_error_pct" else "sai số tương đối lớn nhất (%)")
        ax.set_title(f"E6 · {q}")
    _common.save(fig, "e6", f"error_vs_time_{tag}")


if __name__ == "__main__":
    main()
