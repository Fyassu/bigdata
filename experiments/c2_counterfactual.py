"""C2 — Bằng chứng phản chứng: bỏ từng điểm kỹ thuật của build_candles thì hỏng bao nhiêu.

Tiền điều kiện: đã tải aggTrades + klines (bdi.ingest.download). Không cần Kafka hay Postgres.

    python experiments/c2_counterfactual.py --symbols BTCUSDT ETHUSDT BNBUSDT SOLUSDT XRPUSDT \
        --periods 2022-11 2026-06 2026-07 2026-08
    python experiments/c2_counterfactual.py --symbols SOLUSDT --periods 2026-08    # chạy thử
    python experiments/c2_counterfactual.py --table-only                           # chỉ in lại bảng

Luôn truyền --periods (cùng lý do với E1: manifest trên máy dữ liệu có thêm file theo ngày).
Bước xáo ghi toàn bộ trade ra đĩa tạm (BTCUSDT 2022-11 khoảng 172 triệu dòng): đặt
SPARK_LOCAL_DIRS về ổ còn nhiều chỗ trống.

Mỗi file được quét MỘT lần. Trade được xáo thứ tự (repartition round-robin) trước khi gộp,
giống thứ tự đến không bảo đảm của streaming; mọi biến thể tính trong cùng một groupBy.
Mẫu số là số nến official; nến dựng thiếu tính là không khớp.

Đầu ra: results/c2/counterfactual.csv  (dòng = symbol × kỳ)
  pct_ok_6            : build_candles đúng, khớp O/H/L/C/V/trades (kiểm chứng, phải = 100)
  pct_open_first      : Open bằng first(price) trên dữ liệu đã xáo          — điểm (1)
  pct_close_last      : Close bằng last(price)                               — điểm (1)
  pct_vol_double      : volume = sum(double), so bằng tuyệt đối               — điểm (2)
  pct_vol_double_r8   : như trên nhưng làm tròn 8 chữ số lẻ trước khi so
  pct_trades_rows     : trades = số dòng aggTrade (count)                     — điểm (3)
  trades_per_agg      : trung vị số trade lẻ trong mỗi aggTrade (theo phút)
  pct_quote_current   : quote_volume như build_candles hiện tại (tích bị làm tròn về 6 chữ số lẻ)
  pct_quote_fixed     : quote_volume với tích decimal(18,8) × decimal(18,8) = decimal(37,16)
"""

from __future__ import annotations

import argparse

import _common
import pandas as pd

PCT_COLS = ["pct_ok_6", "pct_open_first", "pct_close_last", "pct_vol_double", "pct_vol_double_r8",
            "pct_trades_rows", "pct_quote_current", "pct_quote_fixed"]


def run(symbols: list[str] | None, periods: list[str] | None) -> pd.DataFrame:
    # Import Spark ở đây để --table-only chạy được trên máy không có Java/pyspark
    from pyspark.sql import functions as F

    from bdi.batch.build_batch import read_trades, select_files
    from bdi.schema import KLINES_CSV_SCHEMA, PRICE, QUOTE_VOLUME, VOLUME, official_from_klines_csv
    from bdi.spark import get_spark

    spark = get_spark("c2_counterfactual")
    n_parts = int(spark.conf.get("spark.sql.shuffle.partitions"))
    klines = {(e["symbol"], e["period"]): e for e in select_files("klines", symbols, periods)}
    rows = []
    for e in select_files("aggTrades", symbols, periods):
        k = klines.get((e["symbol"], e["period"]))
        if not k:
            print(f"[skip] {e['symbol']} {e['period']}: thiếu klines")
            continue
        official = official_from_klines_csv(
            spark.read.csv(k["csv"], schema=KLINES_CSV_SCHEMA), k["symbol"])
        t = (read_trades(spark, e)
             .select("symbol", "agg_id", "price", "qty", "first_trade_id", "last_trade_id",
                     "ts_us", "event_time")
             .repartition(n_parts))
        order_key = F.struct(F.col("ts_us"), F.col("agg_id"))
        price, qty = F.col("price"), F.col("qty")
        # Cùng các phép gộp với build_candles (đúng), thêm các biến thể sai bên cạnh
        c = (t.groupBy(F.col("symbol"), F.window(F.col("event_time"), "1 minute").alias("w"))
             .agg(F.min_by("price", order_key).alias("open"),
                  F.max("price").alias("high"),
                  F.min("price").alias("low"),
                  F.max_by("price", order_key).alias("close"),
                  F.sum("qty").cast(VOLUME).alias("volume"),
                  F.sum(F.col("last_trade_id") - F.col("first_trade_id") + 1).alias("trades"),
                  F.first("price").alias("open_first"),
                  F.last("price").alias("close_last"),
                  F.sum(qty.cast("double")).alias("vol_double"),
                  F.count(F.lit(1)).alias("n_agg"),
                  F.sum(price * qty).cast(QUOTE_VOLUME).alias("quote_current"),
                  F.sum(price.cast("decimal(18,8)") * qty.cast("decimal(18,8)"))
                  .cast(QUOTE_VOLUME).alias("quote_fixed"))
             .withColumn("minute", F.col("w.start"))
             .drop("w"))
        j = official.alias("o").join(c.alias("c"), ["symbol", "minute"], "left")

        def eq(left, right):
            return F.coalesce(left == right, F.lit(False))

        o = lambda f: F.col(f"o.{f}")  # noqa: E731
        ok6 = F.lit(True)
        for f in ["open", "high", "low", "close", "volume", "trades"]:
            ok6 = ok6 & eq(o(f), F.col(f"c.{f}"))
        checks = {
            "pct_ok_6": ok6,
            "pct_open_first": eq(o("open"), F.col("open_first").cast(PRICE)),
            "pct_close_last": eq(o("close"), F.col("close_last").cast(PRICE)),
            "pct_vol_double": eq(o("volume").cast("double"), F.col("vol_double")),
            "pct_vol_double_r8": eq(o("volume"), F.round("vol_double", 8).cast(VOLUME)),
            "pct_trades_rows": eq(o("trades"), F.col("n_agg")),
            "pct_quote_current": eq(o("quote_volume"), F.col("quote_current")),
            "pct_quote_fixed": eq(o("quote_volume"), F.col("quote_fixed")),
        }
        stats = j.agg(
            F.count(F.lit(1)).alias("n_official"),
            *[(100 * F.avg(v.cast("int"))).alias(k) for k, v in checks.items()],
            F.percentile_approx(o("trades") / F.col("n_agg"), 0.5).alias("trades_per_agg"),
        ).first()
        row = {"symbol": e["symbol"], "period": e["period"], **stats.asDict()}
        rows.append(row)
        print(row)
    return pd.DataFrame(rows)


def table(df: pd.DataFrame) -> str:
    """Bảng markdown gộp theo cặp (trọng số = số nến official), dùng cho báo cáo C2."""
    d = df.copy()
    for col in PCT_COLS:
        d[col] = d[col] * d["n_official"] / 100
    g = d.groupby("symbol").agg({**{col: "sum" for col in PCT_COLS}, "n_official": "sum",
                                 "trades_per_agg": "median"})
    g.loc["Tổng"] = [*(d[col].sum() for col in PCT_COLS), d["n_official"].sum(), d["trades_per_agg"].median()]
    for col in PCT_COLS:
        g[col] = 100 * g[col] / g["n_official"]
    head = ["Cặp", "Nến", "Đúng (6 trường)", "Open = first", "Close = last", "Volume double",
            "Double làm tròn 8 số", "trades = số dòng", "Trade/aggTrade", "quote_volume hiện tại",
            "quote_volume đã sửa"]
    lines = ["| " + " | ".join(head) + " |", "|" + "---|" * len(head)]
    for sym, r in g.iterrows():
        n = f"{int(r['n_official']):,}".replace(",", " ")
        pcts = [f"{r[col]:.2f}%".replace(".", ",") for col in PCT_COLS]
        tpa = f"{r['trades_per_agg']:.2f}".replace(".", ",")
        lines.append("| " + " | ".join([sym.replace("USDT", ""), n, *pcts[:6], tpa, *pcts[6:]]) + " |")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", nargs="*")
    ap.add_argument("--periods", nargs="*")
    ap.add_argument("--table-only", action="store_true", help="chỉ in lại bảng từ results/c2/counterfactual.csv")
    a = ap.parse_args()

    out = _common.out_dir("c2") / "counterfactual.csv"
    if a.table_only:
        print(table(pd.read_csv(out)))
        return
    df = run(a.symbols, a.periods)
    df.to_csv(out, index=False)
    print(df.to_string(index=False))
    print(f"[ok] {out}")
    print(table(df))


if __name__ == "__main__":
    main()
