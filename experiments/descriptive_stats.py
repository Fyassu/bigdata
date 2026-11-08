"""B4.2 — Thống kê mô tả bộ dữ liệu (phần đầu báo cáo).

Tiền điều kiện: đã tải aggTrades + klines (bdi.ingest.download). Không cần Kafka, Postgres
hay silver: script đọc thẳng CSV aggTrades nên chạy được trên bất kỳ máy nào có data/raw.

    python experiments/descriptive_stats.py --symbols BTCUSDT ETHUSDT BNBUSDT SOLUSDT XRPUSDT \
        --periods 2022-11 2026-06 2026-07 2026-08
    python experiments/descriptive_stats.py --plots-only     # vẽ lại từ bảng phút đã lưu

Luôn truyền --periods (cùng lý do với E1: manifest trên máy dữ liệu có thêm file theo ngày).

Mỗi file aggTrades được quét MỘT lần, gộp theo (symbol, phút); bảng phút (~878 400 dòng) lưu ở
data/lake/gold/desc_minutes.parquet (không commit) để vẽ lại không cần Spark. Cùng lượt quét,
số trade, quote volume và khối lượng mua chủ động được so tuyệt đối với klines official.

Quy ước: "mua chủ động" (taker buy) là aggTrade có is_buyer_maker = false. Tỉ lệ tính theo
số trade lẻ (Σ last_id − first_id + 1, không phải số dòng aggTrade) và theo quote volume (USDT).

Đầu ra results/desc/:
  pair_month.csv        : bảng cặp × tháng (số aggTrade, số trade, USDT, trade/phút, tỉ lệ mua
                          chủ động, % phút khớp klines)
  trades_per_minute.png : phân phối số trade mỗi phút theo cặp, 11/2022 so với 2026
  daily_volume.png      : khối lượng USDT mỗi ngày theo cặp
  taker_buy.png         : tỉ lệ mua chủ động theo tháng (theo số trade và theo USDT)
"""

from __future__ import annotations

import argparse

import _common
import numpy as np
import pandas as pd

from bdi.config import path

PAIRS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
MINUTES_FILE = "desc_minutes.parquet"


def minutes_path():
    return path("lake") / "gold" / MINUTES_FILE


def scan(symbols: list[str] | None, periods: list[str] | None) -> pd.DataFrame:
    # Import Spark ở đây để --plots-only chạy được trên máy không có Java/pyspark
    from pyspark.sql import functions as F

    from bdi.batch.build_batch import read_trades, select_files
    from bdi.schema import KLINES_CSV_SCHEMA, QUOTE_VOLUME, VOLUME
    from bdi.spark import get_spark
    from bdi.timeutil import to_micros_col

    spark = get_spark("descriptive_stats")
    klines = {(e["symbol"], e["period"]): e for e in select_files("klines", symbols, periods)}
    frames = []
    for e in select_files("aggTrades", symbols, periods):
        t = read_trades(spark, e)
        n_trades = F.col("last_trade_id") - F.col("first_trade_id") + 1
        # Tích decimal(18,8) × decimal(18,8) = decimal(37,16) chính xác (xem C2, quote_volume)
        quote = F.col("price").cast("decimal(18,8)") * F.col("qty").cast("decimal(18,8)")
        buy = ~F.col("is_buyer_maker")
        m = (t.groupBy(F.window("event_time", "1 minute").alias("w"))
             .agg(F.count(F.lit(1)).alias("n_agg"),
                  F.sum(n_trades).alias("trades"),
                  F.sum("qty").cast(VOLUME).alias("volume"),
                  F.sum(quote).cast(QUOTE_VOLUME).alias("quote"),
                  F.sum(F.when(buy, n_trades).otherwise(0)).alias("buy_trades"),
                  # otherwise(0): phút không có lệnh mua chủ động thì klines ghi 0, không phải NULL
                  F.sum(F.when(buy, F.col("qty")).otherwise(0)).cast(VOLUME).alias("buy_volume"),
                  F.sum(F.when(buy, quote).otherwise(0)).cast(QUOTE_VOLUME).alias("buy_quote"))
             .select(F.col("w.start").alias("minute"), "*").drop("w"))

        k = klines.get((e["symbol"], e["period"]))
        if k:
            kl = (spark.read.csv(k["csv"], schema=KLINES_CSV_SCHEMA)
                  .select((to_micros_col("open_time") / 10**6).cast("long").alias("sec"),
                          F.col("trades").cast("long").alias("k_trades"),
                          F.col("quote_volume").cast(QUOTE_VOLUME).alias("k_quote"),
                          F.col("taker_buy_base").cast(VOLUME).alias("k_buy_volume"),
                          F.col("taker_buy_quote").cast(QUOTE_VOLUME).alias("k_buy_quote")))
            # open_time là ms (2022) hoặc µs (từ 2025): to_micros_col của C1 xử lý cả hai
            m = (m.withColumn("sec", F.unix_seconds("minute"))
                 .join(kl, "sec", "full")
                 .withColumn("minute", F.coalesce("minute", F.timestamp_seconds("sec"))))
            eq = lambda a, b: F.coalesce(F.col(a) == F.col(b), F.lit(False))  # noqa: E731
            m = (m.withColumn("in_klines", F.col("k_trades").isNotNull())
                 .withColumn("ok_trades", eq("trades", "k_trades"))
                 .withColumn("ok_quote", eq("quote", "k_quote"))
                 .withColumn("ok_buy", eq("buy_volume", "k_buy_volume") & eq("buy_quote", "k_buy_quote")))
        else:
            print(f"[warn] {e['symbol']} {e['period']}: thiếu klines, bỏ phần so khớp")
            for c in ("in_klines", "ok_trades", "ok_quote", "ok_buy"):
                m = m.withColumn(c, F.lit(None).cast("boolean"))

        pdf = (m.select(
            F.unix_seconds("minute").alias("ts"),
            F.coalesce("n_agg", F.lit(0)).alias("n_agg"),
            F.coalesce("trades", F.lit(0)).alias("trades"),
            F.coalesce("buy_trades", F.lit(0)).alias("buy_trades"),
            *[F.coalesce(F.col(c).cast("double"), F.lit(0.0)).alias(c)
              for c in ("volume", "quote", "buy_volume", "buy_quote")],
            "in_klines", "ok_trades", "ok_quote", "ok_buy")
            .toPandas())
        pdf.insert(0, "period", e["period"])
        pdf.insert(0, "symbol", e["symbol"])
        frames.append(pdf)
        print(f"[desc] {e['symbol']} {e['period']}: {len(pdf):,} phút, {int(pdf['n_agg'].sum()):,} aggTrade")

    df = pd.concat(frames, ignore_index=True)
    df["minute"] = pd.to_datetime(df["ts"], unit="s", utc=True)
    p = minutes_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(p, index=False)
    print(f"[ok] {p}")
    return df


def pair_month(df: pd.DataFrame) -> pd.DataFrame:
    def agg(g: pd.DataFrame) -> pd.Series:
        tpm = g["trades"]
        return pd.Series({
            "minutes": len(g),
            "empty_minutes": int((g["n_agg"] == 0).sum()),
            "n_aggtrades": int(g["n_agg"].sum()),
            "n_trades": int(g["trades"].sum()),
            "trades_per_agg": g["trades"].sum() / g["n_agg"].sum(),
            "volume_base": g["volume"].sum(),
            "quote_usdt": g["quote"].sum(),
            "quote_usdt_per_day": g["quote"].sum() / (len(g) / 1440),
            "trades_per_min_p50": tpm.median(),
            "trades_per_min_p95": tpm.quantile(0.95),
            "trades_per_min_max": int(tpm.max()),
            "taker_buy_pct_trades": 100 * g["buy_trades"].sum() / g["trades"].sum(),
            "taker_buy_pct_quote": 100 * g["buy_quote"].sum() / g["quote"].sum(),
            "pct_minutes_in_klines": 100 * g["in_klines"].mean(),
            "pct_match_trades": 100 * g["ok_trades"].mean(),
            "pct_match_quote": 100 * g["ok_quote"].mean(),
            "pct_match_taker_buy": 100 * g["ok_buy"].mean(),
        })

    out = df.groupby(["symbol", "period"]).apply(agg, include_groups=False).reset_index()
    ints = ["minutes", "empty_minutes", "n_aggtrades", "n_trades", "trades_per_min_max"]
    out[ints] = out[ints].astype("int64")
    out["symbol"] = pd.Categorical(out["symbol"], PAIRS, ordered=True)
    return out.sort_values(["symbol", "period"]).reset_index(drop=True)


def plot_trades_per_minute(df: pd.DataFrame) -> None:
    fig, ax = _common.plt.subplots(figsize=(9, 4.2))
    groups = [("2022-11", "11/2022 (FTX)", _common.SERIES[1]), (None, "6–8/2026", _common.SERIES[0])]
    width, pos = 0.36, np.arange(len(PAIRS))
    for i, (period, label, color) in enumerate(groups):
        data = []
        for s in PAIRS:
            g = df[df["symbol"] == s]
            g = g[g["period"] == period] if period else g[g["period"] != "2022-11"]
            data.append(g["trades"].clip(lower=1).to_numpy())
        bp = ax.boxplot(data, positions=pos + (i - 0.5) * width, widths=width * 0.85,
                        whis=(1, 99), showfliers=False, patch_artist=True,
                        medianprops={"color": _common.TEXT, "linewidth": 1.5})
        for b in bp["boxes"]:
            b.set(facecolor=color, alpha=0.75, edgecolor=color)
        for k in ("whiskers", "caps"):
            for line in bp[k]:
                line.set(color=color, linewidth=1.2)
        ax.plot([], [], color=color, linewidth=8, alpha=0.75, label=label)
    ax.set_yscale("log")
    ax.set_xticks(pos, [s.removesuffix("USDT") for s in PAIRS])
    ax.set_ylabel("Số trade mỗi phút (thang log)")
    ax.set_title("Phân phối số trade mỗi phút (hộp: p25–p75, râu: p1–p99)", loc="left")
    ax.legend(frameon=False, loc="upper right")
    _common.save(fig, "desc", "trades_per_minute")


def plot_daily_volume(df: pd.DataFrame) -> None:
    import matplotlib.dates as mdates

    d = df.assign(day=df["minute"].dt.floor("D")).groupby(["symbol", "day"])["quote"].sum().reset_index()
    fig, axes = _common.plt.subplots(1, 2, figsize=(11, 4.2), sharey=True,
                                     gridspec_kw={"width_ratios": [1, 3]})
    spans = [("11/2022", d["day"] < "2025-01-01", [1, 8, 15, 22]),
             ("6–8/2026", d["day"] >= "2025-01-01", [1, 15])]
    for ax, (title, mask, days) in zip(axes, spans):
        for i, s in enumerate(PAIRS):
            g = d[mask & (d["symbol"] == s)]
            ax.plot(g["day"], g["quote"] / 1e9, color=_common.SERIES[i], label=s.removesuffix("USDT"))
        ax.set_title(title, loc="left")
        ax.set_xlim(d.loc[mask, "day"].min(), d.loc[mask, "day"].max())
        ax.xaxis.set_major_locator(mdates.DayLocator(bymonthday=days))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m"))
    axes[0].set_yscale("log")
    axes[0].set_ylim(top=60)          # chừa chỗ cho chú thích phía trên các đường
    axes[0].set_ylabel("Khối lượng mỗi ngày (tỷ USDT, thang log)")
    axes[1].legend(frameon=False, ncol=5, loc="upper right")
    _common.save(fig, "desc", "daily_volume")


def plot_taker_buy(pm: pd.DataFrame) -> None:
    fig, axes = _common.plt.subplots(1, 2, figsize=(11, 4.2), sharey=True)
    periods = sorted(pm["period"].unique())
    # 11/2022 đứng riêng, cách 6–8/2026 một khoảng: hai mốc cách nhau 3,5 năm, không nối đường
    x = {p: (i if p < "2025" else i + 0.8) for i, p in enumerate(periods)}
    for ax, col, title in zip(axes, ["taker_buy_pct_trades", "taker_buy_pct_quote"],
                              ["Theo số trade", "Theo khối lượng USDT"]):
        for i, s in enumerate(PAIRS):
            g = pm[pm["symbol"] == s].set_index("period")[col]
            old, new = [p for p in periods if p < "2025"], [p for p in periods if p >= "2025"]
            color = _common.SERIES[i]
            ax.plot([x[p] for p in old], g.reindex(old), marker="o", linestyle="none", color=color)
            ax.plot([x[p] for p in new], g.reindex(new), marker="o", color=color, label=s.removesuffix("USDT"))
        ax.axhline(50, color=_common.TEXT_2, linewidth=1, linestyle="--")
        ax.set_xticks([x[p] for p in periods], [f"{p[5:]}/{p[:4]}" for p in periods])
        ax.set_title(title, loc="left")
    lo = min(pm["taker_buy_pct_trades"].min(), pm["taker_buy_pct_quote"].min())
    hi = max(pm["taker_buy_pct_trades"].max(), pm["taker_buy_pct_quote"].max())
    axes[0].set_ylim(lo - 0.5, hi + 1.8)   # chừa chỗ cho chú thích phía trên các đường
    axes[0].set_ylabel("Tỉ lệ mua chủ động (%)")
    axes[1].legend(frameon=False, ncol=5, loc="upper left")
    _common.save(fig, "desc", "taker_buy")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--symbols", nargs="+")
    ap.add_argument("--periods", nargs="+")
    ap.add_argument("--plots-only", action="store_true", help="đọc lại bảng phút đã lưu, không chạy Spark")
    a = ap.parse_args()

    if a.plots_only:
        df = pd.read_parquet(minutes_path())
    else:
        if not a.periods:
            raise SystemExit("Cần --periods (xem docstring).")
        df = scan(a.symbols, a.periods)

    pm = pair_month(df)
    out = _common.out_dir("desc") / "pair_month.csv"
    pm.to_csv(out, index=False, float_format="%.4f")
    print(f"[ok] {out}")
    with pd.option_context("display.width", 200, "display.max_columns", 30):
        print(pm[["symbol", "period", "n_aggtrades", "n_trades", "quote_usdt_per_day",
                  "trades_per_min_p50", "taker_buy_pct_trades", "pct_match_trades",
                  "pct_match_quote", "pct_match_taker_buy"]].to_string(index=False))
    plot_trades_per_minute(df)
    plot_daily_volume(df)
    plot_taker_buy(pm)


if __name__ == "__main__":
    main()
