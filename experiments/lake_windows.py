"""Dựng silver trades + nến batch dạng parquet trên Windows KHÔNG có Hadoop (winutils).

Trên Windows thiếu HADOOP_HOME/winutils, Spark đọc được từng file nhưng không ghi được parquet và
không liệt kê được thư mục. build_batch vì thế hỏng ở bước ghi silver. Script này thay phần ghi:

  silver  : đọc CSV aggTrades bằng pyarrow (theo khối, không cần Spark), đưa về CANONICAL_TRADE_COLS
            của C1 (Decimal(24,8), ts_us drift-aware, event_time UTC) rồi ghi
            data/lake/silver/trades/symbol=<S>/period=<P>/part-00000.parquet
  candles : build_candles (Spark, CÙNG hàm với build_batch) trên CSV, gom về pandas rồi ghi
            data/lake/gold/candles_batch/symbol=<S>/period=<P>/part-00000.parquet
            (không ghi Postgres, không cột lineage — đủ cho E7 --lake)

    python experiments/lake_windows.py silver  --symbols BTCUSDT ... --periods 2022-11 2026-06 2026-07 2026-08
    python experiments/lake_windows.py candles --symbols BTCUSDT ... --periods 2022-05 2026-06 2026-07 2026-08

Đọc lại bằng Spark trên Windows: truyền danh sách file + basePath (xem lake_files()).
Trên Linux/macOS dùng build_batch như bình thường; script này không cần.
"""

from __future__ import annotations

import argparse
import shutil
import time
from pathlib import Path

import _common  # noqa: F401  (đặt sys.path)
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

from bdi.config import path
from bdi.schema import AGGTRADES_CSV_COLS

DEC = pa.decimal128(24, 8)


def lake_files(root: Path, periods: list[str] | None = None) -> list[str]:
    """Danh sách file parquet dưới root (partition symbol=/period=), để Spark đọc không cần liệt kê."""
    files = sorted(str(p) for p in root.glob("symbol=*/period=*/*.parquet"))
    if periods:
        files = [f for f in files if any(f"period={p}" in f for p in periods)]
    return files


def _to_micros(ts: pa.Array) -> pa.Array:
    """Drift-aware như bdi.timeutil.to_micros: suy đơn vị theo độ lớn (s / ms / µs / ns)."""
    a = pc.abs(ts)
    return pc.case_when(
        pc.make_struct(pc.greater_equal(a, 10**17), pc.greater_equal(a, 10**14),
                       pc.greater_equal(a, 10**12), pc.greater_equal(a, 10**9)),
        pc.divide(ts, 1000), ts, pc.multiply(ts, 1000), pc.multiply(ts, 1_000_000))


def silver(symbols, periods) -> None:
    from bdi.batch.build_batch import select_files

    root = path("lake") / "silver" / "trades"
    types = {c: pa.string() for c in AGGTRADES_CSV_COLS}
    types.update({"agg_id": pa.int64(), "first_trade_id": pa.int64(), "last_trade_id": pa.int64(),
                  "transact_time": pa.int64()})
    for e in select_files("aggTrades", symbols, periods):
        out = root / f"symbol={e['symbol']}" / f"period={e['period']}"
        shutil.rmtree(out, ignore_errors=True)
        out.mkdir(parents=True)
        t0, n = time.perf_counter(), 0
        reader = pcsv.open_csv(e["csv"], read_options=pcsv.ReadOptions(column_names=AGGTRADES_CSV_COLS,
                                                                       block_size=64 << 20),
                               convert_options=pcsv.ConvertOptions(column_types=types))
        writer = None
        for b in reader:
            ts = _to_micros(b.column("transact_time"))
            t = pa.table({
                "agg_id": b.column("agg_id"),
                "price": pc.cast(b.column("price"), DEC),
                "qty": pc.cast(b.column("qty"), DEC),
                "first_trade_id": b.column("first_trade_id"),
                "last_trade_id": b.column("last_trade_id"),
                "ts_us": ts,
                "event_time": ts.cast(pa.timestamp("us", tz="UTC")),
                "is_buyer_maker": pc.equal(pc.utf8_lower(b.column("is_buyer_maker")), "true"),
                "origin": pa.array(["batch_csv"] * len(ts), pa.string()),
            })
            if writer is None:
                writer = pq.ParquetWriter(out / "part-00000.parquet", t.schema, compression="snappy")
            writer.write_table(t, row_group_size=1 << 20)
            n += len(ts)
        if writer:
            writer.close()
        print(f"[silver] {e['symbol']} {e['period']}: {n:,} dòng, {time.perf_counter() - t0:.0f} s", flush=True)


def candles(symbols, periods) -> None:
    from bdi.batch.build_batch import read_trades, select_files
    from bdi.candles import build_candles
    from bdi.spark import get_spark

    spark = get_spark("lake_windows_candles")
    root = path("lake") / "gold" / "candles_batch"
    for e in select_files("aggTrades", symbols, periods):
        # toArrow giữ nguyên kiểu Decimal của Spark; from_pandas sẽ suy độ chính xác riêng từng file
        tbl = build_candles(read_trades(spark, e)).drop("symbol").toArrow()
        out = root / f"symbol={e['symbol']}" / f"period={e['period']}"
        shutil.rmtree(out, ignore_errors=True)
        out.mkdir(parents=True)
        pq.write_table(tbl, out / "part-00000.parquet")
        print(f"[candles] {e['symbol']} {e['period']}: {tbl.num_rows:,} nến", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("what", choices=["silver", "candles"])
    ap.add_argument("--symbols", nargs="+")
    ap.add_argument("--periods", nargs="+", required=True)
    a = ap.parse_args()
    (silver if a.what == "silver" else candles)(a.symbols, a.periods)


if __name__ == "__main__":
    main()
