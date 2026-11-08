"""C6 — Truy vết nguồn gốc từng cây nến: mỗi nến trong Postgres trỏ được về đâu?

    python experiments/c6_lineage.py                              # bảng theo nguồn + mẫu
    python experiments/c6_lineage.py --trace BTCUSDT 2026-11-09T12:34   # một nến qua mọi nguồn

Mỗi nến mang 5 cột lineage (bdi.lineage): source, file_checksum, ingested_at,
watermark_config, pipeline_version. Script phân loại `file_checksum` thành:
  - file   : sha256 của file zip Binance → tra trong raw/manifest.jsonl ra URL, kỳ, loại file
             (download.py đã đối chiếu sha256 với file .CHECKSUM của Binance khi tải)
  - kafka  : "kafka:<topic>" → nến streaming, cấu hình watermark nằm ở watermark_config
  - rest   : "rest:<endpoint>" → nến official lấy qua REST (official-poller)
Nến "truy được" khi: file có sha256 nằm trong manifest; kafka/rest có đủ topic/endpoint.

Đầu ra: results/c6/lineage_by_source.csv và results/c6/lineage_sample.csv (1 nến mẫu mỗi
nguồn, kèm URL file gốc nếu có). Cần Postgres; manifest lấy theo BDI__PATHS__RAW
(sau lượt 24 giờ: BDI__PATHS__RAW=$HOME/bdi-data/raw, như sau-24h.sh), có thể thêm
--manifest để gộp nhiều manifest.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import _common  # noqa: F401  (sys.path)
import pandas as pd
import psycopg

from bdi.config import path, pg_dsn


def load_manifests(extra: list[str]) -> pd.DataFrame:
    rows = []
    for p in [path("raw") / "manifest.jsonl", *map(Path, extra)]:
        if p.exists():
            rows += [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]
    cols = ["sha256", "kind", "symbol", "period", "url"]
    m = pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)
    return m[cols].drop_duplicates("sha256")


def origin(chk: pd.Series) -> pd.Series:
    s = chk.fillna("")
    return pd.Series(pd.NA, index=s.index, dtype="object").mask(s.str.startswith("kafka:"), "kafka") \
        .mask(s.str.startswith("rest:"), "rest").mask(s.str.fullmatch(r"[0-9a-f]{64}"), "file").fillna("thiếu")


def query(conn, sql: str, params=()) -> pd.DataFrame:
    cur = conn.execute(sql, params)
    return pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])


def by_source(conn, manifest: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    per_chk = query(conn, """
        SELECT source, file_checksum, count(*) AS n, count(DISTINCT symbol) AS n_cap,
               min(minute) AS tu, max(minute) AS den,
               string_agg(DISTINCT coalesce(watermark_config, '-'), ',') AS watermark_config,
               string_agg(DISTINCT pipeline_version, ',') AS pipeline_version,
               min(ingested_at) AS ingested_tu, max(ingested_at) AS ingested_den
        FROM candles GROUP BY 1, 2""")
    if per_chk.empty:
        raise SystemExit("Bảng candles trống.")
    per_chk["nguon_goc"] = origin(per_chk["file_checksum"])
    per_chk = per_chk.merge(manifest, left_on="file_checksum", right_on="sha256", how="left")
    per_chk["truy_duoc"] = per_chk["nguon_goc"].isin(["kafka", "rest"]) | per_chk["url"].notna()

    def agg(g: pd.DataFrame) -> pd.Series:
        n = g["n"].sum()
        return pd.Series({
            "n_nen": n,
            "nguon_goc": ",".join(sorted(g["nguon_goc"].unique())),
            "n_file_checksum": g["file_checksum"].nunique(),
            "ky_file": ",".join(sorted(g["period"].dropna().unique())) or "-",
            "tu": g["tu"].min(), "den": g["den"].max(),
            "watermark_config": ",".join(sorted({w for s in g["watermark_config"] for w in s.split(",")})),
            "pipeline_version": ",".join(sorted({v for s in g["pipeline_version"].dropna() for v in s.split(",")})),
            "ingested_tu": g["ingested_tu"].min(), "ingested_den": g["ingested_den"].max(),
            "pct_truy_duoc": round(100 * g.loc[g["truy_duoc"], "n"].sum() / n, 4),
        })

    return per_chk.groupby("source").apply(agg, include_groups=False).reset_index(), per_chk


def sample(conn, per_chk: pd.DataFrame) -> pd.DataFrame:
    s = query(conn, """
        SELECT DISTINCT ON (source) source, symbol, minute, file_checksum, watermark_config,
               pipeline_version, ingested_at, emitted_at
        FROM candles ORDER BY source, minute DESC""")
    return s.merge(per_chk[["source", "file_checksum", "nguon_goc", "kind", "period", "url"]],
                   on=["source", "file_checksum"], how="left")


def trace(conn, manifest: pd.DataFrame, symbol: str, minute: str) -> None:
    """Một nến qua mọi nguồn: giá trị, lineage, số phiên bản (C5), nhãn đối soát (C3)."""
    c = query(conn, """
        SELECT c.source, c.open, c.close, c.volume, c.trades, c.file_checksum, c.watermark_config,
               c.pipeline_version, c.ingested_at, c.emitted_at,
               (SELECT count(*) FROM candle_versions v
                 WHERE v.symbol = c.symbol AND v.minute = c.minute AND v.source = c.source) AS so_phien_ban
        FROM candles c WHERE c.symbol = %s AND c.minute = %s::timestamptz ORDER BY c.source""",
              (symbol, minute))
    if c.empty:
        raise SystemExit(f"Không có nến {symbol} {minute}")
    c["nguon_goc"] = origin(c["file_checksum"])
    c = c.merge(manifest[["sha256", "url"]], left_on="file_checksum", right_on="sha256", how="left").drop(columns="sha256")
    r = query(conn, """SELECT run_id, left_src, right_src, all_match, cause FROM reconcile_results
                       WHERE symbol = %s AND minute = %s::timestamptz ORDER BY run_id""", (symbol, minute))
    with pd.option_context("display.width", 200, "display.max_columns", None, "display.max_colwidth", 90):
        print(f"== {symbol} {minute}\n{c.to_string(index=False)}")
        print("\n== Đối soát (reconcile_results)\n" + (r.to_string(index=False) if not r.empty else "  (chưa có)"))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", nargs="*", default=[], help="thêm manifest.jsonl khác để tra sha256")
    ap.add_argument("--trace", nargs=2, metavar=("SYMBOL", "MINUTE"))
    a = ap.parse_args()
    manifest = load_manifests(a.manifest)
    with psycopg.connect(pg_dsn()) as conn:
        if a.trace:
            trace(conn, manifest, *a.trace)
            return
        src, per_chk = by_source(conn, manifest)
        smp = sample(conn, per_chk)
    out = _common.out_dir("c6")
    src.to_csv(out / "lineage_by_source.csv", index=False)
    smp.to_csv(out / "lineage_sample.csv", index=False)
    with pd.option_context("display.width", 200, "display.max_columns", None, "display.max_colwidth", 70):
        print(src.drop(columns=["ingested_tu", "ingested_den"]).to_string(index=False))
        print()
        print(smp[["source", "symbol", "minute", "nguon_goc", "file_checksum", "watermark_config",
                   "pipeline_version", "url"]].to_string(index=False))
    print(f"\n[ok] {out}/lineage_by_source.csv, lineage_sample.csv ({len(manifest)} file trong manifest)")


if __name__ == "__main__":
    main()
