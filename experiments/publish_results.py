"""Đẩy kết quả thí nghiệm (CSV trong results/) vào Postgres để Grafana hiển thị.

    python experiments/publish_results.py

Mỗi file thành một bảng exp_* (ghi đè). Chỉ cần thư mục results/ trong repo, không
cần dữ liệu thô, nên chạy được trên máy vừa clone. Dashboard "BDI · Kết quả thí nghiệm"
(grafana/dashboards/bdi_results.json) đọc các bảng này. Chạy lại sau mỗi lần cập
nhật kết quả.
"""

from __future__ import annotations

import io

import _common  # noqa: F401  (sys.path)
import pandas as pd
import psycopg

from bdi.anomaly.mzscore import detect
from bdi.config import path, pg_dsn
from bdi.reconcile.compare import load_candles

TABLES = {
    "exp_e1": "e1/ablation.csv",
    "exp_e2": "e2/match_by_field.csv",
    "exp_e3": "e3/tradeoff.csv",
    "exp_e4": "e4/incidents.csv",
    "exp_e5": "e5/linkage.csv",
    "exp_e6": "e6/approx_idle_minh.csv",       # đo lại khi máy rảnh (01/11)
    "exp_e6_busy": "e6/approx_local.csv",      # lần đo cũ khi máy tải cao (14/10)
    "exp_e7_sweep": "e7/sweep.csv",
    "exp_c5": "c5/source_quality.csv",
    "exp_e7_sb": "e7/stream_vs_batch.csv",
    "exp_e7_luna": "e7/sweep_luna.csv",
    "exp_c2": "c2/counterfactual.csv",
    "exp_desc": "desc/pair_month.csv",
}


def _pg_type(s: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(s):
        return "boolean"
    if pd.api.types.is_integer_dtype(s):
        return "bigint"
    if pd.api.types.is_float_dtype(s):
        return "double precision"
    return "text"


def write_table(conn, name: str, df: pd.DataFrame) -> None:
    cols = ", ".join(f'"{c}" {_pg_type(df[c])}' for c in df.columns)
    conn.execute(f"DROP TABLE IF EXISTS {name}")
    conn.execute(f"CREATE TABLE {name} ({cols})")
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="\\N")
    with conn.cursor().copy(f"COPY {name} FROM STDIN WITH (FORMAT csv, NULL '\\N')") as cp:
        cp.write(buf.getvalue())
    print(f"[ok] {name}: {len(df)} dòng")


def e7_daily() -> pd.DataFrame:
    """Báo động log-return theo ngày, 11/2022, cấu hình chọn (config/settings.yaml)."""
    c = load_candles("batch", start="2022-11-01", end="2022-12-01")
    a = detect(c)
    a = a[a["metric"] == "log_return"]
    d = a.groupby(a["minute"].dt.strftime("%m-%d")).size().rename("alerts").reset_index()
    d = d.rename(columns={"minute": "day"})
    d["ftx"] = d["day"].isin(["11-08", "11-09"])
    return d


def main() -> None:
    res = path("results")
    with psycopg.connect(pg_dsn()) as conn:
        for name, rel in TABLES.items():
            p = res / rel
            if p.exists():
                write_table(conn, name, pd.read_csv(p))
            else:
                print(f"[skip] {name}: chưa có {rel}")
        # Có file trong repo thì dùng file (máy mới chưa có nến batch); không thì tính từ Postgres
        daily = res / "e7" / "alerts_per_day.csv"
        if daily.exists():
            d = pd.read_csv(daily)
            d["day"] = d["day"].str[5:]  # 2022-11-08 -> 11-08
            write_table(conn, "exp_e7_daily", d)
        else:
            write_table(conn, "exp_e7_daily", e7_daily())
        luna = res / "e7" / "alerts_per_day_luna.csv"
        if luna.exists():
            d = pd.read_csv(luna)
            d["day"] = d["day"].str[5:]  # 2022-05-11 -> 05-11
            write_table(conn, "exp_e7_luna_daily", d)
        conn.commit()


if __name__ == "__main__":
    main()
