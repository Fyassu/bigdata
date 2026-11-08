"""C3 — Bảng phân bố nguyên nhân lệch, gộp từ mọi lượt đối soát (E2, E3, E4, E7, live, C5).

    python experiments/c3_causes.py

Chỉ đọc CSV trong results/, không cần Spark, Postgres hay dữ liệu thô:
  - results/reconcile/<run_id>_summary.csv + _causes.csv   (bdi.reconcile.compare)
  - results/reconcile/<run_id>_late_causes.csv              (experiments/e3_late_labels.py, lượt E3 jitter)
  - results/e3/tradeoff.csv + results/e3/configs/*.json     (số nến lệch của từng cấu hình E3)
  - results/e4/incidents.csv                                (4 kịch bản chaos E4)

Đầu ra: results/c3/causes_by_experiment.csv (mỗi lượt một dòng, mỗi nhãn một cột) và
bảng Markdown in ra màn hình để dán vào báo cáo. Cột `chua_gan_nhan` > 0 nghĩa là lượt
đó có nến lệch nhưng chưa chạy bước gắn nhãn (VD lượt E3 jitter trước khi chạy
e3_late_labels.py). Chạy lại sau mỗi lần có kết quả đối soát mới.
"""

from __future__ import annotations

import fnmatch
import json

import _common  # noqa: F401  (sys.path)
import pandas as pd

from bdi.config import path
from bdi.reconcile.taxonomy import CAUSES

# run_id (cho phép *) → (thí nghiệm, mô tả). Thứ tự ở đây là thứ tự dòng trong bảng.
REGISTRY = [
    ("e2", "E2", "Batch vs official, 5 cặp × 4 tháng"),
    ("e4_sleep", "E4", "Máy ngủ ~5,7 giờ, backfill khi nối lại"),
    ("e4_proactive", "E4", "Ngắt chủ động sau 24 giờ (proactive_24h), ±30 phút"),
    ("e7_ftx_stream", "E7", "Replay FTX 100× trên stream"),
    ("live_d0", "Live", "Luồng live ngày đầu"),
    ("c5_batch_*", "C5", "Batch ngày D của lượt 24 giờ"),
]
# chạy thử hoặc tập con của một lượt đã có
SKIP = ["smoke", "e2_btc_2608", "*_late"]


def _match(run_id: str, pats) -> str | None:
    return next((p for p in pats if fnmatch.fnmatchcase(run_id, p)), None)


def _causes(p) -> dict[str, int]:
    if not p.exists():
        return {}
    c = pd.read_csv(p)
    return {} if c.empty else dict(zip(c["cause"], c["n"].astype(int)))


def _row(exp, run_id, desc, n, n_bad, causes) -> dict:
    r = {"thi_nghiem": exp, "run_id": run_id, "mo_ta": desc, "n_nen": int(n), "n_lech": int(n_bad)}
    r.update({c: causes.get(c, 0) for c in CAUSES})
    r["chua_gan_nhan"] = max(int(n_bad) - sum(causes.values()), 0)
    return r


def reconcile_rows(rec) -> list[dict]:
    out = []
    for p in sorted(rec.glob("*_summary.csv")):
        rid = p.name.removesuffix("_summary.csv")
        if _match(rid, SKIP):
            continue
        s = pd.read_csv(p)
        n = s["n_minutes"].sum()
        n_bad = round((s["n_minutes"] * (100 - s["all_match"]) / 100).sum())
        pat = _match(rid, [r[0] for r in REGISTRY])
        exp, desc = next(((e, d) for k, e, d in REGISTRY if k == pat), ("Khác", ""))
        if pat and "*" in pat:
            desc = f"{desc} ({rid.split('_')[-1]})"
        out.append((REGISTRY.index(next(r for r in REGISTRY if r[0] == pat)) if pat else 99,
                    _row(exp, rid, desc, n, n_bad, _causes(rec / f"{rid}_causes.csv"))))
    return [r for _, r in sorted(out, key=lambda t: t[0])]


def e3_rows(rec) -> list[dict]:
    res = path("results")
    trade, cfg_dir = res / "e3" / "tradeoff.csv", res / "e3" / "configs"
    if not trade.exists():
        return []
    t = pd.read_csv(trade)
    key = ["watermark_s", "trigger_s", "jitter_pct", "jitter_max_s"]
    cfgs = pd.DataFrame([json.loads(p.read_text()) for p in sorted(cfg_dir.glob("*.json"))])
    t = t.merge(cfgs[key + ["run_id"]], on=key, how="left")
    # không jitter trước, trigger 2 s trước trigger mặc định, trong mỗi nhóm tăng dần watermark
    t = t.assign(_j=t["jitter_pct"] > 0, _t=t["trigger_s"] == 0).sort_values(["_j", "_t", "watermark_s"])
    out = []
    for r in t.itertuples(index=False):
        n_bad = r.n_official - round(r.n_official * r.pct_match / 100)
        trig = "trigger mặc định" if r.trigger_s == 0 else f"trigger {r.trigger_s} s"
        jit = f", jitter {r.jitter_pct}%/{r.jitter_max_s} s" if r.jitter_pct else ""
        out.append(_row("E3", r.run_id, f"Watermark {r.watermark_s} s, {trig}{jit}", r.n_official, n_bad,
                        _causes(rec / f"{r.run_id}_late_causes.csv")))
    return out


def e4_chaos_rows() -> list[dict]:
    p = path("results") / "e4" / "incidents.csv"
    if not p.exists():
        return []
    out = []
    for r in pd.read_csv(p).itertuples(index=False):
        n_bad = r.missing_minutes + r.extra_trades_minutes + r.wrong_minutes
        out.append(_row("E4", f"chaos:{r.scenario}", f"Chaos {r.scenario}, sập {r.down_s:.0f} s",
                        r.official_minutes, n_bad, {}))
    return out


def main() -> None:
    rec = path("results") / "reconcile"
    rows = reconcile_rows(rec)
    # chèn E3 sau E2 và chaos E4 trước các lượt E4 khác, để bảng đọc theo thứ tự thí nghiệm
    e2 = [r for r in rows if r["thi_nghiem"] == "E2"]
    rest = [r for r in rows if r["thi_nghiem"] != "E2"]
    df = pd.DataFrame(e2 + e3_rows(rec) + e4_chaos_rows() + rest)
    total = df[["n_nen", "n_lech", *CAUSES, "chua_gan_nhan"]].sum()
    df.loc[len(df)] = {"thi_nghiem": "Tổng", "run_id": "", "mo_ta": "", **total.to_dict()}

    out = _common.out_dir("c3") / "causes_by_experiment.csv"
    df.to_csv(out, index=False)
    print(f"[ok] {out} ({len(df) - 1} lượt)")

    # bảng Markdown: chỉ giữ cột nhãn có ít nhất một nến
    used = [c for c in CAUSES if total[c] > 0] + (["chua_gan_nhan"] if total["chua_gan_nhan"] > 0 else [])
    cols = ["thi_nghiem", "mo_ta", "n_nen", "n_lech", *used]
    head = ["Thí nghiệm", "Lượt", "Số nến", "Lệch", *[c if c != "chua_gan_nhan" else "chưa gắn nhãn" for c in used]]
    print("| " + " | ".join(head) + " |")
    print("|" + "---|" * len(head))
    for r in df[cols].itertuples(index=False):
        cells = [f"{v:,}".replace(",", " ") if isinstance(v, (int, float)) and not isinstance(v, bool) else str(v)
                 for v in r]
        print("| " + " | ".join(cells) + " |")


if __name__ == "__main__":
    main()
