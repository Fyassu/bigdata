"""C2 — build_candles: Decimal, min_by/max_by, số trades; cùng kết quả batch vs streaming."""

from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from bdi.candles import build_candles
from bdi.schema import AGGTRADES_CSV_SCHEMA, canonical_from_aggtrades_csv

M0 = 1_786_795_200_000_000  # 2026-08-15T12:00:00Z (us)

# agg_id, price, qty, first, last, ts(us), maker, best — CỐ Ý xáo trộn thứ tự dòng
ROWS = [
    ("103", "100.30000000", "0.10000000", "13", "13", str(M0 + 40_000_000), "False", "True"),
    ("100", "100.00000000", "0.10000000", "10", "10", str(M0 + 1_000_000), "True", "True"),
    ("102", "99.50000000", "0.20000000", "12", "12", str(M0 + 30_000_000), "True", "True"),
    ("101", "101.10000000", "0.30000000", "11", "11", str(M0 + 20_000_000), "False", "True"),
    # cùng timestamp với 103 nhưng agg_id lớn hơn => là close
    ("104", "100.70000000", "0.10000000", "14", "16", str(M0 + 40_000_000), "False", "True"),
    # phút kế tiếp
    ("105", "100.80000000", "1.00000000", "17", "17", str(M0 + 61_000_000), "False", "True"),
]


@pytest.fixture
def trades(spark):
    raw = spark.createDataFrame(ROWS, AGGTRADES_CSV_SCHEMA).repartition(3)
    return canonical_from_aggtrades_csv(raw, "BTCUSDT")


def test_ohlcv_exact(trades):
    c = {r["minute"].minute: r for r in build_candles(trades).collect()}
    m = c[0]
    assert m["open"] == Decimal("100.00000000")
    assert m["high"] == Decimal("101.10000000")
    assert m["low"] == Decimal("99.50000000")
    assert m["close"] == Decimal("100.70000000")  # tie ts -> agg_id lớn nhất
    assert m["volume"] == Decimal("0.80000000")   # Decimal: không có 0.7999999999
    assert m["trades"] == 1 + 1 + 1 + 1 + 3       # sum(last - first + 1)
    assert m["n_agg"] == 5
    assert c[1]["open"] == c[1]["close"] == Decimal("100.80000000")


def test_volume_is_decimal_not_double(trades):
    dt = dict(build_candles(trades).dtypes)
    assert dt["volume"].startswith("decimal") and dt["open"].startswith("decimal")


def test_batch_equals_streaming(spark, trades, tmp_path):
    """Cùng hàm, chạy dạng streaming (file source) phải cho đúng kết quả như batch."""
    src = tmp_path / "in"
    trades.write.parquet(str(src))
    batch = {(r["symbol"], r["minute"]): r.asDict() for r in build_candles(trades).collect()}

    stream = (spark.readStream.schema(trades.schema).parquet(str(src))
              .withWatermark("event_time", "0 seconds"))
    q = (build_candles(stream).writeStream.outputMode("update").format("memory")
         .queryName("candles_test").start())
    q.processAllAvailable()
    q.stop()
    got = {(r["symbol"], r["minute"]): r.asDict() for r in spark.table("candles_test").collect()}
    assert got == batch


# ---------------------------------------------------------------------------
# C2 với NHIỀU micro-batch: trạng thái cộng dồn qua các batch phải cho đúng kết quả batch.
# Mỗi "đợt" là một file parquet thả vào thư mục nguồn rồi chờ xử lý xong, nên mỗi đợt là
# ít nhất một micro-batch. Các bản tin của phút 0 bị chia ra 3 đợt, đến không theo thứ tự.

WAVES = [
    [ROWS[0], ROWS[1]],            # 103 (40 s), 100 (1 s)
    [ROWS[2], ROWS[3]],            # 102 (30 s), 101 (20 s): đến sau nhưng còn trên watermark
    [ROWS[4], ROWS[5]],            # 104 (close của phút 0), 105 (phút 1)
]
DUP_102 = ROWS[2]                                    # gửi lại 102: producer nối lại WS
SENTINEL = ("200", "100.00000000", "0.01000000", "30", "30", str(M0 + 600_000_000), "False", "True")
LATE = ("150", "1.00000000", "5.00000000", "20", "20", str(M0 + 50_000_000), "True", "True")


def _canon(spark, rows):
    return canonical_from_aggtrades_csv(spark.createDataFrame(rows, AGGTRADES_CSV_SCHEMA), "BTCUSDT")


def _batch(spark, rows):
    return {(r["symbol"], r["minute"]): r.asDict() for r in build_candles(_canon(spark, rows)).collect()}


def _stream(spark, tmp_path, waves, mode, watermark="30 seconds", dedup=False):
    """Chạy build_candles trên file source, mỗi đợt một file.

    Trả (nến cuối, số lần phát mỗi nến, số dòng Spark bỏ vì trễ quá watermark)."""
    src, stage = tmp_path / "in", tmp_path / "stage"
    src.mkdir()
    schema = _canon(spark, ROWS[:1]).schema
    trades = spark.readStream.schema(schema).parquet(str(src)).withWatermark("event_time", watermark)
    if dedup:  # giống stream_candles ở append mode
        trades = trades.dropDuplicatesWithinWatermark(["symbol", "agg_id"])
    final, emits = {}, {}

    def collect(df, _bid):
        for r in df.collect():
            k = (r["symbol"], r["minute"])
            final[k] = r.asDict()
            emits[k] = emits.get(k, 0) + 1

    q = build_candles(trades).writeStream.outputMode(mode).foreachBatch(collect).start()
    try:
        for i, rows in enumerate(waves):
            _canon(spark, rows).coalesce(1).write.parquet(str(stage / str(i)))
            part = next((stage / str(i)).glob("part-*.parquet"))
            part.rename(src / f"{i:02d}.parquet")
            q.processAllAvailable()
        q.processAllAvailable()  # batch không dữ liệu: đẩy watermark, append phát các cửa sổ đã đóng
        dropped = sum(o.get("numRowsDroppedByWatermark", 0)
                      for p in q.recentProgress for o in p.get("stateOperators", []))
    finally:
        q.stop()
    return final, emits, dropped


def test_batch_equals_streaming_many_microbatches_update(spark, tmp_path):
    """update mode (như stream:update của C5): phút 0 được phát nhiều lần, bản cuối = batch."""
    final, emits, dropped = _stream(spark, tmp_path, WAVES, "update")
    assert dropped == 0
    assert final == _batch(spark, ROWS)
    m0 = ("BTCUSDT", next(k[1] for k in final if k[1].minute == 0))
    assert emits[m0] == 3  # cập nhật ở cả 3 đợt => trạng thái thật sự được cộng dồn


def test_batch_equals_streaming_many_microbatches_append_dedup(spark, tmp_path):
    """append mode + loại trùng (như luồng live): mỗi nến phát đúng 1 lần, bản tin gửi lại không tính 2 lần."""
    waves = WAVES[:2] + [WAVES[2] + [DUP_102], [SENTINEL]]
    final, emits, dropped = _stream(spark, tmp_path, waves, "append", dedup=True)
    assert dropped == 0  # 102 gửi lại vẫn trên watermark: bị bỏ do loại trùng, không phải do trễ
    expected = _batch(spark, ROWS)
    assert {k: v for k, v in final.items() if k in expected} == expected
    assert all(emits[k] == 1 for k in expected)


def test_late_beyond_watermark_is_the_only_divergence(spark, tmp_path):
    """Bản tin đến sau khi watermark đã qua cuối phút bị bỏ: stream lệch batch đúng ở phút đó
    (trường hợp nhãn LATE_BEYOND_WATERMARK của C3); phút khác vẫn khớp."""
    waves = WAVES + [[SENTINEL], [LATE]]
    final, _, dropped = _stream(spark, tmp_path, waves, "append")
    assert dropped == 1
    on_time = _batch(spark, ROWS)
    with_late = _batch(spark, ROWS + [LATE])
    m0 = next(k for k in on_time if k[1].minute == 0)
    assert final[m0] == on_time[m0] != with_late[m0]
    assert {k: final[k] for k in on_time} == on_time
