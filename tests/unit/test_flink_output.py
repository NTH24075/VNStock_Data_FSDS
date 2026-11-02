import json
import os

import pytest

from jobs.silver.flink_output import load_flink_silver


@pytest.fixture(scope="module")
def spark():
    try:
        from jobs.spark_session import get_spark

        session = get_spark("test_flink_output")
    except Exception:
        pytest.skip("Java/Spark unavailable")
    try:
        yield session
    finally:
        session.stop()


def _write_part(directory, name, rows):
    os.makedirs(directory, exist_ok=True)
    with open(os.path.join(directory, name), "w") as f:
        f.writelines(json.dumps(row) + "\n" for row in rows)


TRADE = {
    "event_id": "e1",
    "event_type": "trade",
    "event_timestamp": "2025-10-09T09:00:01+00:00",
    "created_ts": "2025-10-09T09:00:05+00:00",
    "session_type": "continuous",
    "ticker": "VCB",
    "exchange": "HOSE",
    "price": 90.5,
    "quantity": 100,
    "side": "buy",
    "trade_id": "t1",
    "kafka_offset": 7,
}
QUOTE = {**TRADE, "event_id": "e2", "event_type": "quote", "bid_price": 90.4, "ask_price": 90.6}


class TestLoadFlinkSilver:
    def test_splits_trades_and_quotes_and_is_idempotent(self, spark, tmp_path):
        flink_dir, silver_dir = str(tmp_path / "flink"), str(tmp_path / "silver")
        _write_part(os.path.join(flink_dir, "stg_events"), "part-0.jsonl", [TRADE, QUOTE])
        # In-progress Flink part files are hidden and must not be loaded.
        _write_part(os.path.join(flink_dir, "stg_events"), ".part-1.inprogress", [TRADE])

        load_flink_silver(spark, flink_dir, silver_dir)
        load_flink_silver(spark, flink_dir, silver_dir)

        trades = spark.read.format("delta").load(os.path.join(silver_dir, "stg_trades"))
        quotes = spark.read.format("delta").load(os.path.join(silver_dir, "stg_quotes"))
        assert [r.event_id for r in trades.collect()] == ["e1"]
        assert [r.event_id for r in quotes.collect()] == ["e2"]
        row = trades.first()
        assert row.event_ts is not None and row.source_offset == 7
        assert not os.path.exists(os.path.join(silver_dir, "stg_events_quarantine"))

    def test_missing_flink_output_fails(self, spark, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_flink_silver(spark, str(tmp_path / "none"), str(tmp_path / "silver"))
