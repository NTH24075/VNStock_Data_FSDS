"""Tests for generator behavior promised by the coursework design."""

from datetime import date, time

import pandas as pd
import pyarrow.parquet as parquet

from generator.generators import Generator
from generator.main import load_config
from generator.problems import apply_volume_skew, tag_schema_version
from generator.writers import write_ohlcv_landing


def _seed(count: int = 2) -> list[dict]:
    """Return a small deterministic ticker seed."""
    return [
        {
            "ticker_id": f"T{index}",
            "ticker": f"T{index}",
            "company_name": f"Ticker {index}",
            "exchange": "HOSE",
            "icb_l1": "Financials",
            "icb_l2": "Banking",
            "listing_date": "2020-01-01",
            "is_active": True,
        }
        for index in range(count)
    ]


def _config() -> dict:
    """Return a minimal generator config suitable for unit tests."""
    return {
        "n_tickers": 2,
        "vn30_count": 1,
        "vn30_volume_share": 0.8,
        "days_history": 10,
        "old_schema_share": 0.6,
        "schema_change_date": "2025-07-01",
        "random_seed": 42,
        "base_events_per_min": 1,
        "burst_multiplier": 2,
        "late_arrival_rate": 0.12,
        "late_delay_sec_min_max": [5, 45],
        "stream_days": 1,
    }


def test_schema_window_has_exact_old_new_split():
    """Trading-day allocation follows the configured 60/40 schema split."""
    generator = Generator(_config(), _seed())
    old = [day for day in generator.trading_days if day < date(2025, 7, 1)]
    new = [day for day in generator.trading_days if day >= date(2025, 7, 1)]
    assert len(old) == 6
    assert len(new) == 4


def test_stream_events_respect_sessions_and_daily_totals():
    """Events remain inside sessions and trades reconcile to daily volume."""
    generator = Generator(_config(), _seed())
    ohlcv = generator.generate_ohlcv()
    ohlcv = tag_schema_version(ohlcv, generator.schema_change_date)
    ohlcv = apply_volume_skew(ohlcv, generator.tickers, generator.cfg)
    events = generator.generate_streaming_events(ohlcv)

    allowed = {
        "ATO": [(time(9, 0), time(9, 15))],
        "continuous": [(time(9, 15), time(11, 30)), (time(13, 0), time(14, 30))],
        "ATC": [(time(14, 30), time(14, 45))],
    }
    for event in events:
        event_time = pd.Timestamp(event["event_timestamp"]).time()
        assert any(start <= event_time < end for start, end in allowed[event["session_type"]])

    stream_date = generator.trading_days[-1]
    daily = (
        ohlcv[ohlcv["trade_date"] == stream_date]
        .drop_duplicates(["ticker_id", "trade_date"])
        .set_index("ticker_id")
    )
    trades = pd.DataFrame(event for event in events if event["event_type"] == "trade")
    actual_volume = trades.groupby("ticker")["quantity"].sum()
    for ticker_id, expected in daily["volume"].items():
        assert actual_volume[ticker_id] == expected
        ticker_trades = trades[trades["ticker"] == ticker_id]
        assert (
            ticker_trades["price"]
            .between(
                daily.loc[ticker_id, "low"],
                daily.loc[ticker_id, "high"],
            )
            .all()
        )


def test_physical_schema_evolution(tmp_path, monkeypatch):
    """V1 Parquet omits new fields while v2 physically contains them."""
    monkeypatch.setattr(
        "generator.writers._upload_to_minio",
        lambda *args, **kwargs: False,
    )
    frame = pd.DataFrame(
        [
            {
                "ticker_id": "A",
                "trade_date": date(2025, 6, 30),
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "volume": 100,
                "value": None,
                "foreign_room": None,
                "_schema_version": 1,
            },
            {
                "ticker_id": "A",
                "trade_date": date(2025, 7, 1),
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "volume": 100,
                "value": 150.0,
                "foreign_room": 20.0,
                "_schema_version": 2,
            },
        ]
    )
    legacy_root = tmp_path / "run_date=2025-07-02" / "ohlcv_daily"
    legacy_root.mkdir(parents=True)
    stale_file = legacy_root / "part-00000.parquet"
    frame.to_parquet(stale_file, index=False)

    outputs = write_ohlcv_landing(
        frame,
        "2025-07-02",
        base_dir=str(tmp_path),
    )
    assert not stale_file.exists()
    schemas = {
        int(path.split("schema_version=")[1].split("/")[0]): set(parquet.read_schema(path).names)
        for path in outputs
    }
    assert "value" not in schemas[1]
    assert "foreign_room" not in schemas[1]
    assert {"value", "foreign_room"} <= schemas[2]


def test_drift_config_is_normalized():
    """Coursework drift YAML keys map to the generator's runtime keys."""
    config = load_config("config/generator.yaml")
    assert config["drift"]["enabled"] is True
    assert config["drift"]["start_date"] == "2025-09-01"
