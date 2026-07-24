import numpy as np
import pandas as pd

from generator.problems import (
    apply_volume_skew,
    inject_duplicates,
    inject_stream_duplicates,
    tag_schema_version,
)


class TestInjectDuplicates:
    def test_injects_correct_count(self):
        rng = np.random.default_rng(42)
        df = pd.DataFrame({"ticker_id": ["A", "B", "C"], "trade_date": ["2025-07-01"] * 3})
        result = inject_duplicates(df, 0.5, ["ticker_id", "trade_date"], rng)
        # 50% of 3 = 1 duplicate → 4 rows
        assert len(result) == 4

    def test_zero_rate_no_change(self):
        rng = np.random.default_rng(42)
        df = pd.DataFrame({"ticker_id": ["A", "B", "C"], "trade_date": ["2025-07-01"] * 3})
        result = inject_duplicates(df, 0.0, ["ticker_id", "trade_date"], rng)
        assert len(result) == 3

    def test_duplicate_rows_identical(self):
        rng = np.random.default_rng(42)
        df = pd.DataFrame(
            {"ticker_id": ["A", "B"], "trade_date": ["2025-07-01"] * 2, "close": [100.0, 200.0]}
        )
        result = inject_duplicates(df, 0.5, ["ticker_id", "trade_date"], rng)
        duplicates = result[result.duplicated(keep=False)]
        assert len(duplicates) == 2  # one pair


class TestInjectStreamDuplicates:
    def test_injects_correct_count(self):
        rng = np.random.default_rng(42)
        events = [
            {"event_id": "1", "data": "a"},
            {"event_id": "2", "data": "b"},
            {"event_id": "3", "data": "c"},
            {"event_id": "4", "data": "d"},
        ]
        result = inject_stream_duplicates(events, 0.5, rng)
        assert len(result) == 6  # 4 + 2 duplicates

    def test_zero_rate_no_change(self):
        rng = np.random.default_rng(42)
        events = [{"event_id": "1"}, {"event_id": "2"}]
        result = inject_stream_duplicates(events, 0.0, rng)
        assert len(result) == 2

    def test_replays_follow_originals_in_arrival_order(self):
        rng = np.random.default_rng(42)
        events = [
            {
                "event_id": "1",
                "created_ts": "2025-07-01T09:00:00",
            },
            {
                "event_id": "2",
                "created_ts": "2025-07-01T09:01:00",
            },
        ]
        result = inject_stream_duplicates(events, 0.5, rng)
        assert [event["created_ts"] for event in result] == sorted(
            event["created_ts"] for event in result
        )
        replay = next(event for event in result if event.get("_is_replay"))
        original = next(
            event
            for event in result
            if event["event_id"] == replay["event_id"] and not event.get("_is_replay")
        )
        assert replay["created_ts"] > original["created_ts"]


class TestApplyVolumeSkew:
    def test_redistributes_volume(self):
        tickers_df = pd.DataFrame(
            {
                "ticker_id": ["A", "B", "C", "D"],
                "vn30": [True, True, False, False],
            }
        )
        df = pd.DataFrame(
            {
                "ticker_id": ["A", "B", "C", "D"],
                "trade_date": ["2025-07-01"] * 4,
                "close": [100.0] * 4,
                "volume": [10000, 10000, 10000, 10000],
                "value": [1000000.0] * 4,
            }
        )
        cfg = {"vn30_volume_share": 0.80}
        result = apply_volume_skew(df, tickers_df, cfg)

        total = result["volume"].sum()
        vn30_total = result[result["ticker_id"].isin(["A", "B"])]["volume"].sum()
        non_vn30_total = result[result["ticker_id"].isin(["C", "D"])]["volume"].sum()

        assert abs(vn30_total / total - 0.80) < 0.01
        assert abs(non_vn30_total / total - 0.20) < 0.01

    def test_value_updated(self):
        tickers_df = pd.DataFrame(
            {
                "ticker_id": ["X"],
                "vn30": [True],
            }
        )
        df = pd.DataFrame(
            {
                "ticker_id": ["X"],
                "trade_date": ["2025-07-01"],
                "close": [100.0],
                "volume": [1000],
                "value": [0.0],
            }
        )
        cfg = {"vn30_volume_share": 1.0}
        result = apply_volume_skew(df, tickers_df, cfg)
        # value should be close * volume after skew
        assert abs(result["value"].iloc[0] - 100.0 * result["volume"].iloc[0]) < 1


class TestTagSchemaVersion:
    def test_tags_before_change_date(self):
        df = pd.DataFrame({"trade_date": pd.to_datetime(["2025-06-01", "2025-06-30"])})
        result = tag_schema_version(df, pd.Timestamp("2025-07-01"))
        assert list(result["_schema_version"]) == [1, 1]

    def test_tags_after_change_date(self):
        df = pd.DataFrame({"trade_date": pd.to_datetime(["2025-07-01", "2025-07-02"])})
        result = tag_schema_version(df, pd.Timestamp("2025-07-01"))
        assert list(result["_schema_version"]) == [2, 2]

    def test_tags_mixed(self):
        df = pd.DataFrame({"trade_date": pd.to_datetime(["2025-06-30", "2025-07-01"])})
        result = tag_schema_version(df, pd.Timestamp("2025-07-01"))
        assert list(result["_schema_version"]) == [1, 2]
