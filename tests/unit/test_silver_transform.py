import pytest

from jobs.silver.daily import dedup, validate_domain


@pytest.fixture(scope="module")
def spark():
    try:
        from pyspark.sql import SparkSession
        session = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        yield session
        session.stop()
    except Exception:
        pytest.skip("Java/Spark unavailable")


class TestDedup:
    def test_removes_duplicates_by_business_key(self, spark):
        df = spark.createDataFrame(
            [("A", "2025-07-01", "2025-07-01T10:00:00", 100.0),
             ("A", "2025-07-01", "2025-07-01T11:00:00", 101.0),
             ("B", "2025-07-01", "2025-07-01T10:00:00", 200.0)],
            ["ticker_id", "trade_date", "_ingested_at", "close"],
        )
        result = dedup(df)
        assert result.count() == 2
        # The later ingest should be kept for ticker A
        row = result.filter("ticker_id = 'A'").collect()[0]
        assert row.close == 101.0

    def test_no_duplicates_unchanged(self, spark):
        df = spark.createDataFrame(
            [("A", "2025-07-01", "2025-07-01T10:00:00", 100.0),
             ("B", "2025-07-02", "2025-07-01T10:00:00", 200.0)],
            ["ticker_id", "trade_date", "_ingested_at", "close"],
        )
        result = dedup(df)
        assert result.count() == 2

    def test_drops_row_number_column(self, spark):
        df = spark.createDataFrame(
            [("A", "2025-07-01", "2025-07-01T10:00:00", 100.0),
             ("B", "2025-07-02", "2025-07-01T10:00:00", 200.0)],
            ["ticker_id", "trade_date", "_ingested_at", "close"],
        )
        result = dedup(df)
        assert "_rn" not in result.columns


class TestValidateDomain:
    def test_adds_dq_columns(self, spark):
        df = spark.createDataFrame(
            [("A", "2025-07-01", 100.0, 105.0, 95.0, 102.0, 1000)],
            ["ticker_id", "trade_date", "open", "high", "low", "close", "volume"],
        )
        result = validate_domain(df)
        assert "_dq_price_positive" in result.columns
        assert "_dq_high_ge_max" in result.columns
        assert "_dq_low_le_min" in result.columns
        assert "_dq_volume_nonneg" in result.columns

    def test_flags_negative_price(self, spark):
        df = spark.createDataFrame(
            [("A", "2025-07-01", 100.0, 105.0, 95.0, -50.0, 1000)],
            ["ticker_id", "trade_date", "open", "high", "low", "close", "volume"],
        )
        result = validate_domain(df)
        row = result.collect()[0]
        assert not row._dq_price_positive

    def test_flags_negative_volume(self, spark):
        df = spark.createDataFrame(
            [("A", "2025-07-01", 100.0, 105.0, 95.0, 102.0, -100)],
            ["ticker_id", "trade_date", "open", "high", "low", "close", "volume"],
        )
        result = validate_domain(df)
        row = result.collect()[0]
        assert not row._dq_volume_nonneg

    def test_valid_data_all_pass(self, spark):
        df = spark.createDataFrame(
            [("A", "2025-07-01", 100.0, 105.0, 95.0, 102.0, 1000),
             ("B", "2025-07-01", 200.0, 210.0, 190.0, 205.0, 2000)],
            ["ticker_id", "trade_date", "open", "high", "low", "close", "volume"],
        )
        result = validate_domain(df)
        bad = result.filter(
            "not (_dq_price_positive and _dq_volume_nonneg and _dq_high_ge_max and _dq_low_le_min)"
        ).count()
        assert bad == 0
