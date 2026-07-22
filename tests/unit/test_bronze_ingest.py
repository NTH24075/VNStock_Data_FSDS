import pytest

from jobs.bronze.offline import add_ingest_metadata, validate_contract


@pytest.fixture(scope="module")
def spark():
    try:
        from pyspark.sql import SparkSession
        session = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        yield session
        session.stop()
    except Exception:
        pytest.skip("Java/Spark unavailable")


class TestValidateContract:
    def test_passes_when_all_required_present(self, spark):
        contract = {
            "required": ["ticker_id", "trade_date", "open", "close", "volume"],
        }
        df = spark.createDataFrame(
            [("A", "2025-07-01", 100.0, 101.0, 1000, "x")],
            ["ticker_id", "trade_date", "open", "close", "volume", "extra_col"],
        )
        validate_contract(df, contract, 1)

    def test_raises_on_missing_required(self, spark):
        contract = {
            "required": ["ticker_id", "trade_date", "open", "close", "volume"],
        }
        df = spark.createDataFrame(
            [("A", "2025-07-01")],
            ["ticker_id", "trade_date"],
        )
        with pytest.raises(ValueError, match="missing required columns"):
            validate_contract(df, contract, 1)

    def test_extra_columns_allowed(self, spark):
        contract = {"required": ["ticker_id"]}
        df = spark.createDataFrame([("A", 42)], ["ticker_id", "bonus"])
        validate_contract(df, contract, 1)


class TestAddIngestMetadata:
    def test_adds_metadata_columns(self, spark):
        df = spark.createDataFrame(
            [("A", 100.0), ("B", 200.0)],
            ["ticker_id", "close"],
        )
        result = add_ingest_metadata(df, "20250101_120000", 2)

        assert "_ingested_at" in result.columns
        assert "_batch_id" in result.columns
        assert "_schema_version" in result.columns
        assert result.filter("_batch_id = '20250101_120000'").count() == 2
        assert result.filter("_schema_version = 2").count() == 2

    def test_original_data_preserved(self, spark):
        df = spark.createDataFrame([("A", 100.0)], ["ticker_id", "close"])
        result = add_ingest_metadata(df, "batch1", 1)
        rows = result.collect()
        assert rows[0].ticker_id == "A"
        assert rows[0].close == 100.0

    def test_does_not_mutate_original(self, spark):
        df = spark.createDataFrame([("A",)], ["ticker_id"])
        add_ingest_metadata(df, "batch1", 1)
        assert "_ingested_at" not in df.columns
