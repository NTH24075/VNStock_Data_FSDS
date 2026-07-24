"""Gold dimensions — dim_date, dim_ticker (SCD2), dim_industry, dim_exchange, dim_session."""

import logging
import os

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def read_silver_daily(spark: SparkSession, silver_dir: str) -> DataFrame:
    """Read the canonical daily-price Silver table."""
    path = os.path.join(silver_dir, "stg_daily_price")
    if not os.path.exists(path):
        raise FileNotFoundError(f"No Silver data at {path}")
    return spark.read.format("delta").load(path)


def build_dim_date(
    spark: SparkSession,
    df: DataFrame,
    gold_dir: str,
) -> DataFrame:
    """Build a gap-aware calendar dimension including non-trading dates."""
    observed = df.select(F.to_date("trade_date").alias("calendar_date")).distinct()
    bounds = observed.agg(
        F.min("calendar_date").alias("min_date"),
        F.max("calendar_date").alias("max_date"),
    )
    dates = bounds.select(F.explode(F.sequence("min_date", "max_date")).alias("calendar_date"))
    dim = (
        dates.join(
            observed.withColumn("_observed", F.lit(True)),
            on="calendar_date",
            how="left",
        )
        .select(
            F.date_format("calendar_date", "yyyyMMdd").cast("int").alias("date_key"),
            F.col("calendar_date"),
            F.date_format("calendar_date", "EEEE").alias("day_of_week"),
            F.month("calendar_date").alias("month"),
            F.quarter("calendar_date").alias("quarter"),
            F.year("calendar_date").alias("year"),
            F.coalesce("_observed", F.lit(False)).alias("is_trading_day"),
            F.when(
                F.dayofweek("calendar_date").isin([1, 7]),
                F.lit("weekend"),
            )
            .when(F.col("_observed").isNull(), F.lit("market_holiday"))
            .otherwise(F.lit(None))
            .cast("string")
            .alias("holiday_name"),
        )
        .orderBy("calendar_date")
    )
    out = os.path.join(gold_dir, "dim_date")
    (
        dim.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(out)
    )
    trading = dim.filter(F.col("is_trading_day")).count()
    nontrading = dim.filter(~F.col("is_trading_day")).count()
    logger.info(
        "  dim_date: %d rows (%d trading, %d non-trading) -> %s",
        dim.count(),
        trading,
        nontrading,
        out,
    )
    return dim


def build_dim_ticker(spark: SparkSession, df: DataFrame, gold_dir: str) -> DataFrame:
    """Apply an idempotent SCD Type 2 merge for ticker attributes."""
    seed_path = os.getenv(
        "SEED_PATH",
        os.path.join(
            os.path.dirname(__file__), "..", "..", "generator", "seed", "tickers_reference.json"
        ),
    )
    tickers = df.select("ticker_id").distinct().orderBy("ticker_id")

    if os.path.exists(seed_path):
        seed_df = spark.read.option("multiline", "true").json(seed_path)
        tickers = tickers.join(seed_df, on="ticker_id", how="left")
    else:
        tickers = (
            tickers.withColumn("ticker", F.col("ticker_id"))
            .withColumn("company_name", F.lit("Unknown"))
            .withColumn("exchange", F.lit("HOSE"))
            .withColumn("icb_l1", F.lit(None).cast("string"))
            .withColumn("icb_l2", F.lit(None).cast("string"))
        )

    vn30_window = Window.orderBy("ticker_id")
    incoming = tickers.select(
        F.col("ticker_id"),
        F.coalesce(F.col("ticker"), F.col("ticker_id")).alias("ticker"),
        F.coalesce(F.col("company_name"), F.lit("Unknown")).alias("company_name"),
        F.coalesce(F.col("exchange"), F.lit("HOSE")).alias("exchange"),
        F.when(F.coalesce(F.col("is_active"), F.lit(True)), F.lit("active"))
        .otherwise(F.lit("inactive"))
        .alias("listing_status"),
        F.col("icb_l1"),
        F.col("icb_l2"),
        (F.row_number().over(vn30_window) <= 30).alias("is_vn30"),
    )
    out = os.path.join(gold_dir, "dim_ticker")

    attribute_columns = [
        "ticker",
        "company_name",
        "exchange",
        "listing_status",
        "icb_l1",
        "icb_l2",
        "is_vn30",
    ]
    effective_ts = spark.sql("SELECT current_timestamp() AS effective_ts").first().effective_ts
    if os.path.exists(out):
        existing = spark.read.format("delta").load(out).cache()
        existing.count()
        # Older coursework runs may predate a newly registered SCD2 attribute.
        # Add the missing field with the incoming type so the normal SCD2 change
        # detection expires the legacy row and creates a complete current row.
        incoming_fields = {field.name: field.dataType for field in incoming.schema.fields}
        for column in attribute_columns:
            if column not in existing.columns:
                existing = existing.withColumn(
                    column,
                    F.lit(None).cast(incoming_fields[column]),
                )
        current = existing.filter(F.col("is_current")).alias("old")
        new = incoming.alias("new")
        changed_condition = ~F.lit(True)
        for column in attribute_columns:
            changed_condition = changed_condition | ~F.col(f"old.{column}").eqNullSafe(
                F.col(f"new.{column}")
            )

        changed_ids = (
            current.join(new, on="ticker_id", how="inner")
            .filter(changed_condition)
            .select("ticker_id")
        )
        new_ids = incoming.join(
            current.select("ticker_id"),
            on="ticker_id",
            how="left_anti",
        ).select("ticker_id")
        version_ids = changed_ids.unionByName(new_ids).distinct()

        expired = (
            existing.join(
                changed_ids.withColumn("_changed", F.lit(True)),
                on="ticker_id",
                how="left",
            )
            .withColumn(
                "valid_to_ts",
                F.when(
                    F.col("_changed") & F.col("is_current"),
                    F.lit(effective_ts).cast("timestamp"),
                ).otherwise(F.col("valid_to_ts")),
            )
            .withColumn(
                "is_current",
                F.when(
                    F.col("_changed") & F.col("is_current"),
                    F.lit(False),
                ).otherwise(F.col("is_current")),
            )
            .drop("_changed")
        )
        new_versions = incoming.join(version_ids, on="ticker_id", how="inner")
        incoming = expired.unionByName(
            _as_scd2_versions(new_versions, effective_ts),
        )
    else:
        incoming = _as_scd2_versions(incoming, effective_ts)

    dim = incoming
    (
        dim.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(out)
    )
    logger.info("  dim_ticker (SCD2): %d rows -> %s", dim.count(), out)
    return dim


def _as_scd2_versions(incoming: DataFrame, effective_ts) -> DataFrame:
    """Attach surrogate key and SCD2 control columns to incoming versions."""
    return (
        incoming.withColumn(
            "valid_from_ts",
            F.lit(effective_ts).cast("timestamp"),
        )
        .withColumn("valid_to_ts", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn(
            "ticker_key",
            F.xxhash64(
                "ticker_id",
                F.col("valid_from_ts").cast("string"),
            ),
        )
        .select(
            "ticker_key",
            "ticker_id",
            "ticker",
            "company_name",
            "exchange",
            "listing_status",
            "icb_l1",
            "icb_l2",
            "is_vn30",
            "valid_from_ts",
            "valid_to_ts",
            "is_current",
        )
    )


def build_dim_industry(spark: SparkSession, gold_dir: str) -> DataFrame:
    """Build the static ICB level-one industry dimension."""
    rows = [
        (1, "BANK", "L1", "Ngân hàng"),
        (2, "REAL", "L1", "Bất động sản"),
        (3, "SEC", "L1", "Chứng khoán"),
        (4, "FOOD", "L1", "Thực phẩm & đồ uống"),
        (5, "OIL", "L1", "Dầu khí"),
        (6, "STEEL", "L1", "Thép"),
        (7, "TECH", "L1", "Công nghệ"),
        (8, "RETAIL", "L1", "Bán lẻ"),
        (9, "UTIL", "L1", "Tiện ích"),
        (10, "OTHER", "L1", "Khác"),
    ]
    dim = spark.createDataFrame(
        rows,
        ["industry_key", "icb_code", "icb_level", "icb_name"],
    )
    out = os.path.join(gold_dir, "dim_industry")
    (
        dim.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(out)
    )
    logger.info("  dim_industry: %d rows -> %s", dim.count(), out)
    return dim


def build_dim_exchange(spark: SparkSession, gold_dir: str) -> DataFrame:
    """Build the static exchange and daily-price-limit dimension."""
    rows = [(1, "HOSE", 0.07), (2, "HNX", 0.10), (3, "UPCOM", 0.15)]
    dim = spark.createDataFrame(rows, ["exchange_key", "exchange_code", "price_limit_pct"])
    out = os.path.join(gold_dir, "dim_exchange")
    (
        dim.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(out)
    )
    logger.info("  dim_exchange: %d rows -> %s", dim.count(), out)
    return dim


def build_dim_session(spark: SparkSession, gold_dir: str) -> DataFrame:
    """Build the static Vietnamese exchange-session dimension."""
    rows = [(1, "ATO"), (2, "CONTINUOUS"), (3, "ATC"), (4, "PUT_THROUGH")]
    dim = spark.createDataFrame(rows, ["session_key", "session_type"])
    out = os.path.join(gold_dir, "dim_session")
    (
        dim.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(out)
    )
    logger.info("  dim_session: %d rows -> %s", dim.count(), out)
    return dim
