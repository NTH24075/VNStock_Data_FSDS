"""Gold dimensions — dim_date, dim_ticker (SCD2), dim_industry, dim_exchange, dim_session."""

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def get_spark(app_name: str = "gold_dimensions") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def read_silver_daily(spark: SparkSession, silver_dir: str) -> DataFrame:
    path = os.path.join(silver_dir, "stg_daily_price")
    if not os.path.exists(path):
        raise FileNotFoundError(f"No Silver data at {path}")
    return spark.read.format("delta").load(path)


def build_dim_date(spark: SparkSession, df: DataFrame, gold_dir: str):
    dates = df.select("trade_date").distinct().orderBy("trade_date")
    dim = dates.select(
        F.xxhash64(F.col("trade_date").cast("string")).alias("date_key"),
        F.col("trade_date").cast("string").alias("date_key_str"),
        F.col("trade_date").alias("calendar_date"),
        (~F.dayofweek(F.col("trade_date")).isin([1, 7])).alias("is_trading_day"),
        F.when(F.dayofweek(F.col("trade_date")).isin([1, 7]), F.lit("weekend")).otherwise(F.lit(None)).cast("string").alias("holiday_name"),
    )
    out = os.path.join(gold_dir, "dim_date")
    dim.write.format("delta").mode("overwrite").save(out)
    trading = dim.filter(F.col("is_trading_day")).count()
    nontrading = dim.filter(~F.col("is_trading_day")).count()
    print(f"  dim_date: {dim.count()} rows ({trading} trading, {nontrading} non-trading) -> {out}")


def build_dim_ticker(spark: SparkSession, df: DataFrame, gold_dir: str) -> DataFrame:
    seed_path = os.getenv(
        "SEED_PATH",
        os.path.join(os.path.dirname(__file__), "..", "..", "generator", "seed", "tickers_reference.json"),
    )
    tickers = df.select("ticker_id").distinct().orderBy("ticker_id")

    if os.path.exists(seed_path):
        seed_df = spark.read.option("multiline", "true").json(seed_path)
        tickers = tickers.join(seed_df, on="ticker_id", how="left")
    else:
        tickers = (
            tickers
            .withColumn("ticker", F.col("ticker_id"))
            .withColumn("company_name", F.lit("Unknown"))
            .withColumn("exchange", F.lit("HOSE"))
            .withColumn("icb_l1", F.lit(None).cast("string"))
            .withColumn("icb_l2", F.lit(None).cast("string"))
        )

    dim = tickers.select(
        F.xxhash64(F.col("ticker_id")).alias("ticker_key"),
        F.col("ticker_id"),
        F.coalesce(F.col("ticker"), F.col("ticker_id")).alias("ticker"),
        F.coalesce(F.col("company_name"), F.lit("Unknown")).alias("company_name"),
        F.coalesce(F.col("exchange"), F.lit("HOSE")).alias("exchange"),
        F.lit("active").alias("listing_status"),
        F.col("icb_l1"),
        F.col("icb_l2"),
        F.current_timestamp().alias("valid_from_ts"),
        F.lit(None).cast("timestamp").alias("valid_to_ts"),
        F.lit(True).alias("is_current"),
    )
    out = os.path.join(gold_dir, "dim_ticker")
    dim.write.format("delta").mode("overwrite").save(out)
    print(f"  dim_ticker (SCD2): {dim.count()} rows -> {out}")
    return dim


def build_dim_industry(spark: SparkSession, gold_dir: str):
    rows = [(1, "BANK", "L1", "Ngân hàng"),
            (2, "REAL", "L1", "Bất động sản"),
            (3, "SEC", "L1", "Chứng khoán"),
            (4, "FOOD", "L1", "Thực phẩm & đồ uống"),
            (5, "OIL", "L1", "Dầu khí"),
            (6, "STEEL", "L1", "Thép"),
            (7, "TECH", "L1", "Công nghệ"),
            (8, "RETAIL", "L1", "Bán lẻ"),
            (9, "UTIL", "L1", "Tiện ích"),
            (10, "OTHER", "L1", "Khác")]
    dim = spark.createDataFrame(rows, ["industry_key", "icb_code", "icb_level", "icb_name"])
    out = os.path.join(gold_dir, "dim_industry")
    dim.write.format("delta").mode("overwrite").save(out)
    print(f"  dim_industry: {dim.count()} rows -> {out}")


def build_dim_exchange(spark: SparkSession, gold_dir: str):
    rows = [(1, "HOSE", 0.07),
            (2, "HNX", 0.10),
            (3, "UPCOM", 0.15)]
    dim = spark.createDataFrame(rows, ["exchange_key", "exchange_code", "price_limit_pct"])
    out = os.path.join(gold_dir, "dim_exchange")
    dim.write.format("delta").mode("overwrite").save(out)
    print(f"  dim_exchange: {dim.count()} rows -> {out}")


def build_dim_session(spark: SparkSession, gold_dir: str):
    rows = [(1, "ATO"),
            (2, "CONTINUOUS"),
            (3, "ATC"),
            (4, "PUT_THROUGH")]
    dim = spark.createDataFrame(rows, ["session_key", "session_type"])
    out = os.path.join(gold_dir, "dim_session")
    dim.write.format("delta").mode("overwrite").save(out)
    print(f"  dim_session: {dim.count()} rows -> {out}")
