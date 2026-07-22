"""Gold facts — fact_daily_price, fact_intraday_trade."""

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def get_spark(app_name: str = "gold_facts") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def build_fact_daily_price(spark: SparkSession, df: DataFrame, gold_dir: str):
    dim_ticker_path = os.path.join(gold_dir, "dim_ticker")
    dim_date_path = os.path.join(gold_dir, "dim_date")

    if os.path.exists(dim_ticker_path):
        dim_ticker = spark.read.format("delta").load(dim_ticker_path).select("ticker_key", "ticker_id")
        df = df.join(dim_ticker, on="ticker_id", how="left")

    if os.path.exists(dim_date_path):
        dim_date = spark.read.format("delta").load(dim_date_path).select("date_key", "calendar_date")
        df = df.join(dim_date, df.trade_date == dim_date.calendar_date, how="left").drop("calendar_date")

    cols = ["ticker_id", "trade_date", "open", "high", "low", "close",
            "volume", "value", "foreign_room", "_schema_version"]
    if "ticker_key" in df.columns:
        cols.insert(0, "ticker_key")
    if "date_key" in df.columns:
        cols.insert(1 if "ticker_key" in df.columns else 0, "date_key")

    fact = df.select(*cols).withColumn("adj_close", F.col("close"))
    fact = fact.withColumn("_event_timestamp", F.current_timestamp())

    out = os.path.join(gold_dir, "fact_daily_price")
    fact.write.format("delta").mode("overwrite").save(out)
    print(f"  fact_daily_price: {fact.count()} rows -> {out}")


def build_fact_intraday_trade(spark: SparkSession, gold_dir: str):
    schema = "trade_id STRING, ticker_id STRING, trade_date DATE, price DOUBLE, quantity BIGINT, trade_value DOUBLE, ticker_key BIGINT, date_key BIGINT, session_key INT, _event_timestamp TIMESTAMP"
    empty = spark.createDataFrame([], schema=schema)
    out = os.path.join(gold_dir, "fact_intraday_trade")
    empty.write.format("delta").mode("overwrite").save(out)
    print(f"  fact_intraday_trade: stub created -> {out}")
