"""Build conformed daily, foreign-flow, and intraday Gold facts."""

import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def build_fact_daily_price(
    spark: SparkSession,
    df: DataFrame,
    gold_dir: str,
) -> DataFrame:
    """Conform Silver prices to dimensions and compute adjusted close."""
    dim_ticker_path = os.path.join(gold_dir, "dim_ticker")
    dim_date_path = os.path.join(gold_dir, "dim_date")

    if os.path.exists(dim_ticker_path):
        dim_ticker = (
            spark.read.format("delta")
            .load(dim_ticker_path)
            .filter(F.col("is_current"))
            .select("ticker_key", "ticker_id")
        )
        df = df.join(dim_ticker, on="ticker_id", how="left")

    if os.path.exists(dim_date_path):
        dim_date = (
            spark.read.format("delta").load(dim_date_path).select("date_key", "calendar_date")
        )
        df = df.join(dim_date, df.trade_date == dim_date.calendar_date, how="left").drop(
            "calendar_date"
        )

    cols = [
        "ticker_id",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "value",
        "foreign_room",
        "_schema_version",
    ]
    if "ticker_key" in df.columns:
        cols.insert(0, "ticker_key")
    if "date_key" in df.columns:
        cols.insert(1 if "ticker_key" in df.columns else 0, "date_key")

    fact = df.select(*cols)
    actions_path = os.getenv(
        "CORPORATE_ACTIONS_PATH",
        os.path.join(
            os.getenv("BRONZE_DIR", "data/bronze"),
            "raw_corporate_actions",
        ),
    )
    if os.path.exists(actions_path):
        actions = (
            spark.read.format("delta")
            .load(actions_path)
            .filter(F.col("action_type").isin("split", "stock_dividend"))
            .select(
                "ticker_id",
                F.col("ex_date").cast("date").alias("action_date"),
                F.when(F.col("action_type") == "split", F.col("ratio"))
                .otherwise(F.lit(1.0) + F.col("ratio"))
                .alias("adjustment_factor"),
            )
        )
        price_columns = fact.columns
        fact = (
            fact.alias("price")
            .join(
                actions.alias("action"),
                (F.col("price.ticker_id") == F.col("action.ticker_id"))
                & (F.col("action.action_date") > F.col("price.trade_date")),
                how="left",
            )
            .groupBy(*[F.col(f"price.{column}") for column in price_columns])
            .agg(
                F.exp(F.sum(F.log(F.coalesce("adjustment_factor", F.lit(1.0))))).alias(
                    "_cumulative_factor"
                )
            )
            .withColumn(
                "adj_close",
                F.col("close") / F.coalesce("_cumulative_factor", F.lit(1.0)),
            )
            .drop("_cumulative_factor")
        )
    else:
        fact = fact.withColumn("adj_close", F.col("close"))
    fact = fact.withColumn("created_ts", F.current_timestamp())

    out = os.path.join(gold_dir, "fact_daily_price")
    (
        fact.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("trade_date")
        .save(out)
    )
    logger.info("  fact_daily_price: %d rows -> %s", fact.count(), out)
    return fact


def build_fact_foreign_flow(
    spark: SparkSession,
    silver_dir: str,
    gold_dir: str,
) -> DataFrame | None:
    """Build the daily foreign-flow fact with derived net value."""
    source = os.path.join(silver_dir, "stg_foreign_flow")
    if not os.path.exists(source):
        logger.warning("stg_foreign_flow is absent; skipping fact_foreign_flow")
        return None
    fact = spark.read.format("delta").load(source)
    fact = fact.withColumn(
        "foreign_net_value",
        F.col("foreign_buy_value") - F.col("foreign_sell_value"),
    )
    out = os.path.join(gold_dir, "fact_foreign_flow")
    (
        fact.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("trade_date")
        .save(out)
    )
    logger.info("  fact_foreign_flow: %d rows -> %s", fact.count(), out)
    return fact


def build_fact_intraday_trade(
    spark: SparkSession,
    gold_dir: str,
    silver_dir: str | None = None,
) -> DataFrame | None:
    """Build the intraday trade fact from deduplicated Silver stream rows."""
    silver_dir = silver_dir or os.getenv("SILVER_DIR", "data/silver")
    source = os.path.join(silver_dir, "stg_trades")
    if not os.path.exists(source):
        logger.warning("stg_trades is absent; skipping fact_intraday_trade")
        return None

    trades = (
        spark.read.format("delta")
        .load(source)
        .withColumnRenamed("ticker", "ticker_id")
        .withColumn("trade_date", F.to_date("event_ts"))
        .withColumn("trade_value", F.col("price") * F.col("quantity"))
    )
    ticker = (
        spark.read.format("delta")
        .load(os.path.join(gold_dir, "dim_ticker"))
        .filter(F.col("is_current"))
        .select("ticker_id", "ticker_key")
    )
    dates = (
        spark.read.format("delta")
        .load(os.path.join(gold_dir, "dim_date"))
        .select("calendar_date", "date_key")
    )
    sessions = spark.read.format("delta").load(os.path.join(gold_dir, "dim_session"))
    fact = (
        trades.join(F.broadcast(ticker), on="ticker_id", how="left")
        .join(
            F.broadcast(dates),
            trades.trade_date == dates.calendar_date,
            how="left",
        )
        .drop("calendar_date")
        .join(
            F.broadcast(sessions),
            F.upper(trades.session_type) == sessions.session_type,
            how="left",
        )
        .select(
            "trade_id",
            "event_id",
            "ticker_id",
            "trade_date",
            "event_ts",
            "created_ts_parsed",
            "price",
            "quantity",
            "trade_value",
            "side",
            "ticker_key",
            "date_key",
            "session_key",
        )
    )
    out = os.path.join(gold_dir, "fact_intraday_trade")
    (
        fact.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("trade_date")
        .save(out)
    )
    logger.info("  fact_intraday_trade: %d rows -> %s", fact.count(), out)
    return fact
