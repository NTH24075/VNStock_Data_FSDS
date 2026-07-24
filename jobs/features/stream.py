"""Stream features — stg_trades → feat_stream_intraday.

Tumbling 5-min volume, sliding 30-min trade count, momentum, burst flag.
Per 02_schema_design.md Section 6 + Section 7.6.
"""

import logging
import os

from pyspark.sql import functions as F

from jobs.spark_session import get_spark

logger = logging.getLogger(__name__)


def run_feat_stream(
    silver_dir: str = "data/silver",
    gold_dir: str = "data/gold",
    checkpoint_dir: str = "data/gold/_checkpoints/feat_stream",
    available_now: bool = False,
):
    """Build 5-minute-volume and 30-minute sliding trade features."""
    spark = get_spark("feat_stream")

    trades_path = os.path.join(silver_dir, "stg_trades")
    if not os.path.exists(trades_path):
        logger.warning("stg_trades not found — waiting for silver_stream")
        return spark, None

    trades = (
        spark.readStream.format("delta")
        .load(trades_path)
        .withColumn("event_ts", F.col("event_ts").cast("timestamp"))
        .withWatermark("event_ts", "60 seconds")
    )

    vol_5m = (
        trades.groupBy(
            F.window("event_ts", "5 minutes"),
            F.col("ticker"),
        )
        .agg(F.sum("quantity").alias("f_stream_volume_5m"))
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
    )

    count_30m = (
        trades.groupBy(
            F.window("event_ts", "30 minutes", "1 minute"),
            F.col("ticker"),
        )
        .agg(
            F.count("*").alias("f_stream_trade_count_30m"),
            F.min_by("price", "event_ts").alias("_first_price"),
            F.max_by("price", "event_ts").alias("_last_price"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
    )

    feat = vol_5m.drop("window_start").join(
        count_30m.drop("window_start"),
        on=["ticker", "window_end"],
        how="inner",
    )

    ticker_count = int(os.getenv("STREAM_TICKER_COUNT", "400"))
    baseline_per_ticker_30m = 200 * 30 / max(ticker_count, 1)

    feat = feat.select(
        F.col("ticker").alias("ticker_id"),
        F.col("window_end").alias("event_timestamp"),
        F.current_timestamp().alias("created_ts"),
        F.coalesce(F.col("f_stream_volume_5m"), F.lit(0)).alias("f_stream_volume_5m"),
        F.coalesce(F.col("f_stream_trade_count_30m"), F.lit(0)).alias("f_stream_trade_count_30m"),
        F.when(
            F.col("_first_price") > 0,
            (F.col("_last_price") - F.col("_first_price")) / F.col("_first_price"),
        ).alias("f_stream_price_momentum_30m"),
        (F.col("f_stream_trade_count_30m") > F.lit(5 * baseline_per_ticker_30m))
        .cast("int")
        .alias("f_stream_burst_flag"),
    )

    out = os.path.join(gold_dir, "feat_stream_intraday")

    logger.info("Feat stream: stg_trades -> %s", out)

    writer = (
        feat.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_dir)
    )
    writer = (
        writer.trigger(availableNow=True)
        if available_now
        else writer.trigger(processingTime="1 minute")
    )
    try:
        query = writer.start(out)
    except Exception:
        spark.stop()
        raise

    return spark, query
