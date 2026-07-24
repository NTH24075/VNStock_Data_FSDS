"""Silver stream — raw_market_events Delta → stg_trades, stg_quotes.

Dedup by event_id, event-time watermark = 60s, late-flag for quarantine.
Per 02_schema_design.md Section 7.2.
"""

import logging
import os

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from jobs.spark_session import get_spark

logger = logging.getLogger(__name__)

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("event_timestamp", StringType()),
        StructField("created_ts", StringType()),
        StructField("session_type", StringType()),
        StructField("ticker", StringType()),
        StructField("exchange", StringType()),
        StructField("price", DoubleType()),
        StructField("quantity", LongType()),
        StructField("side", StringType()),
        StructField("trade_id", StringType()),
        StructField("bid_price", DoubleType()),
        StructField("bid_qty", LongType()),
        StructField("ask_price", DoubleType()),
        StructField("ask_qty", LongType()),
        StructField("index_name", StringType()),
        StructField("index_value", DoubleType()),
        StructField("index_change_pct", DoubleType()),
    ]
)


def run_silver_stream(
    bronze_dir: str = "data/bronze",
    silver_dir: str = "data/silver",
    checkpoint_dir: str = "data/silver/_checkpoints/stream",
    watermark_sec: int = 60,
    available_now: bool = False,
):
    """Parse, deduplicate, and quality-route Bronze market events."""
    spark = get_spark(
        "silver_stream",
        {
            "spark.sql.streaming.schemaInference": "true",
        },
    )

    raw_path = os.path.join(bronze_dir, "raw_market_events")
    if not os.path.exists(raw_path):
        logger.warning("raw_market_events not found — waiting for bronze_stream_ingest")
        return spark, None

    raw = spark.readStream.format("delta").load(raw_path)

    parsed = (
        raw.withColumn("evt", F.from_json(F.col("payload_json"), EVENT_SCHEMA))
        .select(
            F.col("evt.*"),
            F.col("source_offset"),
            F.col("kafka_ts"),
            F.col("_ingested_at"),
        )
        .withColumn("event_ts", F.to_timestamp(F.col("event_timestamp")))
        .withColumn("created_ts_parsed", F.to_timestamp(F.col("created_ts")))
        .withWatermark("event_ts", f"{watermark_sec} seconds")
    )

    deduped = parsed.dropDuplicatesWithinWatermark(
        ["event_id"],
    )

    deduped = deduped.withColumn(
        "_late_flag",
        F.when(
            F.col("event_ts").isNotNull()
            & (
                F.unix_timestamp(F.col("created_ts_parsed")) - F.unix_timestamp(F.col("event_ts"))
                > watermark_sec
            ),
            True,
        ).otherwise(False),
    )

    accepted = deduped.filter(~F.col("_late_flag"))
    quarantine = deduped.filter(F.col("_late_flag")).select(
        "event_id",
        "event_type",
        "event_ts",
        "created_ts_parsed",
        "session_type",
        "ticker",
        "exchange",
        "source_offset",
        "kafka_ts",
        "_ingested_at",
    )
    trades = accepted.filter(F.col("event_type") == "trade").select(
        "event_id",
        "event_ts",
        "created_ts_parsed",
        "session_type",
        "ticker",
        "exchange",
        "price",
        "quantity",
        "side",
        "trade_id",
        "source_offset",
        "kafka_ts",
        "_ingested_at",
        "_late_flag",
    )
    quotes = accepted.filter(F.col("event_type") == "quote").select(
        "event_id",
        "event_ts",
        "created_ts_parsed",
        "session_type",
        "ticker",
        "exchange",
        "bid_price",
        "bid_qty",
        "ask_price",
        "ask_qty",
        "source_offset",
        "kafka_ts",
        "_ingested_at",
        "_late_flag",
    )

    trade_out = os.path.join(silver_dir, "stg_trades")
    quote_out = os.path.join(silver_dir, "stg_quotes")
    quarantine_out = os.path.join(silver_dir, "stg_events_quarantine")

    logger.info("Silver stream: raw_market_events -> %s + %s", trade_out, quote_out)

    trade_writer = (
        trades.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", os.path.join(checkpoint_dir, "trades"))
    )
    quote_writer = (
        quotes.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", os.path.join(checkpoint_dir, "quotes"))
    )
    quarantine_writer = (
        quarantine.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", os.path.join(checkpoint_dir, "quarantine"))
    )
    if available_now:
        trade_writer = trade_writer.trigger(availableNow=True)
        quote_writer = quote_writer.trigger(availableNow=True)
        quarantine_writer = quarantine_writer.trigger(availableNow=True)
    else:
        trade_writer = trade_writer.trigger(processingTime="30 seconds")
        quote_writer = quote_writer.trigger(processingTime="30 seconds")
        quarantine_writer = quarantine_writer.trigger(processingTime="30 seconds")

    queries = []
    try:
        queries.append(trade_writer.start(trade_out))
        queries.append(quote_writer.start(quote_out))
        queries.append(quarantine_writer.start(quarantine_out))
    except Exception:
        for query in queries:
            query.stop()
        spark.stop()
        raise

    return spark, queries
