"""Silver stream tables — load committed PyFlink JSONL output into Delta.

The Flink application (jobs/flink/silver_stream.py) owns event-time dedup,
watermarking and quarantine. This batch step only lands its committed files as
Delta tables (stg_trades, stg_quotes, stg_events_quarantine) for Gold and the
stream-feature job. Rows are insert-only merged on event_id, so reruns are
idempotent and the tables stay append-only for Delta streaming readers.
"""

import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

logger = logging.getLogger(__name__)

FLINK_EVENT_SCHEMA = StructType(
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
        StructField("kafka_offset", LongType()),
    ]
)

QUARANTINE_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("event_ts", StringType()),
        StructField("reason", StringType()),
    ]
)

COMMON_COLUMNS = [
    "event_id",
    "event_ts",
    "created_ts_parsed",
    "session_type",
    "ticker",
    "exchange",
    "source_offset",
]
TRADE_COLUMNS = COMMON_COLUMNS + ["price", "quantity", "side", "trade_id"]
QUOTE_COLUMNS = COMMON_COLUMNS + ["bid_price", "bid_qty", "ask_price", "ask_qty"]


def insert_new_events(spark: SparkSession, frame: DataFrame, path: str) -> None:
    """Insert rows whose event_id is not yet in the Delta table at ``path``."""
    frame = frame.dropDuplicates(["event_id"])
    if not os.path.exists(path):
        frame.write.format("delta").save(path)
        return
    from delta.tables import DeltaTable

    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(frame.alias("s"), "t.event_id = s.event_id")
        .whenNotMatchedInsertAll()
        .execute()
    )


def load_flink_silver(spark: SparkSession, flink_dir: str, silver_dir: str) -> None:
    """Land Flink's committed Silver events and quarantine records as Delta tables."""
    events_path = os.path.join(flink_dir, "stg_events")
    if not os.path.exists(events_path):
        raise FileNotFoundError(
            f"No Flink output at {events_path}; submit jobs/flink/silver_stream.py first"
        )
    # Flink keeps in-progress part files hidden (".part-*"), which Spark skips.
    events = (
        spark.read.schema(FLINK_EVENT_SCHEMA)
        .json(events_path)
        .withColumn("event_ts", F.to_timestamp("event_timestamp"))
        .withColumn("created_ts_parsed", F.to_timestamp("created_ts"))
        .withColumnRenamed("kafka_offset", "source_offset")
    )
    for event_type, columns, table in [
        ("trade", TRADE_COLUMNS, "stg_trades"),
        ("quote", QUOTE_COLUMNS, "stg_quotes"),
    ]:
        rows = events.filter(F.col("event_type") == event_type).select(*columns)
        insert_new_events(spark, rows, os.path.join(silver_dir, table))
        logger.info("  %s: merged from %s", table, events_path)

    quarantine_path = os.path.join(flink_dir, "stg_events_quarantine")
    # Flink creates this directory only after the first late event.
    if os.path.exists(quarantine_path):
        quarantine = (
            spark.read.schema(QUARANTINE_SCHEMA)
            .json(quarantine_path)
            .withColumn("event_ts", F.to_timestamp("event_ts"))
        )
        insert_new_events(spark, quarantine, os.path.join(silver_dir, "stg_events_quarantine"))
