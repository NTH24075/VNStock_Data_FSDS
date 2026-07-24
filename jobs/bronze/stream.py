"""Bronze streaming ingestion — configured Kafka topic -> Delta raw_market_events.

Continuous append-only with Kafka offset metadata, per 02_schema_design.md Section 7.
"""

import logging
import os

from pyspark.sql import functions as F
from pyspark.sql.types import LongType

from jobs.spark_session import get_spark

logger = logging.getLogger(__name__)


def run_stream_ingest(
    kafka_broker: str = "kafka:29092",
    topic: str = "stock_market_events_v3",
    bronze_dir: str = "data/bronze",
    checkpoint_dir: str = "data/bronze/_checkpoints/stream",
    starting_offsets: str = "latest",
    available_now: bool = False,
):
    """Start checkpointed Kafka ingestion into append-only Bronze Delta."""
    spark = get_spark(
        "bronze_stream",
        {
            "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
        },
    )

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_broker)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = raw.select(
        F.col("value").cast("string").alias("payload_json"),
        F.col("offset").cast(LongType()).alias("source_offset"),
        F.col("partition").cast(LongType()).alias("source_partition"),
        F.col("topic").alias("source_topic"),
        F.col("timestamp").alias("kafka_ts"),
        F.col("key").cast("string").alias("kafka_key"),
        F.current_timestamp().alias("_ingested_at"),
    )

    out_path = os.path.join(bronze_dir, "raw_market_events")
    ckpt = checkpoint_dir

    logger.info("Starting bronze stream: kafka=%s/%s -> delta=%s", kafka_broker, topic, out_path)
    logger.info("Checkpoint dir: %s", ckpt)

    writer = (
        parsed.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", ckpt)
    )
    writer = (
        writer.trigger(availableNow=True)
        if available_now
        else writer.trigger(processingTime="30 seconds")
    )
    try:
        query = writer.start(out_path)
    except Exception:
        spark.stop()
        raise

    return spark, query
