"""Bronze streaming ingestion — Kafka stock_market_events -> Delta raw_market_events.

Continuous append-only with Kafka offset metadata, per 02_schema_design.md Section 7.
"""

import logging
import os

from pyspark.sql import functions as F
from pyspark.sql.types import LongType, TimestampType

from jobs.spark_session import get_spark

logger = logging.getLogger(__name__)


def run_stream_ingest(
    kafka_broker: str = "kafka:9092",
    topic: str = "stock_market_events",
    bronze_dir: str = "data/bronze",
    checkpoint_dir: str = "data/checkpoints/bronze_stream",
):
    spark = get_spark("bronze_stream", {
        "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    })

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_broker)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = raw.select(
        F.col("value").cast("string").alias("payload_json"),
        F.col("offset").cast(LongType()).alias("source_offset"),
        F.col("partition").cast(LongType()).alias("source_partition"),
        F.col("topic").alias("source_topic"),
        F.from_unixtime(F.col("timestamp").cast(LongType()) / 1000)
         .cast(TimestampType())
         .alias("kafka_ts"),
        F.col("key").cast("string").alias("kafka_key"),
        F.current_timestamp().alias("_ingested_at"),
    )

    out_path = os.path.join(bronze_dir, "raw_market_events")
    ckpt = checkpoint_dir

    logger.info("Starting bronze stream: kafka=%s/%s -> delta=%s", kafka_broker, topic, out_path)
    logger.info("Checkpoint dir: %s", ckpt)

    query = (
        parsed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", ckpt)
        .trigger(processingTime="30 seconds")
        .start(out_path)
    )

    return spark, query
