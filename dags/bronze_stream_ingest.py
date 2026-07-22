# =============================================================================
# Airflow DAG: Bronze Stream Ingestion (DP2 — streaming path)
# =============================================================================
# Continuous: Spark Structured Streaming from Kafka topic stock_market_events
# into Delta raw_market_events (append-only with Kafka offset metadata).
#
# Per 02_schema_design.md Section 7:
#   - Bronze stream ingest lag: <= 1 minute from Kafka arrival
#   - Append-only with source_offset, source_partition, source_topic
# =============================================================================

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="bronze_stream_ingest",
    default_args=default_args,
    start_date=datetime(2025, 7, 1),
    schedule_interval="@once",
    catchup=False,
    tags=["bronze", "streaming", "dp2"],
) as dag:

    BRONZE_DIR = os.getenv("BRONZE_DIR", "data/bronze")

    def ingest_stream(**context):
        from jobs.bronze.stream import run_stream_ingest

        kafka_broker = os.getenv("KAFKA_BROKER", "kafka:9092")
        spark, query = run_stream_ingest(
            kafka_broker=kafka_broker,
            bronze_dir=BRONZE_DIR,
        )
        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            pass
        finally:
            spark.stop()

    t_ingest = PythonOperator(
        task_id="ingest_kafka_stream",
        python_callable=ingest_stream,
    )
