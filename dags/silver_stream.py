"""Airflow DP2 DAG: run the Spark Silver streaming fallback."""

# =============================================================================
# Airflow DAG: Silver Stream (DP2 — streaming path, Spark Structured Streaming)
# =============================================================================
# Continuous: Spark Structured Streaming reading raw_market_events Delta,
# parsing JSON, dedup by event_id, watermark=60s, splitting into
# stg_trades + stg_quotes + stg_events_quarantine.
#
# For the Flink version (PyFlink Table API), see dags/flink_silver_stream.py.
# Both engines are available; Spark is used for the primary batch-stream
# integration (Delta-native), Flink demonstrates streaming engine diversity.
#
# Per 02_schema_design.md Section 7:
#   - Silver stream freshness: <= 5 minutes
#   - Late events beyond watermark → stg_events_quarantine
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
    dag_id="silver_stream",
    default_args=default_args,
    start_date=datetime(2025, 7, 1),
    schedule_interval="@once",
    catchup=False,
    tags=["silver", "streaming", "dp2"],
) as dag:
    BRONZE_DIR = os.getenv("BRONZE_DIR", "data/bronze")
    SILVER_DIR = os.getenv("SILVER_DIR", "data/silver")

    def run_silver(**context):
        """Start Silver stream queries and wait for their termination."""
        from jobs.silver.stream import run_silver_stream

        conf = getattr(context.get("dag_run"), "conf", {}) or {}
        spark, queries = run_silver_stream(
            bronze_dir=BRONZE_DIR,
            silver_dir=SILVER_DIR,
            checkpoint_dir=conf.get(
                "checkpoint_dir",
                os.path.join(SILVER_DIR, "_checkpoints", "stream"),
            ),
            available_now=bool(conf.get("available_now", False)),
        )
        if queries is None:
            return
        try:
            for q in queries:
                q.awaitTermination()
        except KeyboardInterrupt:
            pass
        finally:
            spark.stop()

    t_silver = PythonOperator(
        task_id="silver_stream_transform",
        python_callable=run_silver,
    )
