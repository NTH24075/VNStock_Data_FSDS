# =============================================================================
# Airflow DAG: Silver Stream (DP2 — streaming path)
# =============================================================================
# Continuous: Spark Structured Streaming reading raw_market_events Delta,
# parsing JSON, dedup by event_id, watermark=60s, splitting into
# stg_trades + stg_quotes.
#
# Per 02_schema_design.md Section 7:
#   - Silver stream freshness: <= 5 minutes
#   - Late events beyond watermark flagged (_late_flag)
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
        from jobs.silver.stream import run_silver_stream

        spark, queries = run_silver_stream(
            bronze_dir=BRONZE_DIR,
            silver_dir=SILVER_DIR,
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
