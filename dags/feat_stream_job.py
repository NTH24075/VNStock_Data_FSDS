"""Airflow DP3 DAG: compute intraday event-time feature windows."""

# =============================================================================
# Airflow DAG: Feature Stream Job (DP2 — streaming path)
# =============================================================================
# Continuous: Spark Structured Streaming reading stg_trades,
# computing rolling window features (5-min volume, 30-min trade count),
# writing feat_stream_intraday.
#
# Per 02_schema_design.md Section 6 + Section 7.6:
#   - Feature freshness: <= 5 minutes during trading hours
#   - Tumbling 5-min + sliding 30-min/1-min windows
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
    dag_id="feat_stream_job",
    default_args=default_args,
    start_date=datetime(2025, 7, 1),
    schedule_interval="@once",
    catchup=False,
    tags=["features", "streaming", "dp2"],
) as dag:
    SILVER_DIR = os.getenv("SILVER_DIR", "data/silver")
    GOLD_DIR = os.getenv("GOLD_DIR", "data/gold")

    def run_feat(**context):
        """Start the stream-feature query and wait for termination."""
        from jobs.features.stream import run_feat_stream

        conf = getattr(context.get("dag_run"), "conf", {}) or {}
        spark, query = run_feat_stream(
            silver_dir=SILVER_DIR,
            gold_dir=GOLD_DIR,
            checkpoint_dir=conf.get(
                "checkpoint_dir",
                os.path.join(GOLD_DIR, "_checkpoints", "feat_stream"),
            ),
            available_now=bool(conf.get("available_now", False)),
        )
        if query is None:
            return
        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            pass
        finally:
            spark.stop()

    t_feat = PythonOperator(
        task_id="compute_stream_features",
        python_callable=run_feat,
    )
