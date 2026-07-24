"""Airflow maintenance DAG: compact and cluster weekly Delta tables."""

# =============================================================================
# Airflow DAG: Delta Maintenance (weekly compaction + Z-order)
# =============================================================================
# Schedule: weekly Sunday 03:00.
# Runs OPTIMIZE compaction + Z-order on Gold tables to reduce small files
# from daily incremental writes. Per 02_schema_design.md Section 8.1.
# =============================================================================

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="delta_maintenance",
    default_args=default_args,
    start_date=datetime(2025, 7, 1),
    schedule_interval="0 3 * * 0",
    catchup=False,
    tags=["maintenance", "delta", "gold"],
) as dag:
    GOLD_DIR = os.getenv("GOLD_DIR", "data/gold")

    def run_optimize(**context):
        """Invoke the shared Delta maintenance job."""
        from jobs.gold.maintenance import run_maintenance

        run_maintenance(gold_dir=GOLD_DIR)

    t_optimize = PythonOperator(
        task_id="optimize_gold_tables",
        python_callable=run_optimize,
    )
