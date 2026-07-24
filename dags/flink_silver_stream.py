"""Airflow DP2 DAG: monitor and validate the continuous PyFlink application."""

# =============================================================================
# Airflow DAG: Flink Silver Stream (DP2 — streaming path, Flink engine)
# =============================================================================
# Continuous: submits PyFlink job to Flink cluster for Kafka → dedup →
# watermark → window features → filesystem sink.
#
# Per 02_schema_design.md Section 7 + Section 7.6:
#   - Event-time watermark = 60s
#   - Tumbling 5-min event-time window (the Spark feature job also supplies
#     the 30-min/1-min sliding feature)
#   - Dedup by event_id + latest created_ts
#   - Late events → quarantine sink
#   - Burst handled via parallelism=4 sized for peak (not average)
# =============================================================================

import json
import os
from datetime import datetime, timedelta
from urllib.request import urlopen

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="flink_silver_stream",
    default_args=default_args,
    start_date=datetime(2025, 7, 1),
    schedule_interval="@once",
    catchup=False,
    tags=["flink", "silver", "streaming", "dp2"],
) as dag:
    FLINK_REST_URL = os.getenv("FLINK_REST_URL", "http://flink-jobmanager:8081")

    def _read_json(path: str) -> dict:
        """Read one response from Flink's internal REST endpoint."""
        with urlopen(f"{FLINK_REST_URL}{path}", timeout=10) as response:
            return json.load(response)

    def check_flink_health(**context):
        """Fail early unless the Flink cluster has an available task manager."""
        overview = _read_json("/overview")
        print(f"Flink cluster status: {overview}")
        if overview.get("taskmanagers", 0) < 1:
            raise RuntimeError("Flink has no registered TaskManager.")

    def validate_streaming_job(**context):
        """Require exactly one running Silver job and report its job ID."""
        jobs = _read_json("/jobs/overview").get("jobs", [])
        running = [
            job
            for job in jobs
            if job.get("name") == "flink-silver-stream" and job.get("state") == "RUNNING"
        ]
        if len(running) != 1:
            raise RuntimeError(f"Expected one RUNNING flink-silver-stream job, found {running}")
        print(f"Validated Flink Silver job: {running[0]['jid']}")

    t_health = PythonOperator(task_id="check_flink_health", python_callable=check_flink_health)
    t_validate = PythonOperator(
        task_id="validate_streaming_job",
        python_callable=validate_streaming_job,
    )

    t_health >> t_validate
