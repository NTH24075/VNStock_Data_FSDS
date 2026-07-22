# =============================================================================
# Airflow DAG: Feature Pipeline — Daily + Labels + Drift Monitoring (DP3)
# =============================================================================
# Schedule: daily, after gold_dimensions_and_facts completes.
# Trading-day window features, drift PSI monitoring, label table, training table.
#
# Pipeline: Ingest → Validate
# Based on schema_design.md §Feature Tables + generator.md §Drift.
# =============================================================================

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="feat_daily_job",
    default_args=default_args,
    start_date=datetime(2025, 7, 1),
    schedule_interval="0 17 * * *",
    catchup=False,
    tags=["features", "gold", "drift", "dp3"],
) as dag:

    GOLD_DIR = os.getenv("GOLD_DIR", "data/gold")

    def compute_daily_features(**context):
        from jobs.gold_model import build_feat_ticker_daily, get_spark

        spark = get_spark("feat_daily")
        try:
            build_feat_ticker_daily(spark, GOLD_DIR)
        finally:
            spark.stop()

    def compute_unified_features(**context):
        from jobs.gold_model import build_feat_ticker_unified, get_spark

        spark = get_spark("feat_unified")
        try:
            build_feat_ticker_unified(spark, GOLD_DIR)
        finally:
            spark.stop()

    def compute_drift_monitoring(**context):
        from jobs.drift_monitor import build_agg_feature_health, get_spark

        spark = get_spark("drift_monitor")
        try:
            build_agg_feature_health(spark, GOLD_DIR)
        finally:
            spark.stop()

    def compute_labels(**context):
        from jobs.gold_model import build_ml_ticker_label, get_spark

        spark = get_spark("labels")
        try:
            build_ml_ticker_label(spark, GOLD_DIR)
        finally:
            spark.stop()

    def build_training_table(**context):
        from jobs.drift_monitor import build_ml_ticker_training, get_spark

        spark = get_spark("training")
        try:
            build_ml_ticker_training(spark, GOLD_DIR)
        finally:
            spark.stop()

    # === Validate stage ===

    def validate_feature_timestamps(**context):
        from pyspark.sql import functions as F

        from jobs.gold_model import get_spark

        spark = get_spark("feat_val_ts")
        try:
            path = os.path.join(GOLD_DIR, "feat_ticker_daily")
            if not os.path.exists(path):
                return
            df = spark.read.format("delta").load(path)
            assert "event_timestamp" in df.columns, "Missing event_timestamp column"
            assert "created_ts" in df.columns, "Missing created_ts column"
            null_ets = df.filter(F.col("event_timestamp").isNull()).count()
            null_cts = df.filter(F.col("created_ts").isNull()).count()
            if null_ets > 0 or null_cts > 0:
                raise ValueError(f"feat_ticker_daily: {null_ets} null event_timestamp, {null_cts} null created_ts")
            print(f"  feat_ticker_daily: {df.count()} rows, timestamps OK")
        finally:
            spark.stop()

    def validate_label_leakage(**context):
        from pyspark.sql import functions as F

        from jobs.gold_model import get_spark

        spark = get_spark("feat_val_label")
        try:
            path = os.path.join(GOLD_DIR, "ml_ticker_label")
            if not os.path.exists(path):
                return
            df = spark.read.format("delta").load(path)
            total = df.count()
            pos = df.filter(F.col("label") == 1).count()
            neg = total - pos
            print(f"  ml_ticker_label: {total} rows, label distribution: 0={neg}, 1={pos}")
        finally:
            spark.stop()

    def validate_drift_alerts(**context):
        from jobs.gold_model import get_spark

        spark = get_spark("feat_val_drift")
        try:
            path = os.path.join(GOLD_DIR, "agg_feature_health_daily")
            if not os.path.exists(path):
                return
            df = spark.read.format("delta").load(path)
            alerts = df.filter("alert_flag = true")
            n_alerts = alerts.count()
            n_total = df.count()
            print(f"  Drift alerts: {n_alerts} / {n_total} rows")
            for row in alerts.collect():
                print(f"    {row.monitoring_date} | {row.feature_name} | PSI={row.psi_vs_baseline}")
        finally:
            spark.stop()

    # Ingest
    t_feat = PythonOperator(task_id="compute_daily_features", python_callable=compute_daily_features)
    t_feat_unified = PythonOperator(task_id="compute_unified_features", python_callable=compute_unified_features)
    t_drift = PythonOperator(task_id="compute_drift_monitoring", python_callable=compute_drift_monitoring)
    t_labels = PythonOperator(task_id="compute_labels", python_callable=compute_labels)
    t_train = PythonOperator(task_id="build_training_table", python_callable=build_training_table)

    # Validate
    t_val_ts = PythonOperator(task_id="validate_feature_timestamps", python_callable=validate_feature_timestamps)
    t_val_leak = PythonOperator(task_id="validate_label_leakage", python_callable=validate_label_leakage)
    t_val_drift = PythonOperator(task_id="validate_drift_alerts", python_callable=validate_drift_alerts)

    t_feat >> t_feat_unified >> t_drift
    t_feat >> t_labels >> t_train
    t_feat >> t_val_ts
    t_labels >> t_val_leak
    t_drift >> t_val_drift
