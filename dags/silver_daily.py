"""Airflow DP2 DAG: daily deduplication, harmonization, and quality gates."""

# =============================================================================
# Airflow DAG: Silver Daily Transformations (DP2 — part 1)
# =============================================================================
# Schedule: daily, after bronze_offline_ingest completes.
# Dedup, type casting, schema harmonization across the v1/v2 contract boundary.
#
# Pipeline: Ingest (from Bronze) → Validate
# Based on schema_design.md, pipeline 2.
# =============================================================================

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="silver_daily",
    default_args=default_args,
    start_date=datetime(2025, 7, 1),
    schedule_interval="0 16 * * *",
    catchup=False,
    max_active_tasks=1,
    tags=["silver", "daily", "dp2"],
) as dag:
    BRONZE_DIR = os.getenv("BRONZE_DIR", "data/bronze")
    SILVER_DIR = os.getenv("SILVER_DIR", "data/silver")

    def dedup_ohlcv(**context):
        """Harmonize OHLCV versions, deduplicate, and attach DQ flags."""
        from jobs.silver.daily import dedup, read_bronze_ohlcv, validate_domain
        from jobs.spark_session import get_spark

        spark = get_spark("silver_ohlcv")
        try:
            print("\n=== Silver: stg_daily_price ===")
            df = read_bronze_ohlcv(spark, BRONZE_DIR)
            print(f"  Read {df.count()} rows from Bronze (v1+v2 merged)")

            df = dedup(df)
            df = validate_domain(df)

            out_path = os.path.join(SILVER_DIR, "stg_daily_price")
            (
                df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(out_path)
            )
            print(f"  Wrote {df.count()} rows -> {out_path}")
        finally:
            spark.stop()

    def dedup_foreign_flow(**context):
        """Deduplicate the daily foreign-flow source."""
        from jobs.silver.daily import dedup
        from jobs.spark_session import get_spark

        spark = get_spark("silver_ff")
        try:
            path = os.path.join(BRONZE_DIR, "raw_foreign_flow")
            if not os.path.exists(path):
                print("  No raw_foreign_flow — skipping")
                return

            print("\n=== Silver: stg_foreign_flow ===")
            df = spark.read.format("delta").load(path)
            df = dedup(df)
            out_path = os.path.join(SILVER_DIR, "stg_foreign_flow")
            (
                df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(out_path)
            )
            print(f"  Wrote {df.count()} rows -> {out_path}")
        finally:
            spark.stop()

    # === Validate stage ===

    def validate_uniqueness(**context):
        """Fail when a Silver table violates its declared grain."""
        from pyspark.sql import functions as F

        from jobs.spark_session import get_spark

        spark = get_spark("silver_val_unique")
        try:
            for table in ["stg_daily_price", "stg_foreign_flow"]:
                path = os.path.join(SILVER_DIR, table)
                if not os.path.exists(path):
                    continue
                df = spark.read.format("delta").load(path)
                total = df.count()
                dupes = (
                    df.groupBy("ticker_id", "trade_date").count().filter(F.col("count") > 1).count()
                )
                if dupes > 0:
                    raise ValueError(f"{table}: {dupes} duplicate (ticker_id, trade_date) groups")
                print(f"  {table}: uniqueness OK ({total} rows)")
        finally:
            spark.stop()

    def validate_referential(**context):
        """Verify that daily rows do not fall on weekend dates."""
        from pyspark.sql import functions as F

        from jobs.spark_session import get_spark

        spark = get_spark("silver_val_ref")
        try:
            stg_path = os.path.join(SILVER_DIR, "stg_daily_price")
            if not os.path.exists(stg_path):
                return
            df = spark.read.format("delta").load(stg_path)
            dates = [
                r.trade_date
                for r in df.select("trade_date").distinct().orderBy("trade_date").collect()
            ]
            # Basic weekday check: count weekend dates
            weekend_count = (
                spark.createDataFrame([(d,) for d in dates], ["trade_date"])
                .withColumn("dow", F.dayofweek(F.col("trade_date")))
                .filter(F.col("dow").isin([1, 7]))
                .count()
            )
            if weekend_count > 0:
                print(f"  WARNING: {weekend_count} rows on weekends — possible calendar gap")
            print(f"  referential check: {len(dates)} unique dates, {weekend_count} weekend dates")
        finally:
            spark.stop()

    def validate_domain(**context):
        """Report row-level market-domain rule failures."""
        from pyspark.sql import functions as F

        from jobs.silver.daily import validate_domain
        from jobs.spark_session import get_spark

        spark = get_spark("silver_val_domain")
        try:
            path = os.path.join(SILVER_DIR, "stg_daily_price")
            if not os.path.exists(path):
                return
            df = spark.read.format("delta").load(path)
            df = validate_domain(df)
            dq_cols = [c for c in df.columns if c.startswith("_dq_")]
            bad = sum(df.filter(~F.col(c)).count() for c in dq_cols if c in df.columns)
            if bad > 0:
                print(f"  WARNING: {bad} rows with domain check failures")
        finally:
            spark.stop()

    def validate_cardinality(**context):
        """Log approximate and exact cardinality evidence."""
        from pyspark.sql import functions as F

        from jobs.spark_session import get_spark

        spark = get_spark("silver_val_card")
        try:
            path = os.path.join(SILVER_DIR, "stg_daily_price")
            if not os.path.exists(path):
                return
            df = spark.read.format("delta").load(path)
            approx_tickers = df.select(F.approx_count_distinct("ticker_id")).collect()[0][0]
            exact_tickers = df.select(F.countDistinct("ticker_id")).collect()[0][0]
            composite_key = F.concat_ws(
                "|",
                F.col("ticker_id"),
                F.col("trade_date").cast("string"),
            )
            approx_keys = df.select(F.approx_count_distinct(composite_key)).collect()[0][0]
            exact_keys = df.select(F.countDistinct("ticker_id", "trade_date")).collect()[0][0]
            print(
                "  Cardinality: "
                f"approx={approx_tickers}, exact={exact_tickers} tickers; "
                f"approx={approx_keys}, exact={exact_keys} (ticker_id, trade_date) pairs"
            )
        finally:
            spark.stop()

    def validate_duplicates(**context):
        """Report the post-dedup duplicate rate."""
        from pyspark.sql import functions as F

        from jobs.spark_session import get_spark

        spark = get_spark("silver_val_dup")
        try:
            path = os.path.join(SILVER_DIR, "stg_daily_price")
            if not os.path.exists(path):
                return
            df = spark.read.format("delta").load(path)
            total = df.count()
            dupes = df.groupBy("ticker_id", "trade_date").count().filter(F.col("count") > 1).count()
            rate = dupes / max(total, 1) * 100
            print(f"  stg_daily_price: {total} rows, {dupes} dupe groups ({rate:.1f}%)")
        finally:
            spark.stop()

    t_dedup_ohlcv = PythonOperator(task_id="dedup_ohlcv", python_callable=dedup_ohlcv)
    t_dedup_ff = PythonOperator(task_id="dedup_foreign_flow", python_callable=dedup_foreign_flow)
    t_val_unique = PythonOperator(
        task_id="validate_uniqueness", python_callable=validate_uniqueness
    )
    t_val_ref = PythonOperator(task_id="validate_referential", python_callable=validate_referential)
    t_val_domain = PythonOperator(task_id="validate_domain", python_callable=validate_domain)
    t_val_dup = PythonOperator(task_id="validate_duplicates", python_callable=validate_duplicates)
    t_val_card = PythonOperator(
        task_id="validate_cardinality", python_callable=validate_cardinality
    )

    [t_dedup_ohlcv, t_dedup_ff] >> t_val_unique >> [t_val_ref, t_val_domain, t_val_dup, t_val_card]
