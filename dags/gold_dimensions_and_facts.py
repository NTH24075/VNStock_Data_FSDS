# =============================================================================
# Airflow DAG: Gold Dimensions + Facts + OBT (DP2 — part 2)
# =============================================================================
# Schedule: daily, after silver_daily completes.
# SCD2 merge for dim_ticker, adj_close computation, fact/OBT incremental merge.
#
# Pipeline: Ingest (from Silver) → Validate
# Based on schema_design.md, pipelines 2-3.
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
    dag_id="gold_dimensions_and_facts",
    default_args=default_args,
    start_date=datetime(2025, 7, 1),
    schedule_interval="30 16 * * *",
    catchup=False,
    tags=["gold", "dimensions", "facts", "obt", "dp2"],
) as dag:

    SILVER_DIR = os.getenv("SILVER_DIR", "data/silver")
    GOLD_DIR = os.getenv("GOLD_DIR", "data/gold")

    def build_dim_ticker_scd2(**context):
        from jobs.gold_model import build_dim_ticker, get_spark, read_silver_daily

        spark = get_spark("gold_dim_ticker")
        try:
            df = read_silver_daily(spark, SILVER_DIR)
            build_dim_ticker(spark, df, GOLD_DIR)
        finally:
            spark.stop()

    def seed_static_dims(**context):
        from jobs.gold_model import (
            build_dim_date, build_dim_exchange, build_dim_industry,
            build_dim_session, get_spark, read_silver_daily,
        )

        spark = get_spark("gold_static_dims")
        try:
            print("\n=== Gold: Static Dimensions ===")
            df = read_silver_daily(spark, SILVER_DIR)
            build_dim_date(spark, df, GOLD_DIR)
            build_dim_industry(spark, GOLD_DIR)
            build_dim_exchange(spark, GOLD_DIR)
            build_dim_session(spark, GOLD_DIR)
        finally:
            spark.stop()

    def build_fact_daily_price(**context):
        from jobs.gold_model import build_fact_daily_price, get_spark, read_silver_daily

        spark = get_spark("gold_fact_price")
        try:
            df = read_silver_daily(spark, SILVER_DIR)
            build_fact_daily_price(spark, df, GOLD_DIR)
        finally:
            spark.stop()

    def build_fact_foreign_flow(**context):
        from jobs.gold_model import get_spark

        spark = get_spark("gold_fact_ff")
        try:
            path = os.path.join(SILVER_DIR, "stg_foreign_flow")
            if not os.path.exists(path):
                print("  No stg_foreign_flow — skipping fact_foreign_flow")
                return
            print("\n=== Gold: fact_foreign_flow ===")
            df = spark.read.format("delta").load(path)
            out_path = os.path.join(GOLD_DIR, "fact_foreign_flow")
            df.write.format("delta").mode("overwrite").save(out_path)
            print(f"  fact_foreign_flow: {df.count()} rows -> {out_path}")
        finally:
            spark.stop()

    def build_fact_intraday(**context):
        from jobs.gold_model import build_fact_intraday_trade, get_spark

        spark = get_spark("gold_fact_intraday")
        try:
            build_fact_intraday_trade(spark, GOLD_DIR)
        finally:
            spark.stop()

    def build_obt(**context):
        from jobs.gold_model import build_obt, get_spark

        spark = get_spark("gold_obt")
        try:
            build_obt(spark, GOLD_DIR)
        finally:
            spark.stop()

    # === Validate stage ===

    def validate_referential(**context):
        from pyspark.sql import functions as F

        from jobs.gold_model import get_spark

        spark = get_spark("gold_val_ref")
        try:
            for table in ["dim_ticker", "dim_date", "dim_industry", "dim_exchange",
                          "dim_session", "fact_daily_price"]:
                path = os.path.join(GOLD_DIR, table)
                if not os.path.exists(path):
                    print(f"  {table}: missing — skipping")
                    continue
                df = spark.read.format("delta").load(path)
                print(f"  {table}: {df.count()} rows")

            # Cross-consistency: every fact row has ticker_id in dim_ticker
            fp = os.path.join(GOLD_DIR, "fact_daily_price")
            dt = os.path.join(GOLD_DIR, "dim_ticker")
            if os.path.exists(fp) and os.path.exists(dt):
                fact = spark.read.format("delta").load(fp)
                dim = spark.read.format("delta").load(dt)
                fact_tickers = {r.ticker_id for r in fact.select("ticker_id").distinct().collect()}
                dim_tickers = {r.ticker_id for r in dim.select("ticker_id").distinct().collect()}
                missing = fact_tickers - dim_tickers
                if missing:
                    raise ValueError(f"fact_daily_price references {len(missing)} tickers not in dim_ticker: {sorted(missing)[:10]}")
                print(f"  referential integrity OK: {len(fact_tickers)} tickers in fact, {len(dim_tickers)} in dim")
        finally:
            spark.stop()

    def validate_scd2(**context):
        from pyspark.sql import functions as F

        from jobs.gold_model import get_spark

        spark = get_spark("gold_val_scd2")
        try:
            path = os.path.join(GOLD_DIR, "dim_ticker")
            if not os.path.exists(path):
                return
            df = spark.read.format("delta").load(path)
            # At most one is_current=True per ticker_id
            current = df.filter(F.col("is_current"))
            dupes = current.groupBy("ticker_id").count().filter(F.col("count") > 1).count()
            if dupes > 0:
                raise ValueError(f"SCD2 violation: {dupes} tickers with >1 is_current=True rows")
            total_current = current.count()
            n_tickers = df.select("ticker_id").distinct().count()
            print(f"  SCD2 OK: {total_current} current rows for {n_tickers} tickers")
        finally:
            spark.stop()

    def write_run_metadata(**context):
        from jobs.gold_model import get_spark, write_pipeline_run_metadata

        spark = get_spark("gold_ops")
        try:
            write_pipeline_run_metadata(spark, GOLD_DIR, "gold_dimensions_and_facts", "completed")
        finally:
            spark.stop()

    t_dim = PythonOperator(task_id="build_dim_ticker_scd2", python_callable=build_dim_ticker_scd2)
    t_static = PythonOperator(task_id="seed_static_dims", python_callable=seed_static_dims)
    t_fp = PythonOperator(task_id="build_fact_daily_price", python_callable=build_fact_daily_price)
    t_ff = PythonOperator(task_id="build_fact_foreign_flow", python_callable=build_fact_foreign_flow)
    t_fi = PythonOperator(task_id="build_fact_intraday", python_callable=build_fact_intraday)
    t_obt = PythonOperator(task_id="build_obt", python_callable=build_obt)
    t_val_ref = PythonOperator(task_id="validate_referential", python_callable=validate_referential)
    t_val_scd2 = PythonOperator(task_id="validate_scd2", python_callable=validate_scd2)
    t_ops = PythonOperator(task_id="write_run_metadata", python_callable=write_run_metadata)

    [t_dim, t_static] >> [t_fp, t_ff, t_fi] >> t_obt >> t_val_ref >> t_val_scd2 >> t_ops
