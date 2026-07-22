# =============================================================================
# Airflow DAG: Bronze Offline Ingestion (DP1)
# =============================================================================
# Schedule: daily at 15:30 (after simulated market close).
# Pulls generator Parquet drops from landing-vendor-offline/ (file-drop pattern)
# and tickers/corporate_actions from vendor_db via JDBC (DB-extract pattern)
# into Delta tables on MinIO.
#
# Pipeline: Ingest → Validate
# Based on schema_design.md, pipeline 1.
# =============================================================================

import glob
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
    dag_id="bronze_offline_ingest",
    default_args=default_args,
    start_date=datetime(2025, 7, 1),
    schedule_interval="30 15 * * *",
    catchup=False,
    tags=["bronze", "offline", "dp1"],
) as dag:

    DATA_DIR = os.getenv("DATA_DIR", "data/landing")
    BRONZE_DIR = os.getenv("BRONZE_DIR", "data/bronze")

    def _read_landing(spark, table_name):
        pattern = os.path.join(DATA_DIR, "run_date=*", table_name, "*.parquet")
        files = glob.glob(pattern)
        if not files:
            raise FileNotFoundError(f"No Parquet files found: {pattern}")
        return spark.read.parquet(*files)

    # === Ingest stage ===

    def ingest_ohlcv(**context):
        from pyspark.sql import functions as F

        from jobs.bronze_ingest import add_ingest_metadata, get_spark, load_contract, validate_contract

        spark = get_spark("bronze_ohlcv")
        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        try:
            print("\n=== Bronze: raw_ohlcv_daily ===")
            df = _read_landing(spark, "ohlcv_daily")
            print(f"  Read {df.count()} rows, columns: {df.columns}")
            for v in [1, 2]:
                contract = load_contract(v)
                df_v = df.filter(F.col("_schema_version") == v)
                cnt = df_v.count()
                if cnt == 0:
                    continue
                validate_contract(df_v, contract, v)
                df_v = add_ingest_metadata(df_v, batch_id, v)
                out_path = os.path.join(BRONZE_DIR, f"raw_ohlcv_daily_v{v}")
                df_v.write.format("delta").mode("overwrite").save(out_path)
                print(f"  Wrote v{v}: {cnt} rows -> {out_path}")
        finally:
            spark.stop()

    def ingest_foreign_flow(**context):
        from jobs.bronze_ingest import add_ingest_metadata, get_spark

        spark = get_spark("bronze_ff")
        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        try:
            print("\n=== Bronze: raw_foreign_flow ===")
            df = _read_landing(spark, "foreign_flow_daily")
            print(f"  Read {df.count()} rows, columns: {df.columns}")
            df = add_ingest_metadata(df, batch_id, 1)
            out_path = os.path.join(BRONZE_DIR, "raw_foreign_flow")
            df.write.format("delta").mode("overwrite").save(out_path)
            print(f"  Wrote {df.count()} rows -> {out_path}")
        except FileNotFoundError as e:
            print(f"  Skipping foreign_flow_daily: {e}")
        finally:
            spark.stop()

    def ingest_corporate_actions(**context):
        from jobs.bronze_ingest import add_ingest_metadata, get_spark

        spark = get_spark("bronze_ca")
        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        try:
            print("\n=== Bronze: raw_corporate_actions ===")
            df = _read_landing(spark, "corporate_actions")
            print(f"  Read {df.count()} rows, columns: {df.columns}")
            df = add_ingest_metadata(df, batch_id, 1)
            out_path = os.path.join(BRONZE_DIR, "raw_corporate_actions")
            df.write.format("delta").mode("overwrite").save(out_path)
            print(f"  Wrote {df.count()} rows -> {out_path}")
        except FileNotFoundError as e:
            print(f"  Skipping corporate_actions: {e}")
        finally:
            spark.stop()

    def ingest_financial_ratios(**context):
        from jobs.bronze_ingest import add_ingest_metadata, get_spark

        spark = get_spark("bronze_fr")
        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        try:
            print("\n=== Bronze: raw_financial_ratios ===")
            df = _read_landing(spark, "financial_ratios")
            print(f"  Read {df.count()} rows, columns: {df.columns}")
            df = add_ingest_metadata(df, batch_id, 1)
            out_path = os.path.join(BRONZE_DIR, "raw_financial_ratios")
            df.write.format("delta").mode("overwrite").save(out_path)
            print(f"  Wrote {df.count()} rows -> {out_path}")
        except FileNotFoundError as e:
            print(f"  Skipping financial_ratios: {e}")
        finally:
            spark.stop()

    # === Validate stage ===

    def validate_contract(**context):
        import json
        from pathlib import Path

        from jobs.bronze_ingest import get_spark, validate_contract as _validate

        CONTRACTS_DIR = Path("/opt/airflow/contracts")
        spark = get_spark("bronze_validate_contract")
        try:
            for table_base, versions in [("raw_ohlcv_daily", [1, 2]),
                                          ("raw_foreign_flow", [1]),
                                          ("raw_corporate_actions", [1]),
                                          ("raw_financial_ratios", [1])]:
                for v in versions:
                    path = os.path.join(BRONZE_DIR, f"{table_base}_v{v}" if table_base == "raw_ohlcv_daily" else table_base)
                    contract_path = CONTRACTS_DIR / f"{table_base}.v{v}.json"
                    if not os.path.exists(path):
                        continue
                    df = spark.read.format("delta").load(path)
                    if contract_path.exists():
                        with open(contract_path) as f:
                            _validate(df, json.load(f), v)
                    print(f"  {table_base} v{v}: {df.count()} rows OK")
        finally:
            spark.stop()

    def validate_quality(**context):
        from jobs.bronze_ingest import get_spark

        spark = get_spark("bronze_validate_quality")
        try:
            print("=== Quality Checks ===")
            for table_base, versions in [("raw_ohlcv_daily", [1, 2]),
                                          ("raw_foreign_flow", [1]),
                                          ("raw_corporate_actions", [1]),
                                          ("raw_financial_ratios", [1])]:
                for v in versions:
                    path = os.path.join(BRONZE_DIR, f"{table_base}_v{v}" if table_base == "raw_ohlcv_daily" else table_base)
                    if not os.path.exists(path):
                        continue
                    df = spark.read.format("delta").load(path)
                    cnt = df.count()
                    print(f"  {table_base} v{v}: {cnt} rows")
                    for col in ["ticker_id", "trade_date"]:
                        if col in df.columns:
                            nulls = df.filter(df[col].isNull()).count()
                            if nulls > 0:
                                raise ValueError(f"{col} has {nulls} nulls in {table_base}")
        finally:
            spark.stop()

    # Ingest tasks
    t_ingest_ohlcv = PythonOperator(task_id="ingest_ohlcv", python_callable=ingest_ohlcv)
    t_ingest_ff = PythonOperator(task_id="ingest_foreign_flow", python_callable=ingest_foreign_flow)
    t_ingest_ca = PythonOperator(task_id="ingest_corporate_actions", python_callable=ingest_corporate_actions)
    t_ingest_fr = PythonOperator(task_id="ingest_financial_ratios", python_callable=ingest_financial_ratios)

    # Validate tasks
    t_validate_contract = PythonOperator(task_id="validate_contract", python_callable=validate_contract)
    t_validate_quality = PythonOperator(task_id="validate_quality", python_callable=validate_quality)

    # DAG structure: Ingest → Validate
    [t_ingest_ohlcv, t_ingest_ff, t_ingest_ca, t_ingest_fr] >> t_validate_contract >> t_validate_quality
