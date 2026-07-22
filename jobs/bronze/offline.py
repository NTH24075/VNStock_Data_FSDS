"""Bronze ingestion — landing Parquet → Bronze Delta tables."""

import argparse
import json
import os
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

CONTRACTS_DIR = Path(os.getenv("CONTRACTS_DIR", str(Path(__file__).parent.parent.parent / "contracts")))


def get_spark(app_name: str = "bronze_ingest") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
        .getOrCreate()
    )


def load_contract(version: int) -> dict:
    with open(CONTRACTS_DIR / f"raw_ohlcv_daily.v{version}.json") as f:
        return json.load(f)


def read_landing_parquet(spark: SparkSession, data_dir: str, table: str) -> DataFrame:
    pattern = os.path.join(data_dir, "run_date=*", table, "*.parquet")
    import glob
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"No Parquet files found: {pattern}")
    return spark.read.parquet(*files)


def validate_contract(df: DataFrame, contract: dict, version: int):
    required = set(contract["required"])
    actual = set(df.columns)
    missing = required - actual
    if missing:
        raise ValueError(f"Contract v{version} violation: missing required columns {missing}")


def add_ingest_metadata(df: DataFrame, batch_id: str, schema_version: int, source_path: str = None) -> DataFrame:
    result = (
        df.withColumn("_ingested_at", F.current_timestamp().cast(StringType()))
          .withColumn("_batch_id", F.lit(batch_id))
          .withColumn("_schema_version", F.lit(schema_version))
    )
    if source_path:
        result = result.withColumn("_source_path", F.lit(source_path))
    return result


def main():
    parser = argparse.ArgumentParser(description="Bronze ingestion")
    parser.add_argument("--data-dir", default="data/landing")
    parser.add_argument("--bronze-dir", default="data/bronze")
    args = parser.parse_args()

    spark = get_spark()

    from datetime import datetime
    batch_id_str = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("\n=== Bronze: raw_ohlcv_daily ===")
    try:
        df = read_landing_parquet(spark, args.data_dir, "ohlcv_daily")
        print(f"  Read {df.count()} rows, columns: {df.columns}")

        for v in [1, 2]:
            contract = load_contract(v)
            df_v = df.filter(F.col("_schema_version") == v)
            cnt = df_v.count()
            if cnt == 0:
                continue
            validate_contract(df_v, contract, v)
            df_v = add_ingest_metadata(df_v, batch_id_str, v)
            out_path = os.path.join(args.bronze_dir, f"raw_ohlcv_daily_v{v}")
            df_v.write.format("delta").mode("overwrite").save(out_path)
            print(f"  Wrote v{v}: {cnt} rows -> {out_path}")
    except FileNotFoundError as e:
        print(f"  Skipping ohlcv_daily: {e}")

    for table in ["foreign_flow_daily", "corporate_actions", "financial_ratios"]:
        delta_name = f"raw_{table}"
        if table == "foreign_flow_daily":
            delta_name = "raw_foreign_flow"
        print(f"\n=== Bronze: {delta_name} ===")
        try:
            df = read_landing_parquet(spark, args.data_dir, table)
            print(f"  Read {df.count()} rows, columns: {df.columns}")
            df = add_ingest_metadata(df, batch_id_str, 1)
            out_path = os.path.join(args.bronze_dir, delta_name)
            df.write.format("delta").mode("overwrite").save(out_path)
            print(f"  Wrote {df.count()} rows -> {out_path}")
        except FileNotFoundError as e:
            print(f"  Skipping: {e}")

    spark.stop()
    print("\nBronze ingestion complete.")


if __name__ == "__main__":
    main()
