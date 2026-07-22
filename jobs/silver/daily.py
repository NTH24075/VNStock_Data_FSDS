"""Silver transformations — Bronze Delta → Silver Delta."""

import argparse
import os

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def get_spark(app_name: str = "silver_transform") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def read_bronze_ohlcv(spark: SparkSession, bronze_dir: str) -> DataFrame:
    dfs = []
    for v in [1, 2]:
        path = os.path.join(bronze_dir, f"raw_ohlcv_daily_v{v}")
        if os.path.exists(path):
            dfs.append(spark.read.format("delta").load(path))

    if not dfs:
        raise FileNotFoundError(f"No Bronze Delta at {bronze_dir}/raw_ohlcv_daily_v*")

    df = dfs[0]
    for other in dfs[1:]:
        df = df.unionByName(other, allowMissingColumns=True)

    for col_name in ["value", "foreign_room"]:
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None))
    return df


def dedup(df: DataFrame) -> DataFrame:
    before = df.count()
    window = Window.partitionBy("ticker_id", "trade_date").orderBy(F.desc("_ingested_at"))
    result = df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")
    after = result.count()
    removed = before - after
    pct = removed / max(before, 1) * 100
    print(f"  Dedup: {before} -> {after} rows ({removed} removed, {pct:.1f}%)")
    return result


def validate_domain(df: DataFrame) -> DataFrame:
    result = (
        df.withColumn("_dq_price_positive", F.col("close") > 0)
          .withColumn("_dq_high_ge_max", F.col("high") >= F.greatest("open", "close"))
          .withColumn("_dq_low_le_min", F.col("low") <= F.least("open", "close"))
          .withColumn("_dq_volume_nonneg", F.col("volume") >= 0)
    )
    bad = result.filter(
        ~(F.col("_dq_price_positive")
          & F.col("_dq_volume_nonneg")
          & F.col("_dq_high_ge_max")
          & F.col("_dq_low_le_min"))
    ).count()
    print(f"  Domain check failures: {bad} rows (flagged, not dropped)")
    return result


def main():
    parser = argparse.ArgumentParser(description="Silver transformation")
    parser.add_argument("--bronze-dir", default="data/bronze")
    parser.add_argument("--silver-dir", default="data/silver")
    args = parser.parse_args()

    spark = get_spark()

    print("\n=== Silver: stg_daily_price ===")

    df = read_bronze_ohlcv(spark, args.bronze_dir)
    print(f"  Read {df.count()} rows from Bronze (v1+v2 merged)")

    df = dedup(df)
    df = validate_domain(df)

    out_path = os.path.join(args.silver_dir, "stg_daily_price")
    df.write.format("delta").mode("overwrite").save(out_path)
    print(f"  Wrote {df.count()} rows -> {out_path}")

    spark.stop()
    print("Silver transformation complete.")


if __name__ == "__main__":
    main()
