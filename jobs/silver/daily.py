"""Silver transformations — Bronze Delta → Silver Delta."""

import argparse
import logging
import os

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from jobs.spark_session import get_spark

logger = logging.getLogger(__name__)


def read_bronze_ohlcv(spark: SparkSession, bronze_dir: str) -> DataFrame:
    """Union contract v1/v2 Bronze tables into one harmonized DataFrame."""
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
    """Keep the latest ingested row per ticker and trading date."""
    before = df.count()
    window = Window.partitionBy("ticker_id", "trade_date").orderBy(F.desc("_ingested_at"))
    result = df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")
    after = result.count()
    removed = before - after
    pct = removed / max(before, 1) * 100
    logger.info("  Dedup: %d -> %d rows (%d removed, %.1f%%)", before, after, removed, pct)
    return result


def validate_domain(df: DataFrame) -> DataFrame:
    """Attach row-level market-domain quality flags without hiding failures."""
    result = (
        df.withColumn("_dq_price_positive", F.col("close") > 0)
        .withColumn("_dq_high_ge_max", F.col("high") >= F.greatest("open", "close"))
        .withColumn("_dq_low_le_min", F.col("low") <= F.least("open", "close"))
        .withColumn("_dq_volume_nonneg", F.col("volume") >= 0)
    )
    bad = result.filter(
        ~(
            F.col("_dq_price_positive")
            & F.col("_dq_volume_nonneg")
            & F.col("_dq_high_ge_max")
            & F.col("_dq_low_le_min")
        )
    ).count()
    logger.info("  Domain check failures: %d rows (flagged, not dropped)", bad)
    return result


def main():
    """Run the command-line Bronze-to-Silver batch transformation."""
    parser = argparse.ArgumentParser(description="Silver transformation")
    parser.add_argument("--bronze-dir", default="data/bronze")
    parser.add_argument("--silver-dir", default="data/silver")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    spark = get_spark()

    logger.info("=== Silver: stg_daily_price ===")

    df = read_bronze_ohlcv(spark, args.bronze_dir)
    logger.info("  Read %d rows from Bronze (v1+v2 merged)", df.count())

    df = dedup(df)
    df = validate_domain(df)

    out_path = os.path.join(args.silver_dir, "stg_daily_price")
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(out_path)
    )
    logger.info("  Wrote %d rows -> %s", df.count(), out_path)

    spark.stop()
    logger.info("Silver transformation complete.")


if __name__ == "__main__":
    main()
