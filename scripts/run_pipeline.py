"""Run the local Silver and Gold batch path without Airflow."""

import logging
import os

from jobs.gold.dimensions import (
    build_dim_date,
    build_dim_exchange,
    build_dim_industry,
    build_dim_session,
    build_dim_ticker,
    read_silver_daily,
)
from jobs.gold.facts import build_fact_daily_price
from jobs.gold.obt import build_obt
from jobs.silver.daily import dedup, read_bronze_ohlcv, validate_domain
from jobs.spark_session import get_spark

logger = logging.getLogger(__name__)


def run_silver(data_dir: str) -> str:
    """Transform local Bronze OHLCV into the canonical Silver table."""
    bronze_dir = os.path.join(data_dir, "bronze")
    silver_dir = os.path.join(data_dir, "silver")
    spark = get_spark("silver_pipeline")
    try:
        frame = read_bronze_ohlcv(spark, bronze_dir)
        frame = validate_domain(dedup(frame))
        output = os.path.join(silver_dir, "stg_daily_price")
        (
            frame.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(output)
        )
        logger.info("Silver wrote %d rows to %s", frame.count(), output)
        return silver_dir
    finally:
        spark.stop()


def run_gold(data_dir: str, silver_dir: str) -> str:
    """Build local Gold dimensions, daily fact, and OBT."""
    gold_dir = os.path.join(data_dir, "gold")
    spark = get_spark("gold_pipeline")
    try:
        frame = read_silver_daily(spark, silver_dir)
        build_dim_ticker(spark, frame, gold_dir)
        build_dim_date(spark, frame, gold_dir)
        build_dim_industry(spark, gold_dir)
        build_dim_exchange(spark, gold_dir)
        build_dim_session(spark, gold_dir)
        build_fact_daily_price(spark, frame, gold_dir)
        build_obt(spark, gold_dir)
        logger.info("Gold tables written to %s", gold_dir)
        return gold_dir
    finally:
        spark.stop()


def main() -> None:
    """Execute the local batch pipeline using DATA_DIR or ``data``."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    data_dir = os.getenv("DATA_ROOT", "data")
    silver_dir = run_silver(data_dir)
    run_gold(data_dir, silver_dir)


if __name__ == "__main__":
    main()
