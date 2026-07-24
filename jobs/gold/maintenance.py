"""Delta Lake maintenance — OPTIMIZE compaction + Z-order on Gold tables.

Weekly job per 02_schema_design.md Section 8.1.
Reduces small-file problem from daily incremental writes.
"""

import logging
import os

from pyspark.sql import SparkSession

from jobs.spark_session import get_spark

logger = logging.getLogger(__name__)


def optimize_table(spark: SparkSession, table_path: str, zorder_cols: list[str] = None):
    """Compact a Delta table and optionally Z-order selected columns."""
    if not os.path.exists(table_path):
        logger.info("  Skipping (not found): %s", table_path)
        return

    tbl = spark.read.format("delta").load(table_path)
    row_count = tbl.count()
    file_count = tbl.inputFiles().__len__() if hasattr(tbl, "inputFiles") else "?"
    logger.info("  %s: %d rows, %s files", table_path, row_count, file_count)

    delta_table = spark._jvm.io.delta.tables.DeltaTable.forPath(spark._jsparkSession, table_path)
    builder = delta_table.optimize()
    builder = builder.executeCompaction()

    if zorder_cols:
        delta_table.optimize().executeZOrderBy(*zorder_cols)

    logger.info("  OPTIMIZE + Z-order(%s) done -> %s", zorder_cols or "none", table_path)


def run_maintenance(gold_dir: str = "data/gold"):
    """Run weekly maintenance across the high-value Gold tables."""
    spark = get_spark(
        "delta_maintenance",
        {
            "spark.databricks.delta.retentionDurationCheck.enabled": "false",
        },
    )

    tables = [
        ("fact_daily_price", ["ticker_id"]),
        ("obt_ticker_daily_performance", ["ticker_id", "trade_date"]),
        ("feat_ticker_daily", ["ticker_id"]),
        ("feat_ticker_unified", ["ticker_id"]),
        ("ml_ticker_training", ["ticker_id"]),
        ("dim_ticker", ["ticker_id"]),
    ]

    logger.info("=== Delta Maintenance: OPTIMIZE + Z-order ===")
    for table_name, zorder_cols in tables:
        path = os.path.join(gold_dir, table_name)
        optimize_table(spark, path, zorder_cols)

    spark.stop()
    logger.info("Delta maintenance complete.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    run_maintenance()
