"""Gold metadata — ops_pipeline_run."""

import logging
import os
from datetime import datetime as dt

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def write_pipeline_run_metadata(
    spark: SparkSession,
    gold_dir: str,
    pipeline: str,
    status: str,
    input_rows: int = None,
    output_rows: int = None,
    error_summary: str = None,
):
    """Append one operational pipeline-run record to the Gold ops table."""
    run_id = f"{pipeline}_{dt.now().strftime('%Y%m%d_%H%M%S')}"
    row = [
        (
            run_id,
            pipeline,
            dt.now().isoformat(),
            dt.now().isoformat(),
            status,
            input_rows,
            output_rows,
            error_summary,
        )
    ]
    schema = "run_id STRING, pipeline STRING, start_time STRING, end_time STRING, status STRING, input_rows BIGINT, output_rows BIGINT, error_summary STRING"
    meta = spark.createDataFrame(row, schema=schema)
    out = os.path.join(gold_dir, "ops_pipeline_run")
    meta.write.format("delta").mode("append").save(out)
    logger.info("  ops_pipeline_run: %s -> %s (run_id=%s)", pipeline, status, run_id)
