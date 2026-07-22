"""Gold metadata — ops_pipeline_run."""

import os
from datetime import datetime as dt

from pyspark.sql import SparkSession


def get_spark(app_name: str = "gold_metadata") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def write_pipeline_run_metadata(spark: SparkSession, gold_dir: str, pipeline: str, status: str,
                                input_rows: int = None, output_rows: int = None,
                                error_summary: str = None):
    run_id = f"{pipeline}_{dt.now().strftime('%Y%m%d_%H%M%S')}"
    row = [(run_id, pipeline, dt.now().isoformat(), dt.now().isoformat(),
            status, input_rows, output_rows, error_summary)]
    schema = "run_id STRING, pipeline STRING, start_time STRING, end_time STRING, status STRING, input_rows BIGINT, output_rows BIGINT, error_summary STRING"
    meta = spark.createDataFrame(row, schema=schema)
    out = os.path.join(gold_dir, "ops_pipeline_run")
    meta.write.format("delta").mode("append").save(out)
    print(f"  ops_pipeline_run: {pipeline} -> {status} (run_id={run_id})")
