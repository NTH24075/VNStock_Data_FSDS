"""Shared Spark session factory — single definition used by all pipeline layers."""

from pyspark.sql import SparkSession


def get_spark(app_name: str = "vnstock_pipeline", extra_configs: dict = None) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
    )
    if extra_configs:
        for key, value in extra_configs.items():
            builder = builder.config(key, value)
    return builder.getOrCreate()
