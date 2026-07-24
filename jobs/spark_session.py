"""Shared Spark session factory — single definition used by all pipeline layers."""

import os

from pyspark.sql import SparkSession


def get_spark(app_name: str = "vnstock_pipeline", extra_configs: dict = None) -> SparkSession:
    """Create a Spark 3.5 session with Delta and coursework optimizations."""
    delta_package = "io.delta:delta-spark_2.12:3.2.0"
    postgres_package = "org.postgresql:postgresql:42.7.3"
    extra_configs = dict(extra_configs or {})
    extra_packages = extra_configs.pop("spark.jars.packages", "")
    packages = ",".join(
        package
        for package in [delta_package, postgres_package, extra_packages]
        if package
    )
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER_URL", "local[*]"))
        .config("spark.jars.packages", packages)
        .config("spark.jars.ivy", os.getenv("SPARK_IVY_DIR", "/tmp/.ivy2"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
        .config("spark.sql.autoBroadcastJoinThreshold", "50MB")
        .config("spark.sql.shuffle.partitions", "200")
    )
    for key, value in extra_configs.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def get_spark_with_minio(app_name: str = "vnstock_pipeline") -> SparkSession:
    """Spark session with MinIO S3 config — for writing Delta tables to MinIO."""
    return get_spark(
        app_name,
        {
            "spark.hadoop.fs.s3a.endpoint": os.getenv(
                "MINIO_ENDPOINT",
                "http://minio:9000",
            ),
            "spark.hadoop.fs.s3a.access.key": os.getenv(
                "MINIO_ACCESS_KEY",
                "minioadmin",
            ),
            "spark.hadoop.fs.s3a.secret.key": os.getenv(
                "MINIO_SECRET_KEY",
                "minioadmin",
            ),
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        },
    )
