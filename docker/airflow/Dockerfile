# =============================================================================
# Airflow custom image — extends apache/airflow with project Python dependencies
# =============================================================================

FROM apache/airflow:2.9.0

RUN pip install --no-cache-dir \
    pyspark delta-spark pandas numpy pyarrow minio
