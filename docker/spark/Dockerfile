# =============================================================================
# Spark custom image — extends bitnami/spark with project Python dependencies
# =============================================================================

FROM bitnami/spark:3.5

USER root
RUN pip install --no-cache-dir \
    delta-spark pandas numpy pyarrow minio

USER 1001
