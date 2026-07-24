# =============================================================================
# Spark custom image — Apache Spark plus the Python dependencies used by jobs/tests
# =============================================================================

FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root
RUN python3 -m pip install --no-cache-dir \
    pandas numpy pyarrow pyyaml minio pytest pytest-cov

ENV PYTHONPATH="/opt/project:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip"

USER 185
