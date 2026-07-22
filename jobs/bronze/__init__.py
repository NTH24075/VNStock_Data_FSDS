"""Bronze ingestion — Spark jobs for landing → Bronze Delta tables."""

# Based on schema_design.md, pipeline 1.
# Jobs:
#   - bronze_offline_ingest.py   — Parquet + JDBC → Delta on MinIO
#   - bronze_stream_ingest.py    — Kafka → Delta append-only
