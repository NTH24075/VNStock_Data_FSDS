"""Silver transformations — Spark + Flink dedup and harmonization."""

# Based on schema_design.md, pipeline 2.
# Jobs:
#   - silver_daily.py           — Spark dedup (ticker_id, trade_date), schema harmonization
#   - silver_stream.py          — Flink dedup (event_id) + watermark + quarantine
