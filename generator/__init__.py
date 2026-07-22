"""
Vietnam Stock Market Data Pipeline — Data Generator
Based on 01_data_generator.md and 03_data_generator_improvement.md

Produces:
  - Offline: Parquet files landed in MinIO (landing-vendor-offline/)
  - Streaming: JSON events to Kafka (stock_market_events) + JSONL file-sink
  - Reference: tickers + corporate_actions to PostgreSQL (vendor_db)
"""

__version__ = "0.1.0"
