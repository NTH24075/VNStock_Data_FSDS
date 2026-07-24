# Data Quality Report

## Generator Configuration
- n_tickers: 400
- vn30_count: 30
- days_history: 180
- random_seed: 42
- trading_calendar: HOSE
- schema_change_date: 2025-07-01
- price_limit: HOSE ±0.07 | HNX ±0.1 | UPCOM ±0.15

## Data Volume & Format
- Offline rows (OHLCV): 73,440
- Offline format: Parquet (snappy), partitioned by trade_date
- Offline tables: ohlcv_daily, foreign_flow_daily, corporate_actions, financial_ratios
- Streaming events: 197,925
- Streaming format: JSON (Kafka topic: stock_market_events_v3) + JSONL file-sink
- Trading days in window: 180

## Skew Distribution
- VN30 volume share: 80.1% (6,867,719,247 / 8,573,678,812)
- Non-VN30 volume share: 19.9% (1,705,959,565 / 8,573,678,812)
- Total volume: 8,573,678,812
- Industry value share (configured):
  - banking: 35%
  - real_estate: 25%

## Cardinality
- approx_count_distinct(ticker_id): 400
- approx_count_distinct(ticker_id, trade_date): 72000
- approx_count_distinct(trade_date): 180
- High-cardinality composite key: ticker_id × trade_date = 72000 unique pairs

## Schema Evolution
- Schema change date: 2025-07-01
- Rows with _schema_version=1 (pre-change): 44043
- Rows with _schema_version=2 (post-change): 29397
- foreign_room IS NULL (v1 partitions, missing by design): 44043
- value IS NULL (v1 partitions, missing by design): 44043

## Duplicate Rate (Offline)
- Configured duplicate rate: 2.0%
- Rows before dedup: 73,440
- Duplicate rows detected: 1440
- Duplicate rate (actual): 2.0%
- Dedup key: (ticker_id, trade_date)

## Streaming Quality
- Total events: 197,925
- Late arrivals: 34037 / 197925 (17.2%)
- Configured late rate: 12.0% (delay 5-45s)
- Duplicate events (stream): 2925 / 197925 (1.5%)
- Configured stream dup rate: 1.5%

## Streaming Burst Profile
- Baseline events/min: 200
- Burst multiplier: ×25 (ATO/ATC: 09:00-09:15 & 14:30-14:45)
- Peak events/min: 5,243
- Average events/min: 776
- Peak-to-average ratio: 6.8×

*Generated with seed=42*