# Data Generator

> Evidence: quality report output, config YAML.
> Linked from: README.md §Documentation

## Design

Hybrid strategy: real seed data from vnstock (ticker list, ICB classification), synthetic data from geometric random walk with injected problems.

## Configuration

See `config/generator.yaml` for all parameters.

### Offline Data Problems

| Problem | Parameter | Value | Verification |
|---------|-----------|-------|-------------|
| Skew | `vn30_volume_share`, `industry_value_share` | 80% VN30, 60% banking+real estate | Volume share by VN30 vs rest, value share by industry |
| High cardinality | `n_tickers × days_history` | ~400 × 180 = 72K keys | `approx_count_distinct` on ticker_id, trade_date |
| Schema evolution | `schema_change_date` | 2025-07-01 (v1→v2) | Column presence + null counts per partition |
| Duplicate rate | `duplicate_rate_offline` | 2% | Row count before/after dedup |

### Streaming Data Problems

| Problem | Parameter | Value | Verification |
|---------|-----------|-------|-------------|
| Burst | `burst_multiplier` | ×25 at ATO/ATC | Events/min timeline |
| Late arrivals | `late_arrival_rate`, `late_delay_sec_min_max` | 12%, 5–45s | Late event rate by window |
| Duplicate rate | `duplicate_rate_stream` | 1.5% | Event count before/after dedup |

## Quality Report

Auto-generated per run:
- Skew distribution (volume by VN30, value by industry)
- Cardinality (approx_count_distinct on key columns)
- Schema evolution (column presence, null counts per version)
- Duplicate rates (offline + stream, before/after dedup)
- Streaming burst profile (events/min timeline)
- Late-arrival histogram

## Output

| Type | Format | Location | Pattern |
|------|--------|----------|---------|
| Offline | Parquet (partitioned by trade_date) | `landing-vendor-offline/run_date=YYYY-MM-DD/` | End-of-day file drop |
| Reference | PostgreSQL | `vendor_db` (tickers, corporate_actions) | Batch DB-extract |
| Streaming | JSON → Kafka + JSONL file-sink | `stock_market_events` topic | Real-time events + replay audit log |
