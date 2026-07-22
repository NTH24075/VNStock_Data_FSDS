# Schema Design — All Zones

> Evidence: DBeaver screenshots, table relationship diagrams.
> Linked from: README.md §Documentation

## Naming Convention

| Layer | Prefix | Storage | Purpose |
|-------|--------|---------|---------|
| Bronze | `raw_` | Delta on MinIO | Raw ingestion, append-only, ingest metadata |
| Silver | `stg_` | Delta on MinIO | Deduped, type-cast, schema-harmonized |
| Gold | `dim_`, `fact_`, `obt_`, `feat_`, `agg_` | Delta + Trino | Business-ready, query-optimized |

Feature tables follow the naming: `feat_<entity>_<granularity>`.

All feature tables carry two timestamp columns:
- `event_timestamp` — the feature as-of time (for point-in-time joins)
- `created_ts` — computation time (for dedup, keep latest)

## Bronze Zone (`raw_`)

| Table | Grain | Source | Notes |
|-------|-------|--------|-------|
| `raw_ohlcv_daily` | (ticker_id, trade_date) | MinIO landing Parquet | Schema v1 (pre-2025-07-01) and v2 (post) |
| `raw_foreign_flow` | (ticker_id, trade_date) | MinIO landing Parquet | |
| `raw_corporate_actions` | action_id | vendor_db (JDBC) | |
| `raw_financial_ratios` | (ticker_id, report_quarter) | MinIO landing Parquet | |
| `raw_market_events` | event_id | Kafka (Flink) | Append-only, Kafka offset metadata |

All Bronze tables carry: `_ingested_at`, `batch_id`, `_schema_version`.

## Silver Zone (`stg_`)

| Table | Grain | Transform | Notes |
|-------|-------|-----------|-------|
| `stg_daily_price` | (ticker_id, trade_date) | Dedup, schema harmonize, trading-calendar validate | Missing columns from v1 → typed NULL + `_schema_version` tag |
| `stg_foreign_flow` | (ticker_id, trade_date) | Dedup | |
| `stg_corporate_actions` | action_id | | |
| `stg_trades` | trade_id | Dedup by event_id + latest created_ts, watermark=60s | |
| `stg_quotes` | event_id | Dedup | |
| `stg_events_quarantine` | event_id | Late-beyond-watermark events | |

## Gold Zone

### Dimensions (`dim_`)

| Table | Grain | SCD Strategy | Key Columns |
|-------|-------|-------------|------------|
| `dim_ticker` | one per ticker version | **SCD Type 2** | `ticker_key` (SK), `ticker_id` (BK), `valid_from_ts`, `valid_to_ts`, `is_current` |
| `dim_date` | one per calendar date | Static | `date_key`, `is_trading_day`, `holiday_name` |
| `dim_industry` | one per ICB node | Static | `industry_key` (SK), `icb_code` (BK), `icb_level`, `icb_name` |
| `dim_exchange` | one per exchange | Static | `exchange_key` (SK), `exchange_code`, `price_limit_pct` |
| `dim_session` | one per session type | Static | `session_key` (SK), `session_type` |

### Facts (`fact_`)

| Table | Grain | Measures |
|-------|-------|----------|
| `fact_daily_price` | (ticker_key, trade_date_key) | open, high, low, close, adj_close, volume, value, foreign_room |
| `fact_intraday_trade` | trade_id | price, quantity, trade_value |
| `fact_foreign_flow` | (ticker_key, trade_date_key) | foreign_buy/sell_vol, foreign_buy/sell_value, foreign_net_value |

### OBT (`obt_`)

| Table | Grain | Purpose |
|-------|-------|---------|
| `obt_ticker_daily_performance` | (ticker_key, trade_date_key) | Denormalized BI dashboard table — no joins needed |

### Feature Tables (`feat_`)

All feature tables carry `event_timestamp` + `created_ts`.

| Table | Grain | Features | Refresh |
|-------|-------|----------|---------|
| `feat_ticker_daily` | (ticker_id, event_timestamp) | return_5d, volatility_20d, ma20_gap, foreign_net_ratio_10d | Daily after close |
| `feat_stream_intraday` | (ticker_id, event_timestamp) | volume_5m, trade_count_30m, price_momentum_30m, burst_flag | 1–5 min during trading |
| `feat_ticker_unified` | (ticker_id, event_timestamp) | Join of daily + intraday features | 15 min / EOD |

### Monitoring Tables (`agg_`)

| Table | Grain | Purpose |
|-------|-------|---------|
| `agg_feature_health_daily` | (monitoring_date, feature_name) | Daily mean + PSI vs baseline; alert when PSI > 0.15 |
| `feature_drift_alerts` | alert_date | Alert log with PSI value + recommended action |

### Label & Training Tables

| Table | Grain | Key Columns |
|-------|-------|------------|
| `ml_ticker_label` | (ticker_id, event_timestamp) | `event_timestamp` (feature snapshot T), `created_ts` (T+3 close), `label` (0/1) |
| `ml_ticker_training` | (ticker_id, event_timestamp) | Point-in-time join of label + feat_ticker_unified |

## Dimension–Fact Relationships

```
dim_date ─────────────┬── fact_daily_price ──┬── obt_ticker_daily_performance
dim_ticker (SCD2) ────┤                      │
dim_exchange ─────────┤                      │
dim_industry ─────────┘                      │
                      ├── fact_foreign_flow ──┤
                      ├── fact_intraday_trade─┤
dim_session ──────────┘                      │
                                             │
feat_ticker_daily ─────┬── feat_ticker_unified ── ml_ticker_training
feat_stream_intraday ──┘       │
                               └── ml_ticker_label
```
