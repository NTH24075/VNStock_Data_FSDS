# Data Governance — Lineage & Data Contracts

> Evidence: DataHub UI screenshots (lineage graph, schema metadata, contract assertions).
> Linked from: README.md §Documentation

## Lineage

Dataset and job lineage published to DataHub via Spark/Airflow OpenLineage emitter.

### DP1: Bronze Ingestion

```
[landing-vendor-offline/] ──┐
                             ├──► [bronze_offline_ingest] ──► raw_ohlcv_daily
[vendor_db (PostgreSQL)] ────┘                                raw_foreign_flow
                                                              raw_corporate_actions
                                                              raw_financial_ratios

[Kafka: stock_market_events] ──► [bronze_stream_ingest] ──► raw_market_events
```

### DP2: Silver + Gold

```
raw_ohlcv_daily ──► [silver_daily] ──► stg_daily_price ──► [gold_facts] ──► fact_daily_price
                                                                           fact_foreign_flow
                                                                           obt_ticker_daily_performance
```

### DP3: Feature Tables

```
fact_daily_price ──► [feat_daily_job] ──► feat_ticker_daily
stg_trades ────────► [feat_stream_job] ──► feat_stream_intraday
feat_ticker_daily ─┬──► [feat_unified_job] ──► feat_ticker_unified
feat_stream_intraday┘
```

## Data Contracts

Versioned JSON schemas in `contracts/`:

| Contract | `_schema_version` | Columns |
|----------|-------------------|---------|
| `raw_ohlcv_daily.v1.json` | 1 | ticker_id, trade_date, open, high, low, close, volume (7 cols) |
| `raw_ohlcv_daily.v2.json` | 2 | + value, foreign_room (9 cols) |

### Enforcement

`bronze_offline_ingest` validates incoming Parquet against the contract matching its `_schema_version` before writing to Delta. A column in the data but absent from any known contract → "contract violation" alert (distinct from known schema evolution, which is a planned v1→v2 change).

`delta.schema.autoMerge.enabled = false` at Bronze table level — every schema change must go through the contract-versioned path.
