# Flink Optimization — Streaming Data Problems

> Evidence: Flink UI screenshots (before/after), code snippets.
> Linked from: README.md §Documentation

## Baseline

Naive Flink job consuming Kafka with default parallelism, processing-time windows, no watermark.

## Optimization 1: Burst Handling (ATO/ATC ×25)

**Problem:** 200 → 5,000 events/min during ATO (09:00–09:15) and ATC (14:30–14:45) → backpressure on single partition.

**Fixes:**
1. Kafka topic partitioned by `ticker_id` hash (12 partitions)
2. `env.setParallelism()` sized for peak (×25), not average
3. `AsyncDataStream` for external enrichment lookups

## Optimization 2: Late Arrival Handling (12%, 5–45s)

**Fix:** Event-time processing with `BoundedOutOfOrdernessWatermark` (60s max out-of-orderness), 60s allowed lateness on windows, side output for quarantine.

## Optimization 3: Duplicate Handling (1.5%)

**Fix:** Dedup by `event_id` keeping latest `created_ts` in a KeyedProcessFunction with state TTL.

## Window Processing

| Feature | Window Type | Size | Slide | Key |
|---------|------------|------|-------|-----|
| `f_stream_volume_5m` | Tumbling | 5 min | — | ticker_id |
| `f_stream_trade_count_30m` | Sliding | 30 min | 1 min | ticker_id |
| `f_stream_price_momentum_30m` | Sliding | 30 min | 1 min | ticker_id |
| `f_stream_burst_flag` | Tumbling | 1 min | — | — (global) |

Session-gap aware: lunch break (11:30–13:00) configured as session gap — windows produce no output during gap.
