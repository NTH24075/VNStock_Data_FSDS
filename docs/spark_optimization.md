# Spark Optimization — Offline Data Problems

> Evidence: Spark UI screenshots (before/after), code snippets, Airflow integration.
> Linked from: README.md §Documentation

## Baseline

Naive Spark job reading Bronze Delta, writing Silver Delta, with default Spark config.
Screenshots from Spark UI showing task skew, shuffle volume, and job duration.

## Optimization 1: Skew Handling (VN30 concentration)

**Problem:** 80% of volume in 30/400 tickers → `groupBy(ticker_id)` or `join` sends disproportionate rows to VN30 partitions.

**Fixes applied:**
1. **AQE skew join** — `spark.sql.adaptive.enabled=true`, `spark.sql.adaptive.skewJoin.enabled=true`
2. **Broadcast join** for `dim_ticker` (~400 rows) against fact tables
3. **Manual salting** (fallback) — documented but not default

**Before/After:** Spark UI screenshots showing task duration distribution.

## Optimization 2: High Cardinality

**Problem:** `countDistinct(trade_id)` over millions of rows triggers full shuffle-and-sort.

**Fix:** `approx_count_distinct` (HyperLogLog) for quality report; exact counts only on daily-grain tables.

## Optimization 3: Schema Evolution

**Problem:** Partitions before 2025-07-01 missing `foreign_room` and `value` columns.

**Fix:** Explicit versioned schema read (StructType per `_schema_version`), missing columns added as typed NULL literals, `delta.schema.autoMerge.enabled = false` at Bronze table level.

## Optimization 4: Duplicate Handling

**Fix:** Dedup by `(ticker_id, trade_date)` keeping latest `_ingested_at` via row_number window.

## Airflow Integration

Each Spark job runs as `SparkSubmitOperator` / `KubernetesPodOperator` within the `silver_daily` and `gold_facts` DAGs.
