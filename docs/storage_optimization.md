# Storage Optimization

> Evidence: Trino EXPLAIN ANALYZE output, Delta OPTIMIZE logs.
> Linked from: README.md §Documentation

## Lakehouse (Delta on MinIO)

### Optimization 1: Dashboard Query (sector heatmap)

- **Workload:** `obt_ticker_daily_performance` filtered by date range and icb_l1
- **Bottleneck:** Full scan across dates; small-file problem from daily incremental writes
- **Fix:** Partition by `trade_date` (monthly), Z-order by `ticker_id`; scheduled `OPTIMIZE` compaction weekly
- **Result:** Expected scan reduction ~70–90% for 1-month dashboard ranges
- **Trade-off:** Weekly compaction adds maintenance job, temporary 2× storage during rewrite

### Optimization 2: Intraday Momentum Query (single ticker)

- **Workload:** `fact_intraday_trade` filtered by single ticker (skewed: VN30 partitions ~10× larger)
- **Bottleneck:** Data skew in file sizes
- **Fix:** Partition by `trade_date`, Z-order by `ticker_id`
- **Trade-off:** Z-order maintenance cost on every compaction

## Datawarehouse (Trino-exposed Gold)

- **Workload:** Ad-hoc analyst queries filtering by `icb_l1` and `is_vn30`
- **Bottleneck:** Categorical filter falls back to full scan
- **Fixes:**
  - Bloom filter index on `dim_ticker.ticker` (point lookup)
  - Zone maps (min/max stats) for `trade_date` range filters
  - Secondary partition on `is_vn30` (low-cardinality, 30/400 split)
- **Result:** File-skip ratio from Trino `EXPLAIN ANALYZE` skipped-splits count
- **Trade-off:** Extra partition increases small files; bloom filter adds write-time overhead
