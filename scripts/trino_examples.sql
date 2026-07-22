-- =============================================================================
-- Trino Federated Query Examples
-- =============================================================================
-- Run via: trino CLI (docker exec -it vnstock-trino trino)
-- Or: DBeaver connected to trino:8083, catalog: delta or postgres
-- =============================================================================

-- 1. SHOW CATALOGS (verify both Delta and PostgreSQL are connected)
SHOW CATALOGS;
-- Expected output: delta, postgres

-- 2. SHOW SCHEMAS in Delta catalog
SHOW SCHEMAS FROM delta;
-- Gold tables are in the default schema (or specify schema name)

-- 3. Describe a Gold table (schema discovery from Delta)
DESCRIBE delta."gold_stock".fact_daily_price;

-- 4. Top 10 tickers by average daily volume (last 20 trading days)
SELECT
    ticker_id,
    ROUND(AVG(volume), 0) AS avg_volume,
    ROUND(AVG(close), 2) AS avg_close
FROM delta."gold_stock".fact_daily_price
WHERE trade_date >= DATE '2025-06-01'
GROUP BY ticker_id
ORDER BY avg_volume DESC
LIMIT 10;

-- 5. Cross-source JOIN: Delta Gold + PostgreSQL reference data
-- Joins fact_daily_price (Delta/MinIO) with tickers (PostgreSQL vendor_db)
SELECT
    f.ticker_id,
    t.company_name,
    t.exchange,
    t.icb_l1,
    f.trade_date,
    f.close,
    f.volume
FROM delta."gold_stock".fact_daily_price f
JOIN postgres.vendor_db.tickers t ON f.ticker_id = t.ticker_id
WHERE f.trade_date = DATE '2025-07-15'
  AND t.is_active = TRUE
ORDER BY f.volume DESC
LIMIT 20;

-- 6. Sector performance: average daily return by industry (ICB L1)
SELECT
    t.icb_l1,
    COUNT(DISTINCT f.ticker_id) AS ticker_count,
    ROUND(AVG((f.close - f.open) / f.open * 100), 2) AS avg_daily_return_pct,
    ROUND(SUM(f.volume), 0) AS total_volume
FROM delta."gold_stock".fact_daily_price f
JOIN postgres.vendor_db.tickers t ON f.ticker_id = t.ticker_id
WHERE f.trade_date = DATE '2025-07-15'
GROUP BY t.icb_l1
ORDER BY total_volume DESC;

-- 7. OBT query: top movers dashboard (single table, no joins needed)
SELECT
    ticker,
    company_name,
    exchange,
    icb_l1,
    trade_date,
    close,
    pct_change_1d,
    pct_change_5d,
    volume_vs_ma20_ratio,
    price_limit_hit_flag
FROM delta."gold_stock".obt_ticker_daily_performance
WHERE trade_date = DATE '2025-07-15'
  AND is_vn30 = TRUE
ORDER BY pct_change_1d DESC
LIMIT 10;

-- 8. EXPLAIN: verify partition pruning works
EXPLAIN
SELECT COUNT(*) FROM delta."gold_stock".fact_daily_price
WHERE trade_date = DATE '2025-07-15';
