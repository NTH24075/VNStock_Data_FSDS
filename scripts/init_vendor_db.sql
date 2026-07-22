-- =============================================================================
-- Initialize vendor_db — reference tables for the "external system" pattern
-- Based on 01_data_generator.md Section 2.5
-- =============================================================================

CREATE TABLE IF NOT EXISTS tickers (
    ticker_id   VARCHAR(10) PRIMARY KEY,
    ticker      VARCHAR(10) NOT NULL,
    company_name VARCHAR(255) NOT NULL,
    exchange    VARCHAR(10) NOT NULL CHECK (exchange IN ('HOSE', 'HNX', 'UPCOM')),
    icb_industry_l1 VARCHAR(100),
    icb_industry_l2 VARCHAR(100),
    listing_date DATE NOT NULL,
    is_active   BOOLEAN DEFAULT true,
    vn30        BOOLEAN DEFAULT false
);

CREATE TABLE IF NOT EXISTS corporate_actions (
    action_id   VARCHAR(20) PRIMARY KEY,
    ticker_id   VARCHAR(10) NOT NULL REFERENCES tickers(ticker_id),
    action_type VARCHAR(20) NOT NULL CHECK (action_type IN ('cash_dividend', 'stock_dividend', 'split')),
    ex_date     DATE NOT NULL,
    ratio       FLOAT NOT NULL,
    announced_ts TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ca_ticker ON corporate_actions(ticker_id);
CREATE INDEX IF NOT EXISTS idx_ca_ex_date ON corporate_actions(ex_date);
