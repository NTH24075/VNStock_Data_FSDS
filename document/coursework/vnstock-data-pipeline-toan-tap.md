# Vietnam Stock Market Data Pipeline — Toàn Bộ Kiến Thức Vấn Đáp

## Mục lục
1. [Tổng quan dự án](#1-tổng-quan-dự-án)
2. [Kiến trúc hệ thống](#2-kiến-trúc-hệ-thống)
3. [Data Generator — Đầu vào](#3-data-generator--đầu-vào)
4. [Pipeline — Bronze → Silver → Gold](#4-pipeline--bronze--silver--gold)
5. [Đầu ra của toàn pipeline](#5-đầu-ra-của-toàn-pipeline)
6. [Các bài toán xử lý & cách giải quyết](#6-các-bài-toán-xử-lý--cách-giải-quyết)
7. [Tại sao chọn X, không chọn Y?](#7-tại-sao-chọn-x-không-chọn-y)
8. [Data Contracts & Schema Evolution](#8-data-contracts--schema-evolution)
9. [Data Governance & Lineage](#9-data-governance--lineage)
10. [Drift Monitoring & Data Quality](#10-drift-monitoring--data-quality)
11. [Tối ưu hóa](#11-tối-ưu-hóa)
12. [Testing Strategy](#12-testing-strategy)
13. [Câu hỏi vấn đáp thường gặp](#13-câu-hỏi-vấn-đáp-thường-gặp)

---

## 1. Tổng quan dự án

### Bài toán kinh doanh (Business Domain)
Mô phỏng **thị trường chứng khoán Việt Nam (HOSE-centric)** — dữ liệu giá, khối lượng, foreign flow (dòng tiền nước ngoài), corporate actions (sự kiện doanh nghiệp: cổ tức, split), chỉ số — phục vụ pipeline Bronze → Silver → Gold cho market analytics và machine learning.

**Phạm vi:** ~400 mã chứng khoán, 3 sàn (HOSE, HNX, UPCOM), dữ liệu daily + streaming intraday.

### Mục tiêu kỹ thuật
- **End-to-end ETL pipeline** từ data generation → ingestion → transformation → serving
- **Medallion architecture** (Bronze → Silver → Gold) theo mô hình Data Lakehouse
- **Xử lý cả batch (offline) và real-time (streaming)** 
- **Tự động inject data problems** để pipeline thể hiện khả năng xử lý thực tế
- **Feature engineering** cho ML
- **Data quality & drift monitoring** tự động

### Công nghệ sử dụng (Stack)
| Layer | Công nghệ | Phiên bản |
|-------|-----------|-----------|
| Data Generation | Python 3.12, NumPy, Pandas, vnstock | numpy 1.26+, pandas 2.2+ |
| Storage (Lakehouse) | MinIO (S3-compatible object storage) | latest |
| Storage (Database) | PostgreSQL 16 | 16 |
| Table Format | Delta Lake (open-source) | 0.18+ |
| Batch Processing | PySpark | 3.5.0 |
| Stream Processing | Apache Flink | 1.18 |
| Message Queue | Apache Kafka | cp-7.6.0 |
| Orchestration | Apache Airflow | 2.9.0 |
| Query Engine | Trino | latest |
| Package Management | uv (Astral) + pip | — |
| Linting | Ruff | 0.4+ |
| Testing | pytest + pytest-cov | 8.2+ |
| Containerization | Docker Compose (10 services) | — |
| Governance | DataHub (OpenLineage) | — |

---

## 2. Kiến trúc hệ thống

### Deployment Diagram (Docker Compose Stack)

```
┌─────────────┐     ┌──────────────────────────────────────────────────────────────┐
│  Generator  │     │                    Docker Compose Stack                       │
│  (Python)   │     │                                                              │
│             │ (1) │  ┌──────────┐    ┌──────────┐    ┌──────────────────────────┐ │
│  ┌───────┐  │Parquet│  │  MinIO   │    │  Kafka   │    │        Airflow           │ │
│  │ offline│──┼─────►│  │(landing) │    │(events)  │    │  ┌────────────────────┐ │ │
│  │ Parquet│  │      │  └────┬─────┘    └────┬─────┘    │  │ bronze_offline     │ │ │
│  └───────┘  │      │       │               │           │  │ _ingest (15:30)    │ │ │
│             │      │       │ (5) Spark      │ (3) Flink │  └────────┬───────────┘ │ │
│  ┌───────┐  │ (2)  │       │ read           │ consume   │           │             │ │
│  │stream │──┼──┐   │  ┌────▼─────┐    ┌────▼─────┐    │  ┌────────▼───────────┐ │ │
│  │ JSON  │  │   │   │  │  Delta   │    │  Delta   │    │  │ silver_daily       │ │ │
│  └───────┘  │   │   │  │ (Bronze) │    │ (Bronze) │    │  │ (16:00)            │ │ │
│             │   │   │  └────┬─────┘    └────┬─────┘    │  └────────┬───────────┘ │ │
│  ┌───────┐  │   │   │       │ (7) Spark      │ (6) Flink│           │             │ │
│  │vendor │──┼───┼───┼──────►│   transform     │  dedup   │  ┌────────▼───────────┐ │ │
│  │  _db  │  │   │   │  ┌────▼─────┐    ┌────▼─────┐    │  │ gold_dimensions    │ │ │
│  │(PG)   │  │   │   │  │  Delta   │    │  Delta   │    │  │ _and_facts (16:30) │ │ │
│  └───────┘  │   │   │  │ (Silver) │    │ (Silver) │    │  └────────┬───────────┘ │ │
│             │   │   │  └────┬─────┘    └────┬─────┘    │           │             │ │
│             │   │   │       │ (9) Spark      │           │  ┌────────▼───────────┐ │ │
│             │   │   │       │   aggregate     │           │  │ feat_daily_job     │ │ │
│             │   │   │  ┌────▼─────┐          │           │  │ (17:00)            │ │ │
│             │   │   │  │  Delta   │◄─────────┘           │  └────────┬───────────┘ │ │
│             │   │   │  │ (Gold)   │                      │           │             │ │
│             │   │   │  └────┬─────┘                      │           │ (10) Trino  │ │
│             │   │   │       │                            │  ┌────────▼───────────┐ │ │
│             │   │   │       └────────────────────────────┼──│       Trino        │ │ │
│             │   │   │                                    │  │  (BI / ad-hoc)     │ │ │
│             │   │   │                                    │  └────────────────────┘ │ │
│             │   │   └────────────────────────────────────┴──────────────────────────┘ │
└─────────────┘
```

### 10 Docker Services
| STT | Service | Image | Port | Vai trò |
|-----|---------|-------|------|---------|
| 1 | postgres | postgres:16 | 5433 | vendor_db (tickers, corporate_actions) + Airflow metadata |
| 2 | minio | minio/minio | 9000, 9001 | S3-compatible storage (landing + Delta Lake) |
| 3 | minio-create-buckets | minio/mc | — | Init: tạo bucket landing + delta-lake |
| 4 | zookeeper | cp-zookeeper:7.6.0 | 2181 | Kafka cluster coordination |
| 5 | kafka | cp-kafka:7.6.0 | 9092 | Message queue cho streaming events |
| 6 | generator | custom (Dockerfile) | — | Sinh dữ liệu offline + streaming |
| 7 | spark-master + spark-worker | apache/spark:3.5.0 | 8080, 7077 | Batch processing (Bronze → Silver → Gold) |
| 8 | flink-jobmanager + taskmanager | flink:1.18 | 8081 | Stream processing (Kafka → Bronze → Silver) |
| 9 | airflow-webserver + scheduler | custom (docker/airflow) | 8082 | Orchestration (schedule DAGs) |
| 10 | trino | trinodb/trino | 8083 | SQL query engine (BI/ad-hoc) |

---

## 3. Data Generator — Đầu vào

### Chiến lược sinh dữ liệu
**Hybrid:** Real seed data từ vnstock (danh sách mã, ICB classification) + Synthetic data từ geometric random walk + Có chủ đích inject các data problems.

### Đầu vào từ Seed Data
- **Ticker reference:** File `generator/seed/tickers_reference.json` — chứa danh sách mã chứng khoán thật từ vnstock API (Listing.all_symbols()), bao gồm: `ticker_id`, `company_name`, `exchange` (HOSE/HNX/UPCOM), ICB classification
- **API vnstock:** `vnstock.api.listing.Listing` — lấy symbols_by_exchange, mapping exchange

### Cấu hình Generator (`config/generator.yaml`)
```yaml
n_tickers: 50                    # Số mã chứng khoán
vn30_count: 30                   # Số mã trong rổ VN30
vn30_volume_share: 0.80          # VN30 chiếm 80% volume
days_history: 30                 # Số ngày giao dịch lịch sử
trading_calendar: "HOSE"         # Lịch giao dịch T2-T6 trừ ngày lễ VN
schema_change_date: "2025-07-01" # Ngày thay đổi schema (v1 → v2)
random_seed: 42                  # Để tái lập kết quả
```

### Dữ liệu sinh ra (Generator Output)

#### a) Offline Batch (Parquet → MinIO landing)
| Table | Grain (Khóa) | Số cột | Mô tả |
|-------|-------------|--------|-------|
| `ohlcv_daily` | (ticker_id, trade_date) | 7 (v1) / 9 (v2) | Open, High, Low, Close, Volume, Value, Foreign Room |
| `foreign_flow_daily` | (ticker_id, trade_date) | 6 | foreign_buy/sell_vol, foreign_buy/sell_value |
| `corporate_actions` | action_id | 5 | cash_dividend, stock_dividend, split |
| `financial_ratios` | (ticker_id, report_quarter) | 6 | EPS, PE, PB, ROE |

#### b) Streaming (JSON → Kafka + JSONL file-sink)
| Event Type | Tỷ lệ | Nội dung |
|-----------|-------|----------|
| trade | 85% × ~80% = 68% | ticker, price, quantity, side (buy/sell), trade_id |
| quote | 15% × ~80% = 12% | bid_price, bid_qty, ask_price, ask_qty |
| index_update | 20% | VNINDEX/VN30, index_value, index_change_pct |

#### c) Reference Tables (PostgreSQL vendor_db)
- **tickers:** Mã chứng khoán, tên công ty, sàn, ngành ICB, listing_date, is_active, vn30
- **corporate_actions:** Sự kiện doanh nghiệp (cổ tức tiền mặt, cổ tức cổ phiếu, split)

### Các vấn đề dữ liệu được inject có chủ đích

#### Offline Problems
| Vấn đề | Cơ chế | Tham số | Mục đích |
|--------|--------|---------|----------|
| **Skew (Volume Concentration)** | VN30 nhận 80% tổng volume, Banking+Real Estate chiếm 60% value | `vn30_volume_share=0.80` | Test khả năng xử lý skew join của Spark |
| **High Cardinality** | n_tickers × days_history ~ 400 × 180 = 72K unique keys | — | Test `countDistinct` performance |
| **Schema Evolution** | Cột `value` và `foreign_room` chỉ có từ 2025-07-01 | v1 (7 cols) → v2 (9 cols) | Test khả năng harmonize schema cũ và mới |
| **Duplicate (2%)** | Random chọn 2% rows và append bản copy | `duplicate_rate_offline=0.02` | Test dedup logic ở Silver |

#### Streaming Problems
| Vấn đề | Cơ chế | Tham số | Mục đích |
|--------|--------|---------|----------|
| **Burst (×25)** | ATO (09:00-09:15) và ATC (14:30-14:45) có volume gấp 25 lần | `burst_multiplier=25` | Test backpressure handling của Flink |
| **Late Arrival (12%)** | 12% events có `event_timestamp` trễ hơn `created_ts` từ 5-45s | `late_arrival_rate=0.12` | Test watermark strategy của Flink |
| **Duplicate (1.5%)** | 1.5% events bị re-emit (same event_id) | `duplicate_rate_stream=0.015` | Test dedup trong streaming |

### Đặc tính dữ liệu (Data Characteristics)
| Dimension | Offline | Streaming |
|-----------|---------|-----------|
| **Volume** | ~50 tickers × 30 trading days × 4 tables ≈ 6K rows | ~150K–200K events/trading day |
| **Velocity** | Daily end-of-day drop (15:00) | 200 events/min baseline, ×25 burst (ATO/ATC) |
| **Key** | (ticker_id, trade_date) per table | event_id (UUID) per event |
| **Problems injected** | 80% volume skew (VN30), schema evolution (v1→v2), 2% duplicates | ×25 burst, 12% late (5–45s), 1.5% duplicates |

---

## 4. Pipeline — Bronze → Silver → Gold

### Tổng quan lịch trình (Airflow DAGs)
| Pipeline | Công cụ | Schedule | Input | Output |
|----------|---------|----------|-------|--------|
| Bronze offline | Airflow + Spark | Daily 15:30 | MinIO landing + PG vendor_db | Delta `raw_*` |
| Bronze stream | Flink | Continuous | Kafka `stock_market_events` | Delta `raw_market_events` |
| Silver daily | Airflow + Spark | Daily 16:00 | Bronze Delta | Delta `stg_*` |
| Silver stream | Flink | Continuous | Bronze Delta | Delta `stg_trades`, `stg_quotes` |
| Gold dims/facts/OBT | Airflow + Spark | Daily 16:30 | Silver Delta | Delta `dim_*`, `fact_*`, `obt_*` |
| Feature + drift + labels | Airflow + Spark/Flink | Daily 17:00 | Gold Delta | `feat_*`, `agg_feature_health_daily`, `ml_ticker_label` |

### Bronze Zone (raw_*) — Raw Ingestion
**Nhiệm vụ:** Ingest dữ liệu thô, không transform, thêm metadata

**Đầu vào:**
- Parquet từ MinIO landing (`landing-vendor-offline/run_date=YYYY-MM-DD/`)
- PostgreSQL vendor_db (tickers, corporate_actions) qua JDBC
- Kafka topic `stock_market_events`

**Xử lý:**
1. Đọc Parquet từ landing zone
2. Phân tách theo `_schema_version` (v1 hoặc v2)
3. Validate data contract (JSON schema) trước khi write
4. Thêm `_ingested_at`, `_batch_id`, `_schema_version`
5. Write Delta Lake append-only

**Đầu ra (Bronze tables):**
| Table | Grain | Nguồn |
|-------|-------|-------|
| `raw_ohlcv_daily_v1` | (ticker_id, trade_date) | Parquet landing |
| `raw_ohlcv_daily_v2` | (ticker_id, trade_date) | Parquet landing |
| `raw_foreign_flow` | (ticker_id, trade_date) | Parquet landing |
| `raw_corporate_actions` | action_id | Parquet landing |
| `raw_financial_ratios` | (ticker_id, report_quarter) | Parquet landing |
| `raw_market_events` | event_id | Kafka (Flink) |

### Silver Zone (stg_*) — Cleansing & Standardization
**Nhiệm vụ:** Dedup, harmonize schema, validate chất lượng

**Xử lý:**
1. **Schema harmonization:** Đọc cả v1 và v2 của raw_ohlcv_daily, merge bằng `unionByName(allowMissingColumns=True)`. Cột `value` và `foreign_room` từ v1 → NULL typed
2. **Dedup:** Window function `row_number() OVER (PARTITION BY ticker_id, trade_date ORDER BY _ingested_at DESC)`, giữ lại row mới nhất
3. **Domain validation:** Thêm cột `_dq_*` flag các giá trị bất thường:
   - `_dq_price_positive`: close > 0
   - `_dq_high_ge_max`: high >= max(open, close)
   - `_dq_low_le_min`: low <= min(open, close)
   - `_dq_volume_nonneg`: volume >= 0
4. **Trading calendar validation:** Kiểm tra không có ngày cuối tuần trong dữ liệu
5. FLink dedup stream: `KeyedProcessFunction` dedup by `event_id` keeping latest `created_ts`, watermark 60s, late events → quarantine

**Đầu ra (Silver tables):**
| Table | Grain | Đặc điểm |
|-------|-------|----------|
| `stg_daily_price` | (ticker_id, trade_date) | Đã dedup + harmonize schema v1/v2 |
| `stg_foreign_flow` | (ticker_id, trade_date) | Đã dedup |
| `stg_trades` | trade_id | Flink dedup by event_id |
| `stg_quotes` | event_id | Flink dedup |
| `stg_events_quarantine` | event_id | Late events (>60s watermark) |

### Gold Zone — Business-Ready
**Nhiệm vụ:** Tạo dimensions (SCD2), facts, OBT, feature tables, labels

#### Dimensions (dim_*)
| Table | SCD Strategy | Key Columns |
|-------|-------------|-------------|
| `dim_ticker` | **SCD Type 2** | `ticker_key` (surrogate), `ticker_id` (business), `valid_from_ts`, `valid_to_ts`, `is_current` |
| `dim_date` | Static | `date_key`, `is_trading_day`, `holiday_name` |
| `dim_industry` | Static | `industry_key`, `icb_code`, `icb_level`, `icb_name` |
| `dim_exchange` | Static | `exchange_key`, `exchange_code`, `price_limit_pct` |
| `dim_session` | Static | `session_key`, `session_type` (ATO/CONTINUOUS/ATC/PUT_THROUGH) |

#### Facts (fact_*)
| Table | Grain | Measures |
|-------|-------|----------|
| `fact_daily_price` | (ticker_key, date_key) | open, high, low, close, adj_close, volume, value, foreign_room |
| `fact_intraday_trade` | trade_id | price, quantity, trade_value |
| `fact_foreign_flow` | (ticker_key, date_key) | foreign_buy/sell_vol, foreign_buy/sell_value |

#### OBT (obt_*) — One Big Table for BI
| Table | Grain | Columns |
|-------|-------|---------|
| `obt_ticker_daily_performance` | (ticker_id, trade_date) | close, adj_close, pct_change_1d, pct_change_5d, volume, volume_vs_ma20_ratio, value, foreign_room, price_limit_hit_flag, is_vn30, company_name, exchange, icb_l1/l2 |

**OBT là bảng denormalized** — analyst có thể query BI dashboard mà không cần JOIN. Bao gồm:
- Window functions: `prev_close`, `close_5d_ago` (LAG)
- `pct_change_1d` = (close - prev_close) / prev_close * 100
- `pct_change_5d` = (close - close_5d_ago) / close_5d_ago * 100
- `volume_vs_ma20_ratio` = volume / MA20(volume)
- `price_limit_hit_flag` = abs(pct_change_1d) >= 7%

#### Feature Tables (feat_*)
**Quy tắc:** Mọi feature table đều có `event_timestamp` (as-of time cho point-in-time join) + `created_ts` (computation time cho dedup)

| Table | Grain | Features | Refresh |
|-------|-------|----------|---------|
| `feat_ticker_daily` | (ticker_id, event_timestamp) | return_5d, volatility_20d, ma20_gap, foreign_net_ratio_10d | Daily |
| `feat_stream_intraday` | (ticker_id, event_timestamp) | volume_5m, trade_count_30m, price_momentum_30m, burst_flag | 1-5 min |
| `feat_ticker_unified` | (ticker_id, event_timestamp) | Join daily + intraday features | 15 min / EOD |

**Cách tính feature:**
- `f_ticker_return_5d`: (close - lag(close, 5)) / lag(close, 5)
- `f_ticker_volatility_20d`: stddev(return_1d) over 20-day rolling window
- `f_ticker_ma20_gap`: (close - MA20(close)) / MA20(close)
- `f_ticker_foreign_net_ratio_10d`: sum(net_foreign) / sum(value) over 10-day window

#### Label Table (ml_ticker_label)
| Table | Grain | Label |
|-------|-------|-------|
| `ml_ticker_label` | (ticker_id, event_timestamp) | 1 nếu close sau 3 ngày > 1.01 × current close, ngược lại 0 |

**Cách tạo label:** `close_t3 = LEAD(adj_close, 3)`, label = 1 khi `close_t3 > adj_close * 1.01` (tăng >1% sau 3 ngày)

#### Training Table (ml_ticker_training)
Point-in-time join của `ml_ticker_label` + `feat_ticker_daily` — sẵn sàng train ML model.

#### Monitoring Tables (agg_*)
| Table | Grain | Columns |
|-------|-------|---------|
| `agg_feature_health_daily` | (monitoring_date, feature_name) | mean_value, psi_vs_baseline, alert_flag |
| `feature_drift_alerts` | alert_date | alert_date, feature_name, psi_value, action |

---

## 5. Đầu ra của toàn pipeline

### Tổng hợp tất cả output tables

| Layer | Table | Định dạng | Số cột (xấp xỉ) | Mục đích |
|-------|-------|-----------|-----------------|----------|
| Bronze | `raw_ohlcv_daily_v1` | Delta | 8 (7 + metadata) | Raw lưu trữ |
| Bronze | `raw_ohlcv_daily_v2` | Delta | 10 (9 + metadata) | Raw lưu trữ |
| Bronze | `raw_foreign_flow` | Delta | 8 | Raw lưu trữ |
| Bronze | `raw_corporate_actions` | Delta | 7 | Raw lưu trữ |
| Bronze | `raw_financial_ratios` | Delta | 8 | Raw lưu trữ |
| Bronze | `raw_market_events` | Delta | — | Streaming events |
| Silver | `stg_daily_price` | Delta | 13+ | Đã dedup + validated |
| Silver | `stg_foreign_flow` | Delta | 8 | Đã dedup |
| Gold | `dim_ticker` | Delta (SCD2) | 11 | Dimension |
| Gold | `dim_date` | Delta | 5 | Dimension |
| Gold | `dim_industry` | Delta | 4 | Dimension |
| Gold | `dim_exchange` | Delta | 3 | Dimension |
| Gold | `dim_session` | Delta | 2 | Dimension |
| Gold | `fact_daily_price` | Delta | 12 | Fact |
| Gold | `fact_intraday_trade` | Delta | 9 | Fact |
| Gold | `fact_foreign_flow` | Delta | 7 | Fact |
| Gold | `obt_ticker_daily_performance` | Delta | 15+ | BI Dashboard |
| Gold | `feat_ticker_daily` | Delta | 8 | ML Features |
| Gold | `feat_ticker_unified` | Delta | 7 | ML Features (unified) |
| Gold | `ml_ticker_label` | Delta | 5 | ML Labels |
| Gold | `ml_ticker_training` | Delta | 8+ | ML Training |
| Gold | `agg_feature_health_daily` | Delta | 4 | Monitoring |
| Gold | `feature_drift_alerts` | Delta | 4 | Monitoring alerts |
| Gold | `ops_pipeline_run` | Delta | 8 | Pipeline metadata |

### Data Products cuối cùng
1. **BI Dashboard:** Trino query `obt_ticker_daily_performance` — không cần JOIN
2. **ML Training Dataset:** `ml_ticker_training` — feature + label point-in-time join
3. **Drift Alerts:** `feature_drift_alerts` — PSI > 0.15 tự động cảnh báo
4. **Quality Report:** `data/quality_report.md` — auto-generated hàng ngày
5. **Drift Validation Report:** `data/drift_validation_report.csv` — PSI report

---

## 6. Các bài toán xử lý & cách giải quyết

### 6.1 Skew (Volume Concentration)
**Vấn đề:** VN30 (30/400 tickers) chiếm 80% volume → `groupBy(ticker_id)` gửi phần lớn rows vào vài partition → task skew trong Spark.

**Giải pháp:**
- **AQE Skew Join:** `spark.sql.adaptive.enabled=true`, `spark.sql.adaptive.skewJoin.enabled=true` — Spark tự động detect partition bị skew và split
- **Broadcast Join:** `dim_ticker` chỉ ~400 rows → broadcast join thay vì shuffle join
- **Manual salting:** Dự phòng (documented)

### 6.2 High Cardinality
**Vấn đề:** `countDistinct(ticker_id, trade_date)` trên hàng triệu rows → full shuffle-and-sort.

**Giải pháp:** Dùng `approx_count_distinct` (HyperLogLog) cho quality report; exact count chỉ trên daily-grain tables đã được aggregate.

### 6.3 Schema Evolution
**Vấn đề:** Vendor thay đổi schema giữa chừng — partitions trước 2025-07-01 có 7 cột, partitions sau có 9 cột (thêm `value` và `foreign_room`).

**Giải pháp:**
- Versioned data contracts (`contracts/raw_ohlcv_daily.v1.json` và `.v2.json`)
- Bronze: Phân tách theo `_schema_version`, validate từng version riêng
- Silver: `unionByName(allowMissingColumns=True)`, missing columns → typed NULL
- `delta.schema.autoMerge.enabled = false` — mọi thay đổi schema phải qua contract-versioned path

### 6.4 Duplicates
**Vấn đề:** 2% offline duplicates (same business key), 1.5% streaming re-emit (same event_id)

**Giải pháp:**
- **Offline:** `row_number() OVER (PARTITION BY ticker_id, trade_date ORDER BY _ingested_at DESC)` → keep latest
- **Streaming:** `KeyedProcessFunction` in Flink, dedup by `event_id`, keep latest `created_ts`, state TTL

### 6.5 Burst Handling (Streaming)
**Vấn đề:** 200 events/min → 5,000 events/min trong ATO (09:00-09:15) và ATC (14:30-14:45)

**Giải pháp:**
- Kafka topic 12 partitions (hash by ticker_id)
- Flink parallelism sized cho peak (×25), không phải average
- `AsyncDataStream` cho external enrichment lookups

### 6.6 Late Arrival (Streaming)
**Vấn đề:** 12% events có `event_timestamp` trễ hơn `created_ts` 5-45s

**Giải pháp:**
- Event-time processing (không phải processing-time)
- `BoundedOutOfOrdernessWatermark` = 60s max out-of-orderness
- 60s allowed lateness on windows
- Side output cho quarantine (events trễ quá watermark)

### 6.7 Feature Drift
**Vấn đề:** Phân phối feature thay đổi theo thời gian → model degradation

**Giải pháp:**
- **PSI (Population Stability Index)** so sánh distribution hiện tại vs baseline (30 ngày đầu)
- 10-bin histogram, PSI = Σ(A_i - E_i) × ln(A_i / E_i)
- Alert khi PSI > 0.15 (ngưỡng industry standard)
- 4 mức drift status: baseline (< 0.05), detected (0.05-0.15), strong (0.15-0.20), alert (> 0.20)

### 6.8 Weekend/Holiday Detection
**Vấn đề:** Dữ liệu chỉ có trading days (T2-T6, trừ ngày lễ VN)

**Giải pháp:**
- `generate_trading_calendar()` tự động tính toán lịch giao dịch
- Xử lý cả Tết Dương lịch, Tết Âm lịch, Giỗ Tổ Hùng Vương, 30/4, 1/5, 2/9
- Silver validate không có weekend dates

---

## 7. Tại sao chọn X, không chọn Y?

### Tại sao Delta Lake, không phải Apache Iceberg hay Hudi?
| Tiêu chí | Delta Lake | Iceberg | Hudi |
|----------|-----------|---------|------|
| **Tích hợp Spark** | Native, built-in (delta-spark) | Cần cấu hình catalog riêng | Cần cấu hình riêng |
| **ACID transactions** | Có | Có | Có |
| **Schema evolution** | Tốt (schema auto-merge) | Tốt | Trung bình |
| **Time travel** | Có (version-based) | Có (snapshot-based) | Có |
| **Z-order** | Có (multi-dim clustering) | Có (sorting) | Không native |
| **Hệ sinh thái** | Databricks, Spark native | Netflix, Apple | Uber |

**Lý do chọn:** Delta Lake tích hợp sẵn với Spark (không cần thêm catalog), schema evolution mạnh, Z-order optimization phục vụ BI query, và là lựa chọn phổ biến nhất trong hệ sinh thái Lakehouse hiện nay. Iceberg phù hợp hơn khi dùng nhiều engine (Flink, Trino, Presto), nhưng ở Kỳ 1 project này Spark là engine chính.

### Tại sao MinIO, không phải Local FS hay AWS S3?
| Tiêu chí | MinIO | Local FS | AWS S3 |
|----------|-------|----------|--------|
| **S3-compatible API** | Có | Không | Native |
| **Mô phỏng production** | Có | Không | Production thật |
| **Chi phí** | Free, local | Free, local | Pay-as-you-go |
| **Multi-engine access** | Có (Spark, Flink, Trino) | Khó (file locking) | Có |

**Lý do chọn:** MinIO cung cấp S3-compatible API cho phép Spark, Flink, Trino cùng đọc/ghi qua cùng một giao thức — mô phỏng production lakehouse architecture mà không tốn chi phí cloud. Local FS không hỗ trợ multi-engine concurrent access tốt bằng object storage.

### Tại sao Trino, không phải PostgreSQL/MySQL cho BI?
| Tiêu chí | Trino | PostgreSQL |
|----------|-------|------------|
| **Federated query** | Có (query Delta + PG cùng lúc) | Không (chỉ PG) |
| **Scale** | Distributed (MPP) | Single-node (chủ yếu) |
| **OLAP** | Tối ưu cho analytic | Tối ưu cho OLTP |
| **Connector ecosystem** | Delta Lake, Kafka, nhiều DB | Hạn chế |

**Lý do chọn:** Trino cho phép federated query — analyst có thể JOIN Delta Lake table với PostgreSQL reference table trong một câu SQL. Đây là distributed SQL engine tối ưu cho OLAP workload.

### Tại sao Airflow, không phải Prefect/Dagster?
| Tiêu chí | Airflow | Prefect | Dagster |
|----------|---------|---------|---------|
| **Maturity** | Rất cao (10+ năm) | Trung bình | Thấp hơn |
| **Ecosystem** | Rộng nhất (providers, hooks) | Đang phát triển | Đang phát triển |
| **Community** | Lớn nhất | Nhỏ hơn | Nhỏ hơn |
| **Dynamic DAGs** | Hạn chế | Tốt | Tốt |
| **Asset-based** | Không (task-based) | Không | Có |

**Lý do chọn:** Airflow là industry standard cho orchestration, có ecosystem provider rộng (Spark, Kafka), phù hợp với mô hình batch scheduling của pipeline. Prefect/Dagster mạnh hơn ở dynamic workflow nhưng pipeline này có schedule cố định nên Airflow là lựa chọn tự nhiên.

### Tại sao Flink, không phải Spark Structured Streaming?
| Tiêu chí | Flink | Spark Structured Streaming |
|----------|-------|---------------------------|
| **Processing model** | True streaming (event-by-event) | Micro-batch |
| **Latency** | Sub-second | Seconds |
| **State management** | RocksDB backend, mạnh | Hạn chế hơn |
| **Event-time processing** | Native, rất mạnh | Có nhưng không linh hoạt bằng |
| **Watermark** | Tốt (bounded out-of-orderness) | Có giới hạn |
| **Ecosystem integration** | Cần setup riêng | Native với Spark |

**Lý do chọn:** Flink là true streaming engine (không micro-batch), cần cho xử lý event-time với late arrival và burst handling. Watermark strategy của Flink linh hoạt hơn Spark Structured Streaming. Spark SS phù hợp hơn khi đã có Spark cluster và latency requirements không khắt khe.

### Tại sao uv, không phải pip/poetry?
| Tiêu chí | uv | pip | poetry |
|----------|----|-----|--------|
| **Tốc độ** | Rất nhanh (Rust) | Chậm | Trung bình |
| **Lock file** | Có (cross-platform) | Không native | Có |
| **PEP 621** | Có (pyproject.toml) | Không | Có |
| **Sản phẩm** | Astral (ruff team) | Python Foundation | Cộng đồng |

**Lý do chọn:** uv cực nhanh (viết bằng Rust), cùng team với Ruff, PEP 621-compliant, cross-platform lock file. Python 3.12 requirement đảm bảo tính hiện đại.

### Tại sao Ruff, không phải Black + isort + Flake8?
| Tiêu chí | Ruff | Black + isort + Flake8 |
|----------|------|------------------------|
| **Tốc độ** | Rất nhanh (Rust) | Chậm hơn (Python) |
| **Tính năng** | Linter + Formatter | 3 tools riêng |
| **Rules** | 700+ built-in (E, F, I, N, W, UP, B, C4, SIM) | Flake8 plugins |
| **Cấu hình** | Một file duy nhất | 3-4 files |

**Lý do chọn:** Ruff thay thế cả Black, isort, và Flake8 trong một tool duy nhất, nhanh hơn 10-100x, cấu hình đơn giản trong `pyproject.toml`.

---

## 8. Data Contracts & Schema Evolution

### Khái niệm Data Contract
Data contract là **thỏa thuận giữa producer và consumer** về schema dữ liệu. Ở đây, contract là JSON Schema file mô tả các cột bắt buộc và kiểu dữ liệu.

### Versioned Contracts
File trong `contracts/`:

```json
// raw_ohlcv_daily.v1.json — partitions trước 2025-07-01 (7 cột)
{
  "required": ["ticker_id", "trade_date", "open", "high", "low", "close", "volume"]
}

// raw_ohlcv_daily.v2.json — partitions từ 2025-07-01 (9 cột)
{
  "required": ["ticker_id", "trade_date", "open", "high", "low", "close", "volume", "value", "foreign_room"]
}
```

### Cơ chế enforcement
1. Generator gắn `_schema_version` tag (1 hoặc 2) dựa trên `trade_date` so với `schema_change_date`
2. Bronze đọc Parquet, filter theo `_schema_version`, validate từng version với contract tương ứng
3. Column mới không có trong contract → "contract violation" alert
4. `delta.schema.autoMerge.enabled = false` → mọi schema change phải qua contract path

### Pattern xử lý schema evolution
Đây là **schema-on-read với versioned contract**:
- **Không auto-merge:** Tránh tình huống vendor thêm cột rác tự động propagate
- **Version-aware ingestion:** Mỗi version có pipeline validate riêng
- **Harmonization tại Silver:** `unionByName(allowMissingColumns=True)` + NULL fill

---

## 9. Data Governance & Lineage

### Lineage (DataHub + OpenLineage)

**DP1 — Bronze Ingestion:**
```
[landing-vendor-offline/] ──┐
                             ├──► [bronze_offline_ingest] ──► raw_ohlcv_daily
[vendor_db (PostgreSQL)] ────┘                                raw_foreign_flow
                                                              raw_corporate_actions
                                                              raw_financial_ratios

[Kafka: stock_market_events] ──► [bronze_stream_ingest] ──► raw_market_events
```

**DP2 — Silver + Gold:**
```
raw_ohlcv_daily ──► [silver_daily] ──► stg_daily_price ──► [gold_facts] ──► fact_daily_price
                                                                           fact_foreign_flow
                                                                           obt_ticker_daily_performance
```

**DP3 — Feature Tables:**
```
fact_daily_price ──► [feat_daily_job] ──► feat_ticker_daily
stg_trades ────────► [feat_stream_job] ──► feat_stream_intraday
feat_ticker_daily ─┬──► [feat_unified_job] ──► feat_ticker_unified
feat_stream_intraday┘
```

### Pipeline Run Metadata
Mọi pipeline run đều được ghi vào `ops_pipeline_run` table với: `run_id`, `pipeline`, `start_time`, `end_time`, `status`, `input_rows`, `output_rows`, `error_summary`.

---

## 10. Drift Monitoring & Data Quality

### PSI (Population Stability Index)
**Công thức:** PSI = Σ(A_i - E_i) × ln(A_i / E_i)

Trong đó:
- E_i = phân phối baseline (30 ngày đầu)
- A_i = phân phối hiện tại
- 10 bins histogram
- EPS = 1e-6 để tránh log(0)

**Ngưỡng đánh giá:**
| PSI | Status | Ý nghĩa |
|-----|--------|---------|
| < 0.05 | baseline | Không drift |
| 0.05 - 0.15 | detected | Drift nhẹ |
| 0.15 - 0.20 | strong | Cần chú ý |
| > 0.20 | alert | Cần hành động ngay |

### Các cột được monitor
- `f_ticker_return_5d`
- `f_ticker_volatility_20d`
- `f_ticker_ma20_gap`

### Data Quality Checks (Silver Zone)
Mỗi row được gắn 4 flag:
- `_dq_price_positive`: close > 0
- `_dq_high_ge_max`: high >= max(open, close)
- `_dq_low_le_min`: low <= min(open, close)
- `_dq_volume_nonneg`: volume >= 0

Row vi phạm bị flag nhưng **không bị drop** — downstream có thể quyết định xử lý.

### Quality Report (Auto-generated)
File `data/quality_report.md` được sinh tự động mỗi run, bao gồm:
- Skew Distribution (VN30 volume share, volume by industry)
- Cardinality (unique ticker_id, unique business keys)
- Schema Evolution (row counts per version, null counts)
- Stream Quality (total events, late arrival %, duplicate %)

---

## 11. Tối ưu hóa

### Spark Optimization
| Optimization | Kỹ thuật | Impact |
|-------------|----------|--------|
| Skew Join | AQE Skew Join + Broadcast Join | Task phân phối đều hơn |
| High Cardinality | approx_count_distinct (HyperLogLog) | Tránh full shuffle |
| Schema Evolution | Versioned StructType read, disable autoMerge | Schema an toàn |
| Dedup | row_number() window + keep latest _ingested_at | Loại bỏ duplicate |

### Flink Optimization
| Optimization | Kỹ thuật | Impact |
|-------------|----------|--------|
| Burst Handling | 12 Kafka partitions, peak-sized parallelism | Không backpressure |
| Late Arrival | Watermark 60s, side output quarantine | Không mất dữ liệu muộn |
| Dedup | KeyedProcessFunction + state TTL | Loại bỏ re-emit events |

### Storage Optimization (Delta on MinIO)
| Optimization | Kỹ thuật | Impact |
|-------------|----------|--------|
| Partition Pruning | Partition by trade_date (monthly) | Scan reduction 70-90% |
| Data Skipping | Z-order by ticker_id | File-level skip |
| Small Files | Weekly OPTIMIZE compaction | Merge small files |
| Point Lookup | Bloom filter on dim_ticker.ticker | Index filter |

### Trino Optimization
| Optimization | Kỹ thuật |
|-------------|----------|
| File Skipping | Zone maps (min/max stats) cho trade_date range filter |
| Categorical Filter | Secondary partition on is_vn30 |
| Point Lookup | Bloom filter index |

---

## 12. Testing Strategy

### Unit Tests (tests/unit/)
| Module | Test File | Số test | Nội dung |
|--------|-----------|---------|----------|
| Calendar | test_calendar.py | 9 | Trading day detection, calendar generation, weekend skip, holiday detection |
| Problems | test_problems.py | 12 | Duplicate injection, volume skew, schema version tagging |
| Bronze | test_bronze_ingest.py | 6 | Contract validation, metadata addition, data preservation |
| Silver | test_silver_transform.py | 7 | Dedup logic (keep latest), domain validation flags, edge cases |
| Drift | test_drift_monitor.py | 4 | PSI computation (identical, different, small sample, NaN handling) |

### Integration Tests (tests/integration/)
| Test | Mô tả |
|------|-------|
| test_full_pipeline | End-to-end: Generator → Bronze → Silver → Gold → Drift. 10 tickers, 60 days. Verify tất cả output tables tồn tại, training table có label + features |

### Test Runner
```bash
pytest tests/ -v --cov=generator --cov=jobs --cov-report=term
```

Tests dùng PySpark local[1] mode — không cần cluster thật để chạy.

---

## 13. Câu hỏi vấn đáp thường gặp

### Về kiến trúc

**Q: Tại sao dùng Medallion Architecture (Bronze → Silver → Gold)?**
A: Đây là kiến trúc phổ biến trong Data Lakehouse (Databricks phổ biến). Bronze lưu raw data không transform (append-only, có thể replay), Silver dedup và harmonize, Gold là business-ready (dimensions, facts, features). Mỗi layer phục vụ một mục đích khác nhau và có thể độc lập scale.

**Q: Sự khác nhau giữa batch pipeline (Spark) và streaming pipeline (Flink)?**
A: Batch pipeline (Spark) xử lý dữ liệu daily end-of-day — đọc Parquet từ landing, transform, write Delta. Streaming pipeline (Flink) xử lý real-time events từ Kafka — cần xử lý burst, late arrival, watermark. Cả hai bổ trợ cho nhau: Flink xử lý intraday, Spark xử lý EOD.

**Q: Tại sao cần cả Spark và Flink? Sao không dùng một cái cho tất cả?**
A: Spark tốt cho batch processing khối lượng lớn (EOD aggregation), Flink tốt cho sub-second streaming với event-time semantics. Spark Structured Streaming có thể làm streaming nhưng là micro-batch (latency cao hơn), và watermark handling không linh hoạt bằng Flink.

### Về dữ liệu

**Q: Schema evolution được xử lý như thế nào?**
A: Dùng versioned data contracts (JSON Schema v1/v2). Generator tag `_schema_version` dựa trên `trade_date`. Bronze validate từng version riêng. Silver harmonize bằng `unionByName(allowMissingColumns=True)`, missing columns từ version cũ → typed NULL.

**Q: Làm sao để biết dữ liệu có bị drift?**
A: Monitor PSI (Population Stability Index) cho 3 feature columns (return_5d, volatility_20d, ma20_gap). So sánh distribution hiện tại với baseline 30 ngày đầu tiên. Alert khi PSI > 0.15. Kết quả lưu trong `agg_feature_health_daily` và `feature_drift_alerts`.

**Q: Tại sao có 2 loại duplicate (offline 2%, stream 1.5%)?**
A: Offline duplicate mô phỏng vendor gửi file 2 lần (same business key, same data). Stream duplicate mô phỏng event bị re-emit từ Kafka (same event_id). Cả hai được xử lý khác nhau: offline dùng row_number window, stream dùng KeyedProcessFunction state TTL.

**Q: Tại sao VN30 volume share là 80%?**
A: Phản ánh thực tế thị trường chứng khoán Việt Nam — các mã vốn hóa lớn (VN30) chiếm phần lớn thanh khoản. Con số 80% mô phỏng mức độ tập trung cao, tạo ra skew data problem để pipeline chứng minh khả năng xử lý.

**Q: Late arrival trong streaming là gì và xử lý ra sao?**
A: 12% events có `event_timestamp` (thời điểm thực tế xảy ra) trễ hơn `created_ts` (thời điểm hệ thống nhận được) từ 5-45s. Xử lý bằng event-time processing + `BoundedOutOfOrdernessWatermark` = 60s. Events trễ hơn 60s bị đưa vào quarantine table.

**Q: Trading calendar hoạt động thế nào?**
A: `generate_trading_calendar()` sinh ra danh sách ngày giao dịch dựa trên lịch HOSE: T2-T6, trừ các ngày lễ Việt Nam (Tết Dương lịch, Tết Âm lịch, Giỗ Tổ, 30/4, 1/5, 2/9). Calendar được dùng để validate dữ liệu (không có weekend dates) và sinh streaming events.

### Về SCD2

**Q: SCD Type 2 trong dim_ticker hoạt động thế nào?**
A: Khi một ticker thay đổi thông tin (vd: đổi tên công ty), thay vì update row cũ, ta insert một row mới với:
- `valid_from_ts` = thời điểm thay đổi
- `valid_to_ts` của row cũ = thời điểm thay đổi
- `is_current` của row cũ = false, row mới = true

Điều này cho phép query "ticker này có tên gì vào ngày X" — critical cho backtesting.

### Về Feature Engineering

**Q: Tại sao feature table cần 2 timestamp columns?**
A: `event_timestamp` = thời điểm feature có hiệu lực (as-of time), dùng cho point-in-time join để tránh look-ahead bias. `created_ts` = thời điểm feature được tính toán, dùng để dedup (nếu feature được tính lại nhiều lần, lấy bản mới nhất).

**Q: Label được tạo như thế nào?**
A: Label = 1 nếu `adj_close` sau 3 ngày tăng >1% so với hiện tại. Dùng `LEAD(adj_close, 3)` window function. Đây là binary classification problem: dự đoán giá có tăng >1% trong 3 ngày tới không.

**Q: Tại sao dùng OBT (One Big Table) cho BI?**
A: OBT là bảng denormalized chứa tất cả columns cần cho dashboard — analyst query không cần JOIN. Đánh đổi: storage redundancy (data được copy), nhưng query performance tăng đáng kể. Phù hợp cho BI dashboard có pattern query cố định.

### Về vận hành

**Q: Pipeline được orchestrate như thế nào?**
A: Airflow DAGs với schedule cố định: bronze_offline_ingest (15:30) → silver_daily (16:00) → gold_dimensions_and_facts (16:30) → feat_daily_job (17:00). Mỗi DAG retry 3 lần với 5-10 phút delay. Flink pipeline chạy continuous (không schedule).

**Q: Làm sao để reproduce kết quả?**
A: Generator dùng `numpy.random.default_rng(42)` — tất cả dữ liệu được sinh từ seed cố định. Trading calendar cũng deterministic. Chỉ cần dùng cùng config và seed là ra cùng dữ liệu.

**Q: Làm sao để monitor pipeline health?**
A: 3 cơ chế: (1) `ops_pipeline_run` table ghi metadata mọi run, (2) `agg_feature_health_daily` monitor feature drift, (3) Quality report auto-generated mỗi ngày.

---

## Phụ lục: Project Structure
```
vnstock-data-pipeline/
├── README.md                     # Project overview
├── docker-compose.yml            # 10 services
├── Dockerfile                    # Generator image
├── Makefile                      # make up/down/test/lint/generate
├── pyproject.toml                # Python config + deps
├── requirements.txt              # pinned deps
│
├── config/
│   ├── generator.yaml            # 50 tickers, 30 days, burst ×25, seed=42
│   └── drift.yaml                # volatility regime shift (×2.1 sigma)
│
├── contracts/                    # Versioned JSON schemas
│   ├── raw_ohlcv_daily.v1.json   # 7 cols (pre-2025-07-01)
│   └── raw_ohlcv_daily.v2.json   # 9 cols (post-2025-07-01)
│
├── generator/                    # Data Generator
│   ├── main.py                   # CLI: --mode offline|stream|all
│   ├── generators.py             # PriceGen, ForeignFlowGen, EventGen
│   ├── problems.py               # Duplicate, skew, schema tag injection
│   ├── calendar.py               # HOSE trading calendar
│   ├── writers.py                # Parquet, Kafka, PostgreSQL, quality report
│   └── seed/                     # Frozen tickers_reference.json
│
├── dags/                         # Airflow DAGs
│   ├── bronze_offline_ingest.py  # 15:30 daily
│   ├── silver_daily.py           # 16:00 daily
│   ├── gold_dimensions_and_facts.py # 16:30 daily
│   └── feat_daily_job.py         # 17:00 daily
│
├── jobs/                         # PySpark / Flink implementations
│   ├── bronze_ingest.py          # Landing → Bronze (validate + metadata)
│   ├── silver_transform.py       # Dedup + domain check + schema harmonize
│   ├── gold_model.py             # SCD2, facts, OBT, features, labels
│   └── drift_monitor.py          # PSI monitoring + training table
│
├── docker/
│   ├── airflow/Dockerfile        # Custom Airflow image
│   ├── spark/Dockerfile          # Custom Spark image
│   └── trino/catalog/            # Delta + PostgreSQL connectors
│
├── scripts/init_vendor_db.sql    # tickers + corporate_actions tables
│
├── docs/                         # Chi tiết từng khía cạnh
│   ├── schema_design.md
│   ├── generator.md
│   ├── spark_optimization.md
│   ├── flink_optimization.md
│   ├── storage_optimization.md
│   └── data_governance.md
│
└── tests/
    ├── unit/                     # 38+ unit tests
    └── integration/              # Full pipeline E2E test
```
