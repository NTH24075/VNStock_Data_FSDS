# Vietnam Stock Market Data Pipeline

Business domain: mô phỏng thị trường chứng khoán Việt Nam (HOSE-centric) — dữ liệu giá, khối lượng, foreign flow, corporate actions, chỉ số — phục vụ pipeline Bronze → Silver → Gold cho market analytics và ML.

## Table of Contents

- [High-Level System Deployment](#high-level-system-deployment)
- [Repo Structure](#repo-structure)
- [Quick Start](#quick-start)
- [Data Characteristics](#data-characteristics)
- [Pipeline Overview](#pipeline-overview)
- [Documentation](#documentation)

## High-Level System Deployment

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

**Data flow (numbered arrows):**
1. Generator writes offline Parquet → MinIO `landing-vendor-offline/`
2. Generator publishes JSON events → Kafka `stock_market_events` + vendor_db (PG)
3. Flink consumes Kafka → Bronze Delta (append-only with offset metadata)
4. Airflow triggers Spark: pull from vendor_db (JDBC) → Bronze Delta
5. Airflow triggers Spark: pull Parquet from landing → Bronze Delta
6. Flink dedup + watermark + quarantine → Silver Delta
7. Airflow triggers Spark: dedup + schema harmonization → Silver Delta
8. Airflow triggers Spark: aggregate + SCD2 → Gold Delta (dims, facts, OBT)
9. Airflow triggers Spark + Flink: feature windows → Gold feature tables
10. Trino queries Gold Delta tables for BI

## Repo Structure

```
vnstock-data-pipeline/
├── README.md                          # This file — project overview
├── docker-compose.yml                 # 10 services: PG, MinIO, Kafka, Spark, Flink, Airflow, Trino
├── Makefile                           # up/down/build/test/lint/generate targets
├── pyproject.toml                     # Python package config
├── requirements.txt                   # Runtime deps
├── requirements-dev.txt               # Dev deps (pytest, ruff)
│
├── config/
│   ├── generator.yaml                 # Generator parameters (tickers, price model, data problems)
│   └── drift.yaml                     # Drift simulation parameters (volatility regime shift)
│
├── contracts/                         # Versioned JSON schemas for Bronze data validation
│   ├── raw_ohlcv_daily.v1.json        # Schema before 2025-07-01 (7 columns)
│   └── raw_ohlcv_daily.v2.json        # Schema after 2025-07-01 (9 columns, adds foreign_room + value)
│
├── generator/                         # 01 — Data Generator
│   ├── main.py                        # CLI entry: --mode offline|stream|all
│   ├── models/                        # Dataclasses: Ticker, OHLCRow, MarketEvent, CorporateAction
│   ├── generators/                    # PriceGen, ForeignFlowGen, EventGen, DataProblemInjector
│   └── seed/                          # Frozen vnstock tickers_reference.json for reproducibility
│
├── dags/                              # Airflow DAGs
│   ├── bronze_offline_ingest.py       # DP1: landing → Bronze Delta (ingest + validate)
│   ├── silver_daily.py                # DP2: Bronze → Silver Delta (dedup + schema harmonization + validate)
│   ├── gold_dimensions_and_facts.py   # DP2: Silver → Gold Delta (SCD2, facts, OBT)
│   └── feat_daily_job.py             # DP3: Gold feature tables + drift monitoring + labels
│
├── jobs/                              # Spark / Flink job implementations
│   ├── bronze/                        # Landing → Bronze (schema validate, append Delta)
│   ├── silver/                        # Dedup, watermark, quarantine, schema harmonization
│   ├── gold/                          # SCD2 dims, facts, OBT, labels, drift monitoring
│   └── features/                      # feat_ticker_daily, feat_stream_intraday, feat_ticker_unified
│
├── docker/trino/catalog/              # Trino connector configs (Delta Lake + PostgreSQL)
├── scripts/init_vendor_db.sql         # PostgreSQL schema (tickers + corporate_actions)
│
├── docs/                              # Detailed documentation (linked from this README)
│   ├── schema_design.md               # All-zone table designs, SCD2, feature store
│   ├── generator.md                   # Generator design, config, data problems, quality report
│   ├── spark_optimization.md          # Spark UI before/after, AQE skew join, broadcast, schema evolution
│   ├── flink_optimization.md          # Flink UI before/after, watermark, window processing
│   ├── storage_optimization.md        # Lakehouse (compaction, Z-order) + warehouse (indexing)
│   ├── data_governance.md             # DataHub lineage + data contracts
│   └── images/                        # Screenshots for evidence
│
└── tests/                             # pytest unit + integration tests
    ├── unit/                          # Per-module tests
    ├── integration/                   # Pipeline integration tests
    └── fixtures/                      # Sample Parquet + JSONL test data
```

## Quick Start

```bash
# 1. Start infrastructure
make up

# 2. Initialize Airflow (first time only)
make airflow-init

# 3. Generate data
make generate-all

# 4. Run tests
make test

# 5. View services
# MinIO console:    http://localhost:9001
# Spark master UI:  http://localhost:8080
# Flink dashboard:  http://localhost:8081
# Airflow UI:       http://localhost:8082  (admin/admin)
# Trino UI:         http://localhost:8083
```

## Data Characteristics

| Dimension | Offline | Streaming |
|-----------|---------|-----------|
| **Volume** | ~400 tickers × 180 trading days × 5 tables ≈ 100K rows | ~150K–200K events/trading day |
| **Velocity** | Daily end-of-day drop (15:00) | 200 events/min baseline, ×25 burst (ATO/ATC) |
| **Key** | (ticker_id, trade_date) per table | event_id (UUID) per event |
| **Problems injected** | 80% volume skew (VN30), schema evolution (v1→v2), 2% duplicates | ×25 burst, 12% late (5–45s), 1.5% duplicates |

## Pipeline Overview

| Pipeline | Tool | Schedule | Input | Output |
|----------|------|----------|-------|--------|
| Bronze offline | Airflow + Spark | Daily 15:30 | MinIO landing + PG vendor_db | Delta `raw_*` tables |
| Bronze stream | Flink | Continuous | Kafka `stock_market_events` | Delta `raw_market_events` |
| Silver daily | Airflow + Spark | Daily 16:00 | Bronze Delta | Delta `stg_*` tables |
| Silver stream | Flink | Continuous | Bronze Delta | Delta `stg_trades`, `stg_quotes` |
| Gold dims/facts/OBT | Airflow + Spark | Daily 16:30 | Silver Delta | Delta `dim_*`, `fact_*`, `obt_*` |
| Feature + drift + labels | Airflow + Spark/Flink | Daily 17:00 | Gold Delta | Delta `feat_*`, `agg_feature_health_daily`, `ml_ticker_label` |

## Documentation

Detailed design and optimization documents:

- [Schema Design (all zones)](docs/schema_design.md) — table catalog, SCD2, feature store, OBT
- [Data Generator](docs/generator.md) — price model, data problems, quality report, config
- [Spark Optimization](docs/spark_optimization.md) — AQE skew join, broadcast, schema evolution handling
- [Flink Optimization](docs/flink_optimization.md) — watermark strategy, window processing, burst handling
- [Storage Optimization](docs/storage_optimization.md) — Delta compaction/Z-order, Trino indexing
- [Data Governance](docs/data_governance.md) — DataHub lineage, data contracts
