# Vietnam Stock Market Data Pipeline

End-to-end data engineering project simulating the **Vietnamese stock market**
(HOSE, HNX, UPCOM). It produces deterministic offline (Parquet) and streaming
(Kafka) data with intentionally injected real-world problems — skew, high
cardinality, schema evolution, duplicates, burst, late arrivals, and feature
drift — then ingests, cleans, models, and monitors the data through a full
Bronze → Silver → Gold pipeline with feature engineering for downstream ML.

**Scope:** Kỳ 1 (M1–M12) — Linux, Python, Database, WebAPI, Validation,
Containerization, Kafka, Delta Lake/MinIO, Spark, Flink, Dimensional Modeling,
Orchestration (Airflow), Data Governance (DataHub).

## Table of Contents

- [Business Domain](#business-domain)
- [Architecture](#architecture)
  - [Deployable Units](#deployable-units)
  - [Data Flow Diagram](#data-flow-diagram)
  - [Numbered Data Flows](#numbered-data-flows)
- [Data Layers & Naming](#data-layers--naming)
- [Injected Data Problems](#injected-data-problems)
- [Repository Structure](#repository-structure)
- [Quick Start](#quick-start)
- [Pipelines](#pipelines)
- [Documentation & Evidence](#documentation--evidence)

## Business Domain

The project models the **Vietnam stock market** across three exchanges:

| Exchange | Price Limit | Characteristics |
|----------|-------------|-----------------|
| **HOSE** (Sở Giao dịch Chứng khoán TP.HCM) | ±7% | Main board, ~400 tickers, includes VN30 large-cap index |
| **HNX** (Sở Giao dịch Chứng khoán Hà Nội) | ±10% | Mid-cap board |
| **UPCOM** (Unlisted Public Company Market) | ±15% | Small-cap / unlisted public companies |

### Data Sources

| Dataset | Format | Grain | Source |
|---------|--------|-------|--------|
| OHLCV daily prices | Parquet (v1/v2 schema) | ticker × trading day | Simulated vendor file drop |
| Foreign flow daily | Parquet | ticker × trading day | Simulated vendor file drop |
| Corporate actions | PostgreSQL `vendor_db` | one per action | Simulated reference system |
| Financial ratios | Parquet | ticker × quarter | Simulated vendor file drop |
| Ticker reference | PostgreSQL `vendor_db` + seed JSON | one per ticker | Real `vnstock` snapshot + synthetic |
| Intraday trades/quotes/index | Kafka topic `stock_market_events_v3` | event | Streaming generator |

### Trading Calendar

Data only exists on **trading days** (Monday–Friday, excluding Vietnamese public
holidays: Tết, Hùng Kings, 30/4–1/5, 2/9). Non-trading days produce no rows —
downstream pipelines treat calendar gaps as expected, not as data quality errors.

### Generation Strategy

**Hybrid "real seed, synthetic data":** real reference data (ticker list, ICB
industry classification, price ranges) is collected once from the public
`vnstock` library and frozen as a static seed. All records are then
**synthetically generated** with configurable data problems — giving full control
over injected issues while keeping the domain realistic. Deterministic seed
(`random_seed: 42`) ensures every run reproduces identical data.

## Architecture

### Deployable Units

Every box in the diagram below is a runnable container, cluster, or external
tool. Delta Lake is **not** drawn as a standalone service — it is a storage
format whose files live in the shared `./data` mount and the MinIO `delta-lake`
bucket.

| ID | Deployable Unit | Runtime | Endpoint | Responsibility |
|----|-----------------|---------|----------|----------------|
| `GEN` | Generator | On-demand container | — | Produces deterministic Parquet, PostgreSQL rows, and Kafka/JSONL events |
| `KAFKA` | Kafka Broker | Container | `:29092` / host `:9092` | 4-partition `stock_market_events_v3` event transport |
| `ZK` | ZooKeeper | Container | `:2181` | Kafka broker coordination |
| `PG` | PostgreSQL 16 | Container | Host `:5433` | `vendor_db` reference tables + Airflow metadata |
| `AF` | Airflow | Webserver + Scheduler containers | Host `:8082` | Orchestrates DP1–DP3; launches Spark, monitors Flink |
| `SPARK` | Spark Cluster | Master + Worker containers | UI `:8080`, RPC `:7077` | Offline Bronze/Silver/Gold/Feature batch transforms |
| `FLINK` | Flink Cluster | JobManager + TaskManager containers | UI `:8081` | Event-time dedup, watermarking, quarantine, 5-min windows |
| `VOL` | Shared Data Volume | Host bind mount `./data` | — | Landing, Delta tables (Bronze/Silver/Gold), Flink checkpoints |
| `SYNC` | Gold Sync Tool | One-shot `minio/mc` container | — | Mirrors local Gold Delta → MinIO for Trino |
| `MINIO` | MinIO Object Storage | Container | API `:9000`, UI `:9001` | Vendor landing (`landing-vendor-offline`) + `delta-lake` bucket |
| `HIVE` | Hive Metastore | Container | Thrift `:9083` | Table/location metadata for Trino Delta catalog |
| `TRINO` | Trino Query Engine | Container | Host `:8083` | Federated queries across Delta (MinIO) + PostgreSQL |
| `DBEAVER` | DBeaver | External desktop client | — | Schema inspection, Trino SQL, PostgreSQL `EXPLAIN` |
| `DHCLI` | DataHub CLI | One-shot host process | — | Reads `lineage.yml`, publishes dataset lineage |
| `DATAHUB` | DataHub Quickstart | Separate deployment | UI `:9002`, GMS `:8084` | Displays DP1–DP3 dataset lineage |

### Data Flow Diagram

Solid arrows carry data records/files. Dashed gray arrows are restricted to
orchestration/coordination (control plane, not data plane). Arrow prefixes
identify independent flows: **B** (batch), **S** (streaming), **C**
(consumption), **G** (governance), **O** (operations).

## Data Layers & Naming

| Layer | Storage | Prefix | Contents |
|-------|---------|--------|----------|
| **Landing** | MinIO `landing-vendor-offline/` + PostgreSQL `vendor_db` | — | Raw vendor files and reference tables (external boundary) |
| **Bronze** | Delta Lake on `./data/bronze` | `raw_` | Raw ingested data with schema version tags and ingest metadata |
| **Silver** | Delta Lake on `./data/silver` | `stg_` | Deduplicated, type-casted, schema-harmonized data |
| **Gold** | Delta Lake on `./data/gold` + MinIO mirror + Trino | `dim_`, `fact_`, `obt_`, `feat_`, `ml_` | Business-ready dimensions, facts, OBT, feature, and label tables |

### Key Gold Tables

| Table | Grain | Type | Notes |
|-------|-------|------|-------|
| `dim_ticker` | One per ticker version | SCD Type 2 | `valid_from_ts`, `valid_to_ts`, `is_current` |
| `dim_date` | One per calendar date | Static dim | `is_trading_day`, `holiday_name` |
| `dim_industry` | One per ICB node | Static dim | ICB level 1–2 classification |
| `dim_exchange` | One per exchange | Static dim | HOSE/HNX/UPCOM with price limit |
| `fact_daily_price` | ticker × trading day | Fact | adj_close with versioned adjustment factors |
| `fact_intraday_trade` | One per matched trade | Fact | Built from deduped stream events |
| `fact_foreign_flow` | ticker × trading day | Fact | Foreign buy/sell volume and value |
| `obt_ticker_daily_performance` | ticker × trading day | OBT | Denormalized BI dashboard table |
| `feat_ticker_daily` | ticker × end-of-day | Feature | Return, volatility, MA gap, foreign net ratio |
| `feat_stream_intraday` | ticker × window end | Feature | Volume, trade count, momentum, burst flag |
| `feat_ticker_unified` | ticker × event_timestamp | Feature | Point-in-time join of daily + intraday features |
| `ml_ticker_label` | ticker × event_timestamp | Label | `price_up_next_3d` with structural 3-day delay |
| `ml_ticker_training` | ticker × event_timestamp | Training | Label + unified features, leakage-checked |

## Injected Data Problems

All problems are reproducible (deterministic seed 42).

| Problem | Offline | Streaming | Detail |
|---------|---------|-----------|--------|
| **Skew** | Yes | — | 80% volume in 30 VN30 tickers; banking + real estate = 60% value |
| **High Cardinality** | Yes | — | ~72K ticker×date composite keys; millions of unique trade_ids |
| **Schema Evolution** | Yes | — | Partitions before 2025-07-01 missing `foreign_room` and `value` columns |
| **Duplicates** | 2% | 1.5% | Offline: same (ticker_id, trade_date, OHLCV); Stream: same `event_id` re-emitted |
| **Burst** | — | Yes | ×25 (200 → 5,000 events/min) during ATO (09:00–09:15) and ATC (14:30–14:45) |
| **Late Arrivals** | — | 12% | Events 5–45s late, clustered in burst windows |
| **Feature Drift** | Yes | — | Volatility regime shift: σ 1.2% → 2.5% after 2025-09-01 (Scenario A) |

## Repository Structure

```
.
├── config/                          Generator and pipeline configuration
│   └── generator.yaml               Universe size, data problems, drift, seed
├── contracts/                       Versioned JSON data contracts (per schema version)
│   ├── raw_ohlcv_daily.v1.json      Bronze v1 contract (before 2025-07-01)
│   ├── raw_ohlcv_daily.v2.json      Bronze v2 contract (after 2025-07-01)
│   ├── raw_market_events.v1.json    Streaming event contract
│   ├── stg_trades.v1.json           Silver deduped trades contract
│   ├── stg_quotes.v1.json           Silver deduped quotes contract
│   ├── stg_events_quarantine.v1.json Quarantine table contract
│   ├── feat_stream_intraday.v1.json Streaming feature contract
│   ├── feat_ticker_daily.v1.json    Daily feature contract
│   ├── dim_ticker.v1.json           SCD2 dimension contract
│   └── fact_daily_price.v1.json     Daily fact contract
├── dags/                            Airflow DAG definitions (orchestration)
│   ├── bronze_offline_ingest.py     DP1: landing → raw_* Bronze tables
│   ├── bronze_stream_ingest.py      DP1: Kafka → raw_market_events Bronze
│   ├── silver_daily.py              DP2: raw_* → stg_* Silver (dedup, harmonize)
│   ├── silver_stream.py             DP2: raw stream → stg_trades/quotes Silver
│   ├── gold_dimensions_and_facts.py DP2: stg_* → dim_*, fact_*, obt_* Gold
│   ├── feat_daily_job.py            DP3: fact_* → feat_ticker_daily
│   ├── feat_stream_job.py           DP3: stg_trades → feat_stream_intraday
│   ├── delta_maintenance.py         Weekly Delta OPTIMIZE compaction
│   └── flink_silver_stream.py       Flink job submission wrapper
├── datahub/                         Data governance
│   ├── lineage.yml                  Dataset-level lineage edges (DP1→DP2→DP3)
│   └── recipe.yml                   DataHub ingestion recipe
├── docker/                          Custom Docker images and service configs
│   ├── airflow/Dockerfile           Airflow with PySpark + Delta + project deps
│   ├── spark/Dockerfile             Spark 3.5.3 with Delta Lake + Hadoop AWS
│   ├── flink/Dockerfile             PyFlink with Kafka connector
│   ├── trino/catalog/delta.properties Trino Delta Lake catalog config
│   └── hive/hive-site.xml           Hive Metastore S3A configuration
├── docs/                            Design docs and rubric evidence
│   ├── generator.md                 Generation model, config, data problems
│   ├── schema_design.md             All zones, keys, SCD2, feature time semantics
│   ├── processing_jobs.md           Spark/Flink baseline and optimization write-ups
│   ├── storage_optimization.md      Delta and PostgreSQL storage optimization
│   ├── orchestration_governance.md  DP1–DP3 pipelines, contracts, lineage
│   ├── docker.md                    Docker deployment and image optimization
│   ├── novel_ideas.md               Trino federation + versioned contracts
│   ├── rubric_evidence.md           Rubric-to-proof matrix and evidence inventory
│   ├── evidence/                    Screenshot evidence and quality reports
│   └── images/                      Architecture and diagram images
├── generator/                       Deterministic data generator
│   ├── main.py                      CLI entry point (--mode offline|stream)
│   ├── generators.py                Price (random walk), volume, event generators
│   ├── problems.py                  Duplicate, late arrival, burst, drift injection
│   ├── calendar.py                  HOSE trading calendar with VN holidays
│   ├── writers.py                   MinIO, PostgreSQL, Kafka, Parquet/JSONL writers
│   ├── seed/                        Frozen real-world ticker reference data
│   │   ├── fetch.py                 One-time vnstock API fetcher
│   │   └── tickers_reference.json   Committed seed file (ICB, exchange, listing date)
│   └── __init__.py                  Package init
├── jobs/                            Processing job modules (run by Spark/Flink)
│   ├── spark_session.py             Shared Spark session builder (Delta, AQE, MinIO)
│   ├── bronze/
│   │   ├── offline.py               Landing → raw_* ingestion with contract validation
│   │   └── stream.py                Kafka → raw_market_events append-only ingest
│   ├── silver/
│   │   ├── daily.py                 Dedup, type cast, schema harmonize → stg_*
│   │   └── stream.py                Dedup, watermark, quarantine → stg_trades/quotes
│   ├── gold/
│   │   ├── dimensions.py            SCD2 dim_ticker merge + static dims
│   │   ├── facts.py                 adj_close with versioned factors, fact tables
│   │   ├── obt.py                   Denormalized obt_ticker_daily_performance
│   │   ├── features.py              feat_ticker_daily (trading-day windows)
│   │   ├── labels.py                ml_ticker_label (price_up_next_3d)
│   │   ├── drift.py                 PSI monitoring, agg_feature_health_daily
│   │   ├── maintenance.py           Delta compaction and cleanup
│   │   └── metadata.py              Pipeline run metadata
│   ├── features/
│   │   └── stream.py                feat_stream_intraday (rolling windows)
│   └── flink/
│       └── silver_stream.py         PyFlink event-time app (dedup + windows)
├── scripts/                         Utility and setup scripts
│   ├── init_vendor_db.sql           PostgreSQL schema for vendor_db
│   ├── run_pipeline.py              Local pipeline runner for testing
│   ├── spark_ui_capture.py          Spark UI evidence capture driver
│   └── trino_examples.sql           Example Trino federated queries
├── tests/                           Test suite
│   ├── unit/
│   │   ├── test_bronze_ingest.py    Bronze ingestion and contract validation
│   │   ├── test_silver_transform.py Silver dedup, schema evolution handling
│   │   ├── test_drift_monitor.py    PSI computation and alert logic
│   │   └── test_problems.py         Data problem injection verification
│   └── integration/
│       └── test_pipeline.py         End-to-end pipeline integration tests
├── Dockerfile                       Generator container (multi-stage)
├── docker-compose.yml               Full stack: 16 services
├── Makefile                         Build, run, test, lint, and evidence targets
├── pyproject.toml                   Python project config, dependencies, tool settings
├── requirements.txt                 Pinned dependency list
└── README.md                        This file
```

## Quick Start

**Prerequisites:** Docker Compose, Make, Python 3.10+, 24+ GB RAM.

```bash
cp .env.example .env
docker compose build
docker compose up -d

# Initialize Airflow metadata database on first run
make airflow-init

# Generate vendor history and one streaming trading day
docker compose --profile generator run --rm generator \
  --mode offline --config config/generator.yaml
docker compose --profile generator run --rm generator \
  --mode stream --config config/generator.yaml

# Submit the continuous Flink Silver application
docker compose exec -T flink-jobmanager flink run -d \
  -py /opt/project/jobs/flink/silver_stream.py \
  --kafka-broker kafka:29092 \
  --topic stock_market_events_v3 \
  --group-id flink-silver-stream-v1 \
  --output-dir /opt/flink/data/current
```

**Useful endpoints:**

| UI | URL |
|----|-----|
| Spark Master | http://localhost:8080 |
| Spark Driver (capture) | http://localhost:4040 |
| Flink Dashboard | http://localhost:8081 |
| Airflow | http://localhost:8082 |
| Trino | http://localhost:8083 |
| MinIO Console | http://localhost:9001 |

**Run checks:**

```bash
make lint                    # ruff format check + lint
make test                    # pytest with coverage
docker compose exec -T airflow-scheduler airflow dags list-import-errors
docker compose config --quiet
```

## Pipelines

| Pipeline | DAG | Input → Output | Key Quality Gates |
|----------|-----|----------------|-------------------|
| **DP1** — Bronze Ingest | `bronze_offline_ingest` | Landing Parquet + PostgreSQL → `raw_ohlcv_daily`, `raw_foreign_flow`, `raw_corporate_actions`, `raw_financial_ratios` | v1/v2 contract validation, required fields, domain checks, row counts |
| **DP1** — Bronze Stream | `bronze_stream_ingest` | Kafka `stock_market_events_v3` → `raw_market_events` | Schema check, offset tracking |
| **DP2** — Silver Daily | `silver_daily` | `raw_*` → `stg_daily_price`, `stg_foreign_flow`, `stg_corporate_actions` | Dedup (ticker_id, trade_date), type casting, schema harmonization, calendar validation |
| **DP2** — Silver Stream | `silver_stream` / Flink | `raw_market_events` → `stg_trades`, `stg_quotes`, `stg_events_quarantine` | Dedup by event_id, 60s watermark, late-event quarantine |
| **DP2** — Gold | `gold_dimensions_and_facts` | `stg_*` → `dim_*`, `fact_*`, `obt_*` | SCD2 merge, adj_close versioning, referential integrity, OHLC domain checks |
| **DP3** — Features | `feat_daily_job` | `fact_*` + `dim_date` → `feat_ticker_daily` | Trading-day window correctness, event_timestamp + created_ts |
| **DP3** — Stream Features | `feat_stream_job` / Flink | `stg_trades` → `feat_stream_intraday` | Rolling window integrity, session-gap awareness |
| **Maintenance** | `delta_maintenance` | Delta tables → compacted Delta tables | Weekly `OPTIMIZE` with Z-order |

### Pipeline Update Strategy

| Layer | Strategy | Idempotency |
|-------|----------|-------------|
| Bronze | Append-only with `_ingested_at`, `batch_id`, `source_offset` | Append is naturally idempotent |
| Silver | Incremental dedup by business key + event time | Partition overwrite by trade_date |
| Gold | Incremental merge/upsert on stable keys | Merge on (ticker_id, trade_date) or (trade_id) |
| Features | Rolling window recompute + merge | Merge on (ticker_id, event_timestamp), keep latest `created_ts` |

## Documentation & Evidence

| Document | Content |
|----------|---------|
| [`docs/generator.md`](docs/generator.md) | Generation model, configuration reference, data problems, quality report |
| [`docs/schema_design.md`](docs/schema_design.md) | All zones (Bronze → Gold), table designs, SCD2, feature time semantics, point-in-time correctness |
| [`docs/processing_jobs.md`](docs/processing_jobs.md) | Spark optimization (AQE, broadcast, salting, schema handling) and Flink optimization (watermark, partitioning, async I/O, windows) |
| [`docs/storage_optimization.md`](docs/storage_optimization.md) | Delta compaction, Z-order, partitioning; Trino bloom filter, zone maps, secondary partitions |
| [`docs/orchestration_governance.md`](docs/orchestration_governance.md) | DP1–DP3 pipeline design, Airflow DAGs, data contracts, DataHub lineage |
| [`docs/docker.md`](docs/docker.md) | Multi-stage Dockerfiles, image size optimization, stack deployment |
| [`docs/novel_ideas.md`](docs/novel_ideas.md) | Trino federated queries across Delta + PostgreSQL; versioned data contracts |
| [`docs/rubric_evidence.md`](docs/rubric_evidence.md) | Rubric-to-evidence matrix, screenshot inventory, known gaps |

### Reference Dataset (seed 42)

| Characteristic | Value |
|----------------|-------|
| Tickers | 400 |
| Trading days | 180 |
| Unique ticker/date keys | 72,000 |
| Offline rows (with 2% duplicates) | 73,440 |
| Schema split | 60% v1 / 40% v2 |
| VN30 volume share | ~80% |
| Streaming records | 197,925 |
| Streaming duplicates | 1.5% |
| Auction peak | ~5,000 events/min |
