# VNStock Data Pipeline — Tài Liệu Thuyết Trình & Chứng Minh Rubric

> **Tổng điểm rubric:** 100 điểm | **Phạm vi:** Kỳ 1 (M1–M12) | **Seed:** 42 | **Ngày xác nhận:** 2026-07-23

---

## Mục Lục

1. [Tổng Quan Dự Án](#1-tổng-quan-dự-án)
2. [README & Architecture Diagram](#2-readme--architecture-diagram)
3. [Docker & Docker Compose](#3-docker--docker-compose)
4. [Data Generator — Offline Problems](#4-data-generator--offline-problems)
5. [Data Generator — Streaming Problems](#5-data-generator--streaming-problems)
6. [Processing Jobs — Spark](#6-processing-jobs--spark)
7. [Processing Jobs — Flink](#7-processing-jobs--flink)
8. [Data Storage Optimization](#8-data-storage-optimization)
9. [Data Pipeline Orchestration](#9-data-pipeline-orchestration)
10. [Data Governance](#10-data-governance)
11. [Schema Design & Documentation](#11-schema-design--documentation)
12. [Novel Ideas](#12-novel-ideas)
13. [Bảng Tổng Hợp Evidence Gaps](#13-bảng-tổng-hợp-evidence-gaps)

---

## 1. Tổng Quan Dự Án

### 1.1 Business Domain

Dự án mô phỏng **thị trường chứng khoán Việt Nam** qua 3 sàn giao dịch:

| Sàn | Biên độ giá | Đặc điểm |
|-----|------------|----------|
| **HOSE** (Sở Giao dịch Chứng khoán TP.HCM) | ±7% | Sàn chính, ~400 mã, bao gồm VN30 |
| **HNX** (Sở Giao dịch Chứng khoán Hà Nội) | ±10% | Sàn mid-cap |
| **UPCOM** (Unlisted Public Company Market) | ±15% | Sàn small-cap / doanh nghiệp chưa niêm yết |

### 1.2 Chiến Lược Sinh Dữ Liệu: "Real Seed, Synthetic Data"

```
Dữ liệu tham chiếu thật (ticker list, ICB, giá) → Frozen Seed → Generator (seed=42)
                                                                    ↓
                                          Dữ liệu tổng hợp với các vấn đề được kiểm soát
```

- Dữ liệu tham chiếu thu thập 1 lần từ thư viện `vnstock` public và đóng băng (`generator/seed/tickers_reference.json`)
- Tất cả records được sinh tổng hợp với các data problems cấu hình được
- **Deterministic seed 42** — mọi lần chạy tạo ra dữ liệu giống hệt nhau

### 1.3 Dataset Tham Chiếu (Seed 42)

| Đặc điểm | Giá trị |
|-----------|---------|
| Số mã chứng khoán | 400 (30 VN30) |
| Số ngày giao dịch | 180 |
| Unique ticker × date keys | 72,000 |
| Offline rows (với 2% duplicates) | 73,440 |
| Schema phân bố | 60% v1 / 40% v2 |
| VN30 volume share | ~80% |
| Streaming records | 197,925 |
| Streaming duplicates | 1.5% |

### 1.4 Nguồn Dữ Liệu

| Dataset | Định dạng | Grain | Nguồn mô phỏng |
|---------|-----------|-------|----------------|
| OHLCV daily | Parquet (v1/v2 schema) | ticker × trading day | Vendor file drop (MinIO) |
| Foreign flow daily | Parquet | ticker × trading day | Vendor file drop (MinIO) |
| Corporate actions | PostgreSQL `vendor_db` | one per action | Reference DB |
| Financial ratios | Parquet | ticker × quarter | Vendor file drop (MinIO) |
| Ticker reference | PostgreSQL + seed JSON | one per ticker | Real `vnstock` snapshot + synthetic |
| Intraday events | Kafka `stock_market_events_v3` | event | Streaming generator |

### 1.5 Các Vấn Đề Dữ Liệu Được Inject

| Vấn đề | Offline | Streaming | Chi tiết |
|---------|---------|-----------|----------|
| **Skew** | ✓ | — | 80% volume trong 30 mã VN30; banking + real estate = 60% value |
| **High Cardinality** | ✓ | — | ~72K composite keys; hàng triệu trade_id unique |
| **Schema Evolution** | ✓ | — | Partition trước 2025-07-01 thiếu cột `foreign_room` và `value` |
| **Duplicates** | 2% | 1.5% | Offline: (ticker_id, trade_date); Stream: event_id re-emitted |
| **Burst** | — | ✓ | ×25 (200 → 5,000 events/min) trong ATO (09:00–09:15) và ATC (14:30–14:45) |
| **Late Arrivals** | — | 12% | Events trễ 5–45s, tập trung trong burst windows |
| **Feature Drift** | ✓ | — | Volatility regime shift: σ 1.2% → 2.5% sau 2025-09-01 |

### 1.6 Kiến Trúc Hệ Thống

![Dataflow diagram](images/Dataflow.svg)

**Deployable Units:**

| ID | Unit | Runtime | Endpoint | Trách nhiệm |
|----|------|---------|----------|-------------|
| `GEN` | Generator | On-demand container | — | Tạo Parquet, PostgreSQL rows, Kafka/JSONL events |
| `KAFKA` | Kafka Broker | Container | `:29092` / `:9092` | 4-partition `stock_market_events_v3` |
| `ZK` | ZooKeeper | Container | `:2181` | Kafka coordination |
| `PG` | PostgreSQL 16 | Container | `:5433` | `vendor_db` + Airflow metadata |
| `AF` | Airflow | Webserver + Scheduler | `:8082` | Orchestrate DP1–DP3; launch Spark, monitor Flink |
| `SPARK` | Spark Cluster | Master + Worker | `:8080`, `:7077` | Batch Bronze/Silver/Gold/Feature transforms |
| `FLINK` | Flink Cluster | JobManager + TaskManager | `:8081` | Event-time dedup, watermark, quarantine, 5-min windows |
| `MINIO` | MinIO | Container | `:9000`, `:9001` | Landing + `delta-lake` bucket |
| `HIVE` | Hive Metastore | Container | `:9083` | Table/location metadata cho Trino |
| `TRINO` | Trino | Container | `:8083` | Federated queries (Delta + PostgreSQL) |
| `DATAHUB` | DataHub | Separate deployment | `:9002`, `:8084` | Dataset lineage |

**Data Flow:** Batch (`B0–B8`), Streaming (`S1–S5`), Query/Serving (`C1–C4`), Metadata/Governance (`M1–M8`)
— Mỗi mũi tên có đánh số, mô tả protocol, input/output và mức độ confidence.

**Data Layers & Naming:**

| Layer | Storage | Prefix | Nội dung |
|-------|---------|--------|----------|
| Landing | MinIO + PostgreSQL | — | Raw vendor files & reference tables |
| Bronze | Delta Lake | `raw_` | Raw ingested với schema version tags + ingest metadata |
| Silver | Delta Lake | `stg_` | Deduplicated, type-casted, schema-harmonized |
| Gold | Delta Lake + MinIO mirror + Trino | `dim_`, `fact_`, `obt_`, `feat_`, `ml_`, `agg_` | Business-ready |

---

## 2. README & Architecture Diagram

### Yêu cầu từ Rubric

> Viết file README.md giới thiệu business domain và high-level system deployment diagram.
> README cần có Repo Structure, Table of Contents, docstring cho hàm/class.
> Mỗi thành phần chính của diagram phải là 1 deployable unit.
> Mũi tên nên đi theo đường truyền dữ liệu, có description và đánh số thứ tự.
> Hạn chế mũi tên nét đứt (chỉ dùng cho luồng không phải luồng chính).

### Cách Dự Án Chứng Minh

| Yêu cầu | Bằng chứng | File |
|----------|-----------|------|
| Business domain | Mô tả 3 sàn (HOSE/HNX/UPCOM), lịch giao dịch, nguồn dữ liệu, chiến lược generation | [`README.md`](../README.md) § Business Domain |
| Repo Structure | Cây thư mục với mô tả từng file/folder | [`README.md`](../README.md) § Repository Structure |
| Table of Contents | TOC với link đến từng section | [`README.md`](../README.md) § Table of Contents |
| Docstring | Docstring trong toàn bộ `generator/`, `jobs/`, `dags/` | Source code |
| Deployable units | 15 deployable units với ID, Runtime, Endpoint, Responsibility | [`README.md`](../README.md) § Deployable Units |
| Data flow numbering | 4 nhóm flow riêng biệt: Batch (B0–B8), Streaming (S1–S5), Query (C1–C4), Metadata (M1–M8) | [`architecture.md`](architecture.md) § Data Flow Catalog |
| SVG deployment diagram | Dataflow.svg hiển thị tất cả services + data paths | [`images/Dataflow.svg`](images/Dataflow.svg) |
| Solid/dashed arrows | Solid = data payload; Dashed (xám) = orchestration/control plane | [`architecture.md`](architecture.md) § C — màu đường nối |

**Điểm mạnh:**
- Mỗi mũi tên trong Data Flow Catalog có: protocol, input, output, evidence file, confidence level
- 4 màu mũi tên riêng biệt cho batch/streaming/query/metadata flows
- Dashed arrows chỉ dùng cho M* (metadata/control), không phải data path chính

---

## 3. Docker & Docker Compose

| Rubric Item | Điểm |
|-------------|------|
| Có sử dụng Docker & Docker Compose | 1 |
| Optimize Dockerfile (multistage build) | 2 |

### 3.1 Có sử dụng Docker & Docker Compose

**Bằng chứng:**
- `docker-compose.yml` — 15+ services: PostgreSQL, MinIO, Kafka, ZooKeeper, Spark (master + worker), Flink (JobManager + TaskManager), Airflow (webserver + scheduler), Hive Metastore, Trino
- `Dockerfile` — multi-stage build cho generator
- Custom Dockerfiles cho Spark (`docker/spark/Dockerfile`), Flink (`docker/flink/Dockerfile`), Airflow (`docker/airflow/Dockerfile`)
- Named volumes: `postgres_data`, `minio_data`, `hive_metastore_data`
- Bind mounts: `./data` (shared lakehouse), `./jobs`, `./dags`, `./config`, `./contracts`
- Healthchecks cho PostgreSQL, MinIO, Kafka
- Profile-gated services: `generator`, `minio-sync-gold`, `spark-capture`

### 3.2 Optimize Dockerfile (Multi-stage Build)

**Phương pháp tối ưu:**
1. **Multi-stage build** — builder stage cài đặt virtual environment, runtime stage chỉ copy artifacts
2. **`python:3.12-slim`** runtime image — nhẹ hơn full image
3. **`pip --no-cache-dir`** — không lưu pip cache trong image
4. **Chỉ copy runtime code & config** — không include `vnstock` library (chỉ cần cho frozen seed refresh), không include Faker

**Kết quả đo được:**

| Build | Bytes | Giảm |
|-------|-------|------|
| Baseline | 195,099,639 | — |
| Optimized | 158,641,463 | **18.7%** |

```bash
# Cách đo:
docker image inspect vnstock-data-pipeline-generator:latest --format '{{.Size}}'
```

**File liên quan:** [`Dockerfile`](../Dockerfile), [`docker.md`](docker.md)

**Cảnh báo evidence gap:** Không có screenshot Docker image comparison trong `docs/images/`. Giá trị baseline được ghi nhận từ historical tag đã bị xóa — cần rebuild lại baseline image để chụp màn hình so sánh.

---

## 4. Data Generator — Offline Problems

| Rubric Item | Điểm |
|-------------|------|
| Simulate skew | 2 |
| Simulate high cardinality | 2 |
| Simulate schema evolution | 2 |
| Simulate another offline data problem (2% duplicate rate) | 2 |
| Using generator configuration | 2 |
| Store data for Bronze ingestion (MinIO/PostgreSQL) | 2 |

### 4.1 Skew (2 điểm)

**Vấn đề:** Trong thị trường thực, VN30 chiếm phần lớn thanh khoản. Dữ liệu mô phỏng có 80% volume tập trung vào 30/400 mã.

**Implementation:**
- `generator/problems.py:apply_volume_skew()` — phân phối lại volume: VN30 nhận 80%, non-VN30 nhận 20%
- `config/generator.yaml`: `vn30_volume_share: 0.80`, `industry_value_share: {banking: 0.35, real_estate: 0.25}`

**Capture yêu cầu:** Màn hình output chất lượng dữ liệu:
- **Skew distribution:** VN30 volume share = 80.1%
- **Cardinality:** `approx_count_distinct` by composite key
- **Schema evolution:** nulls trong old partitions
- **Duplicate rate:** trước/sau dedup

**Bằng chứng:** [Figure 02](images/01_generator_offline_quality.png) — hiển thị skew 80.1%, 72,000 distinct keys, 44,043 v1 rows với `foreign_room`/`value` null

### 4.2 High Cardinality (2 điểm)

**Vấn đề:** ~72,000 unique ticker × date composite keys; hàng triệu trade_id trong stream events.

**Implementation:** Tự nhiên từ cấu hình `n_tickers: 400` × `days_history: 180`.

**Cách xử lý downstream:** Spark `approx_count_distinct()` dùng HyperLogLog thay vì full shuffle `countDistinct()`; composite key expression thay vì đếm riêng từng cột.

### 4.3 Schema Evolution (2 điểm)

**Vấn đề:** Mô phỏng vendor API upgrade — partition trước `2025-07-01` có 7 cột (v1), partition sau có 9 cột (v2: thêm `value`, `foreign_room`).

**Implementation:**
- `generator/problems.py:tag_schema_version()` — đánh dấu `_schema_version=1` hoặc `=2`
- `generator/generators.py:generate_ohlcv()` — chỉ populate `value` và `foreign_room` cho `trade_date >= schema_change_date`
- 60% records là v1, 40% là v2

**Kiểm chứng:** 44,043 v1 rows có `foreign_room IS NULL` và `value IS NULL` do thiết kế (không phải lỗi).

**Bằng chứng:** [Figure 04](images/04_minio_schema_versions.png) — MinIO bucket hiển thị cả 2 `schema_version=1` và `schema_version=2` directories

### 4.4 Duplicate Rate (2 điểm)

**Vấn đề:** 2% rows có cùng `(ticker_id, trade_date, OHLCV)` — mô phỏng API retry.

**Implementation:**
- `generator/problems.py:inject_duplicates()` — nhân bản `rate` fraction của rows, giống hệt trên tất cả cột

**Kết quả:** 73,440 total rows, 72,000 distinct → **1,440 duplicates (2.0%)**

**Cách xử lý downstream:** `row_number()` window với `orderBy(desc("_ingested_at"))` — giữ lại row được ingest sau cùng, deterministic.

### 4.5 Generator Configuration (2 điểm)

**Config file đầy đủ:** `config/generator.yaml`

```yaml
n_tickers: 400
vn30_count: 30
vn30_volume_share: 0.80
days_history: 180
old_schema_share: 0.60
schema_change_date: "2025-07-01"
duplicate_rate_offline: 0.02
base_events_per_min: 200
burst_multiplier: 25
late_arrival_rate: 0.12
duplicate_rate_stream: 0.015
random_seed: 42
```

Drift config riêng: `config/drift.yaml` — auto-merged vào main config.

**Bằng chứng:** [Figure 03](images/03_generator_config.png) — screenshot của YAML config thực tế

### 4.6 Store Data for Bronze Ingestion (2 điểm)

**Mô phỏng external department data sources:**

| Landing Store | Công nghệ | Pattern | Content |
|---------------|-----------|---------|---------|
| `landing-vendor-offline/` | MinIO (Parquet) | End-of-day file drop | OHLCV, foreign flow, corporate actions, financial ratios |
| `vendor_db` | PostgreSQL | Batch DB-extract (JDBC) | Tickers, corporate actions |
| `stock_market_events_v3` | Kafka | Real-time event stream | Trades, quotes, index updates |
| `events/YYYY-MM-DD/HH/` | JSONL | Audit/replay log | Same events as Kafka |

**Nguyên tắc:** Landing stores được coi là **read-only** từ góc nhìn pipeline — Bronze ingestion kết nối đến chúng như kết nối đến hệ thống bên thứ 3 thực sự.

**Bằng chứng:**
- [Figure 05](images/04_minio_schema_versions.png) — MinIO physical schema versions
- [Figure 06](images/05_minio_physical_partitions.png) — v1 partition tại `schema_version=1/trade_date=2025-01-20`

---

## 5. Data Generator — Streaming Problems

| Rubric Item | Điểm |
|-------------|------|
| Simulate burst | 2 |
| Simulate late arrivals | 2 |
| Simulate another streaming problem (1.5% duplicate rate) | 2 |
| Using generator configuration | 2 |

### 5.1 Burst (2 điểm)

**Vấn đề:** ATO (09:00–09:15) và ATC (14:30–14:45) — khớp lệnh tập trung, volume events ×25 lần baseline.

**Implementation:**
- `burst_multiplier: 25` áp dụng trong `burst_windows: ["09:00-09:15", "14:30-14:45"]`
- Baseline: 200 events/min → Peak: **5,243 events/min**

**Kiểm chứng:** 2,667 burst flags trong stream features; event timeline visible.

### 5.2 Late Arrivals (2 điểm)

**Vấn đề:** 12% events có `created_ts` trễ hơn `event_timestamp` 5–45 giây — mô phỏng market data feed congestion.

**Implementation:**
- `late_arrival_rate: 0.12`, `late_delay_sec_min_max: [5, 45]`
- Late probability cao hơn trong burst windows (17.2% measured do correlation với burst)
- 60-second watermark + allowed lateness — covers 5–45s range với margin

### 5.3 Duplicate Rate (1.5%) (2 điểm)

**Vấn đề:** 1.5% events có `event_id` bị re-emit — mô phỏng at-least-once delivery.

**Implementation:**
- `inject_stream_duplicates()` — exact copies của randomly selected events, shuffle ngược lại vào stream

**Kết quả:** 197,925 emitted, 2,925 replayed event IDs (1.5%)

### 5.4 Generator Configuration (2 điểm)

Tất cả streaming parameters trong `config/generator.yaml`:
- `stream_days: 1`, `kafka_topic: "stock_market_events_v3"`
- `kafka_partitions: 4`, `kafka_retention_ms: -1`
- Cấu hình session model: ATO (×25), Continuous (×1), Lunch break (0), ATC (×25)
- Lunch break 11:30–13:00 — zero events, downstream watermark phải tolerate silence

**Bằng chứng:** [Figure 03](images/02_generator_stream_quality.png) — streaming quality output

---

## 6. Processing Jobs — Spark

| Rubric Item | Điểm |
|-------------|------|
| Baseline (without optimization) | 2 |
| Handle skew with explanation | 3 |
| Handle high cardinality with explanation | 3 |
| Handle schema evolution with explanation | 3 |
| Handle other offline problem (duplicates) with explanation | 3 |
| Spark job được tích hợp vào Airflow pipeline | 2 |

### 6.1 Baseline vs Optimized — Phương pháp

Script `scripts/spark_ui_capture.py` chạy **cùng một workload** trong 2 chế độ:
- **Baseline:** tắt hết optimization (AQE, broadcast, coalesce)
- **Optimized:** bật hết optimization

Dữ liệu đầu vào, số shuffle partitions (32), và transform logic **giữ nguyên** — chỉ config thay đổi.

### 6.2 Handle Skew — Strategy & Explanation (3 điểm)

**Vấn đề:** 80% volume trong VN30 → 1 partition overloaded, các partition khác gần như trống:
```
Partition VNM: ████████████████████████  800MB  → 1 task quá tải
Partition HPG: ██████                     200MB
Partition 3-200: ▏ ▏ ▏                    ~10MB each → idle
```

**Giải pháp — AQE Skew Join (Strategy 2):**
```python
# jobs/spark_session.py:29-33
.config("spark.sql.adaptive.enabled", "true")
.config("spark.sql.adaptive.skewJoin.enabled", "true")
.config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
.config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
```

AQE tự động detect partition có size/median > 5× **và** > 256MB, sau đó split thành pieces bằng nhau. Kết quả: task dài nhất không còn chi phối tổng thời gian.

**Tại sao không dùng manual salting?** AQE làm việc này tự động tại runtime, không cần biết trước hot keys, không làm clutter business logic.

### 6.3 Handle High Cardinality — Strategy & Explanation (3 điểm)

**Vấn đề:** 72,000 composite keys — `countDistinct(trade_id)` trigger full shuffle-and-sort.

**Giải pháp:**
- Dùng `approx_count_distinct()` (HyperLogLog) cho validation
- Composite key expression: `concat(ticker_id, '_', trade_date)` thay vì đếm riêng
- `autoBroadcastJoinThreshold=50MB` — tự động broadcast small dimension tables

### 6.4 Handle Schema Evolution — Strategy & Explanation (3 điểm)

**Vấn đề:** v1 (7 cột) và v2 (9 cột) trong cùng landing directory. `mergeSchema=true` tự động fill nulls — downstream không phân biệt được "missing by design (v1)" vs "missing by error".

**Giải pháp 3-stage:**

| Stage | Code | Mô tả |
|-------|------|-------|
| **Bronze** | `jobs/bronze/offline.py:37-129` | Dùng `mergeSchema=true` chỉ để load heterogeneous files → tách theo `_schema_version` từ path → validate từng version với contract riêng → ghi vào **2 Delta tables riêng** |
| **Silver** | `jobs/silver/daily.py:15-33` | Union 2 tables với `unionByName(allowMissingColumns=True)` → fill v1-absent columns với typed nulls → tag `_schema_version` |
| **Prevention** | `jobs/spark_session.py:28` | `spark.databricks.delta.schema.autoMerge.enabled=false` — mọi schema change phải qua explicit contract |

### 6.5 Handle Duplicates — Strategy & Explanation (3 điểm)

**Vấn đề:** 2% duplicate `(ticker_id, trade_date)` pairs. `dropDuplicates()` không deterministic.

**Giải pháp — Window Dedup (Strategy 5):**
```python
# jobs/silver/daily.py:36-45
window = Window.partitionBy("ticker_id", "trade_date").orderBy(F.desc("_ingested_at"))
result = df.withColumn("_rn", F.row_number().over(window))
          .filter(F.col("_rn") == 1).drop("_rn")
```
`row_number()` window với `orderBy(desc("_ingested_at"))` đảm bảo row được ingest **sau cùng** luôn thắng — kết quả giống hệt trên mọi lần rerun.

### 6.6 Spark Job Tích Hợp Airflow (2 điểm)

| DAG | Spark Job | Input → Output |
|-----|-----------|----------------|
| `bronze_offline_ingest` | `jobs/bronze/offline.py` | Landing Parquet + PostgreSQL → Bronze Delta |
| `silver_daily` | `jobs/silver/daily.py` | Bronze Delta → Silver Delta |
| `gold_dimensions_and_facts` | `jobs/gold/dimensions.py`, `facts.py`, `obt.py` | Silver → Dimensions + Facts + OBT |
| `feat_daily_job` | `jobs/gold/features.py`, `labels.py`, `drift.py` | Gold facts → Features + Labels + Drift |

### Tổng Hợp Spark Config

| Config | Value | Strategy |
|--------|-------|----------|
| `spark.sql.adaptive.enabled` | `true` | Skew Join + Coalesce |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | Skew Join |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | Coalesce |
| `spark.sql.autoBroadcastJoinThreshold` | `50MB` | Broadcast Join |
| `spark.databricks.delta.schema.autoMerge.enabled` | `false` | Schema Evolution |
| Partition `by("trade_date")` on facts | — | Partition Design |

**Bằng chứng hình ảnh:**
- [Figure 06–08](images/) — Baseline: Spark Jobs, Stages, Terminal output
- [Figure 09–12](images/) — Optimized: Spark Jobs, Stages, Master UI, Terminal output

---

## 7. Processing Jobs — Flink

| Rubric Item | Điểm |
|-------------|------|
| Baseline (without optimization) | 2 |
| Handle burst with explanation | 3 |
| Handle late arrival with explanation | 3 |
| Handle other streaming problem (duplicates) with explanation | 3 |
| Window processing (capture code) | 2 |

### 7.1 Baseline

Job `377ab5e5eb029d8f905db390c54549b6` — RUNNING, 12/12 tasks.

**Kiến trúc Flink Job (`jobs/flink/silver_stream.py`):**
```
Kafka Source (p=4) → Timestamp/Watermark Assignment → KeyedProcessFunction (dedup + late routing)
                                                              ↓
                                              [side output: quarantine]
                                                              ↓
                                              TumblingEventTimeWindows (5 min) → FileSink
```

### 7.2 Handle Burst — Strategy & Explanation (3 điểm)

**Vấn đề:** ATO/ATC auction ×25 baseline volume — single partition backpressure.

**Giải pháp:**
1. **4 partitions** trên Kafka topic `stock_market_events_v3`, keyed by ticker
2. **Parallelism 4** trên source, keyed process, và window vertices = 12 running tasks
3. `retention.ms=-1` — giữ deterministic historical event timestamps
4. Checkpointed `FileSink` — consistent output sau mỗi 30s checkpoint

### 7.3 Handle Late Arrivals — Strategy & Explanation (3 điểm)

**Vấn đề:** 12% events trễ 5–45s, tập trung trong burst windows. Processing-time windows assign events vào sai window.

**Giải pháp — Event-time processing với watermark:**
```
watermark = event_timestamp - 60s        ← covers 5–45s range với margin
allowed_lateness = 60s                   ← late firing: recompute window
side_output (quarantine)                 ← events beyond grace → stg_events_quarantine
```

Đây map trực tiếp vào Flink API surface: watermark strategy + allowed lateness + side output.

### 7.4 Handle Duplicates — Strategy & Explanation (3 điểm)

**Vấn đề:** 1.5% events re-emit cùng `event_id`.

**Giải pháp:**
- PyFlink `KeyedProcessFunction` — keyed state lưu `event_id` đã thấy
- **First arrival wins** — giữ event đầu tiên, suppress các replay sau
- Spark Silver path dùng `dropDuplicatesWithinWatermark(["event_id"])`

**Kết quả:** Source records 197,925 → After dedup 195,000 → Trade-window input 165,668

### 7.5 Window Processing — Code Evidence (2 điểm)

| Feature | Window Type | Size | Key | Code |
|---------|------------|------|-----|------|
| `f_stream_volume_5m` | Tumbling event time | 5 min | ticker | `jobs/flink/silver_stream.py` |
| `f_stream_trade_count_30m` | Sliding event time | 30 min / 1 min slide | ticker | `jobs/features/stream.py` |

**Kết quả:** 17,845 five-minute windows, 2,667 burst flags, watermark `2025-10-09 14:43:58 UTC`

**Bằng chứng hình ảnh:**
- [Figure 10](images/10_flink_job_graph.png) — Flink job graph RUNNING
- [Figure 11](images/11_flink_checkpoints.png) — 858 checkpoints, 0 failed
- [Figure 12](images/12_flink_records_watermark.png) — Source metrics & records
- [Figure 13](images/13_flink_backpressure.png) — Drained-state backpressure (OK, 0%)
- [Figure 14](images/14_flink_watermark_window_code.png) — PyFlink code

---

## 8. Data Storage Optimization

| Rubric Item | Điểm |
|-------------|------|
| Lakehouse (compaction, Z-order, partitioning) | 2 |
| Datawarehouse (indexing) | 2 |

### 8.1 Lakehouse Optimization (2 điểm)

**Chiến lược:**

| Vấn đề | Giải pháp | Implementation |
|---------|-----------|----------------|
| Small files từ repeated writes | `OPTIMIZE` compaction | `jobs/gold/maintenance.py:run_maintenance()` |
| Ticker-filter queries scan nhiều files | Z-order by `ticker_id` | 6 tables được Z-ordered |
| Full scan trên date-range queries | Partition by `trade_date` | Applied trên fact/OBT tables |
| Schema drift tự động | `autoMerge.enabled=false` | `jobs/spark_session.py` |

**Tables được tối ưu:**
- `fact_daily_price` — Z-order `ticker_id`
- `obt_ticker_daily_performance` — Z-order `ticker_id, trade_date`
- `feat_ticker_daily` — Z-order `ticker_id`
- `feat_ticker_unified` — Z-order `ticker_id`
- `ml_ticker_training` — Z-order `ticker_id`
- `dim_ticker` — Z-order `ticker_id`

**Maintenance schedule:** Weekly `delta_maintenance` DAG (Sun 03:00)

**Evidence gap:** Không có screenshot before/after về file count, bytes scanned, hoặc query duration. Cần chạy `OPTIMIZE` và capture kết quả so sánh.

### 8.2 Datawarehouse Optimization — PostgreSQL Indexing (2 điểm)

**Vấn đề:** Point lookup by `ticker_id` trên bảng `tickers`.

**Giải pháp:**
- `tickers.ticker_id` là **PRIMARY KEY** → B-tree index `tickers_pkey`
- Secondary indexes: `corporate_actions(ticker_id)`, `corporate_actions(ex_date)`
- `scripts/init_vendor_db.sql` tạo tất cả constraints
- Generator dùng `TRUNCATE ... CASCADE` + append (không xóa indexes)

**Đo đạc thực tế:**

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM tickers WHERE ticker_id = 'VIC';
```

| Chỉ số | Giá trị |
|--------|---------|
| Scan strategy | `Index Scan using tickers_pkey` |
| Planning time | 1.043 ms |
| Execution time | 0.098 ms |

**Bằng chứng:** [Figure 18](images/18_postgres_index_explain.png) — DBeaver `EXPLAIN (ANALYZE, BUFFERS)` result

---

## 9. Data Pipeline Orchestration

| Rubric Item | Điểm |
|-------------|------|
| DP1 — Ingest + Validate stages | 2 + 2 |
| DP2 — Ingest + Validate stages | 2 + 2 |
| DP3 — Ingest + Validate stages | 2 + 2 |

### 9.1 DP1 — Bronze Ingestion (4 điểm)

**DAG:** `bronze_offline_ingest` | **Schedule:** 30 15 * * * (15:30 UTC daily) | **Retries:** 3 × 5 min

```
[landing-vendor-offline/] ──┐
                             ├──► OHLCV_v1 ──┐
                             ├──► OHLCV_v2 ──┤
                             ├──► Foreign    ─┼──► validate_contract ──► validate_quality
                             ├──► CorpActions─┤
[vendor_db (PostgreSQL)] ────┼──► FinRatios  ─┘
                             └──► Tickers
```

**Validate stage:** Contract validation (`validate_contract()`) theo `_schema_version` — cột có trong data nhưng không có trong contract → fail. Quality validation: row counts, key uniqueness, domain checks.

**Bằng chứng:** [Figure 06](images/06_airflow_dp1_bronze.png) — 5 source tasks → validate_contract → validate_quality. Tất cả green/success.

### 9.2 DP2 — Silver + Gold (4 điểm)

**DAGs:** `silver_daily` (16:00 UTC) + `gold_dimensions_and_facts` (16:30 UTC)

**Silver stage:** Dedup branches → `validate_uniqueness` → referential → domain → duplicate → cardinality gates (7 tasks)

**Gold stage:** SCD2 dimensions → 3 fact builders → OBT → referential checks → SCD2/contract checks → run metadata

**Bằng chứng:**
- [Figure 07](images/07_airflow_dp2_silver.png) — 7 tasks success
- [Figure 08](images/08_airflow_dp2_gold.png) — SCD2 + facts + OBT + checks

### 9.3 DP3 — Features (4 điểm)

**DAG:** `feat_daily_job` | **Schedule:** 0 17 * * * (17:00 UTC daily)

**Stages:** daily features → unified features → labels → training table → drift monitoring → PSI computation → timestamp checks → contract checks → leakage checks → drift alerts (9 tasks)

**Bằng chứng:** [Figure 09](images/09_airflow_dp3_features.png) — 9 tasks success

### Verified Reference Execution

| Stage | Airflow run ID | Duration | Verified Output |
|-------|---------------|----------|-----------------|
| Bronze offline | `manual__2026-07-23T17:26:00` | 312.8 s | 73,440 OHLCV input rows |
| Silver daily | `manual__2026-07-23T17:30:00` | 388.5 s | 72,000 deduplicated price rows |
| Gold dims/facts | `manual__2026-07-23T17:40:00` | 446.6 s | 72,000 daily facts + OBT rows |
| DP3 features/labels/drift | `manual__2026-07-23T17:50:00` | 1,262.6 s | 72,000 features; 70,800 labels |

---

## 10. Data Governance

| Rubric Item | Điểm |
|-------------|------|
| DP1 — Lineage + Data validation/contract | 2 + 2 |
| DP2 — Lineage + Data validation/contract | 2 + 2 |
| DP3 — Lineage + Data validation/contract | 2 + 2 |

### 10.1 Data Contracts

**Versioned JSON Schema contracts trong `contracts/`:**

| Contract | Version | Required Columns |
|----------|---------|-----------------|
| `raw_ohlcv_daily.v1.json` | 1 | ticker_id, trade_date, open, high, low, close, volume (7 cols) |
| `raw_ohlcv_daily.v2.json` | 2 | + value, foreign_room (9 cols) |
| `raw_market_events.v1.json` | — | payload_json, source_offset, source_partition, source_topic, _ingested_at |
| `stg_trades.v1.json` | — | event_id, event_ts, ticker, price, quantity, trade_id |
| `stg_quotes.v1.json` | — | event_id, event_ts, ticker, bid_price, ask_price |
| `feat_stream_intraday.v1.json` | — | ticker_id, event_timestamp |

**Enforcement:** `jobs/bronze/offline.py:validate_contract()` kiểm tra data trước khi ghi vào Bronze:
- Thiếu required column → `ValueError`
- Cột populated nhưng không có trong contract → "contract violation"
- `spark.databricks.delta.schema.autoMerge.enabled=false` — ngăn silent schema merge

### 10.2 DataHub Lineage

**Manifest:** `datahub/lineage.yml` — static dataset-to-dataset lineage edges:

```
DP1: landing-vendor-offline → raw_* → stg_*
DP2: stg_* → dim_*/fact_* → obt_*
DP3: fact_* → feat_* → ml_*
```

**Công cụ:** DataHub CLI + recipe publish qua REST.

**Evidence gap quan trọng:** KHÔNG có DataHub UI screenshots nào trong `docs/images/`. Recipe chỉ publish dataset-level lineage, không publish Airflow runs, runtime lineage, contracts, assertions, hoặc schema entities. Cả 6 governance UI rubric items đều thiếu visual proof.

---

## 11. Schema Design & Documentation

| Rubric Item | Điểm |
|-------------|------|
| Visualize tables on all zones (DBeaver) | 2 |
| Dim table with SCD 2 (valid_from_ts, valid_to_ts, is_current) | 2 |
| Feature tables with event_timestamp and created_ts | 2 |
| Relationship between dim & fact tables | 2 |
| Naming convention | 2 |

### 11.1 Visualize Tables — All Zones (2 điểm)

**Zones và naming:**

| Layer | Prefix | Storage | Purpose |
|-------|--------|---------|---------|
| Bronze | `raw_` | Delta | Contract-checked snapshot + ingest metadata |
| Silver | `stg_` | Delta | Deduped, type-cast, schema-harmonized |
| Gold | `dim_`, `fact_`, `obt_`, `feat_`, `agg_`, `ml_` | Delta + MinIO mirror | Business-ready |

**Bằng chứng:** [Figure 15](images/15_dbeaver_bronze_silver_gold_er.png) — DBeaver ER view từ Trino `delta.gold_stock` catalog (partial: không hiển thị Bronze/Silver tables, cần bổ sung full-zone ER diagram)

### 11.2 SCD Type 2 — dim_ticker (2 điểm)

**Tại sao SCD2:** Tickers thay đổi — chuyển sàn (UPCOM → HOSE), đổi tên công ty, hủy niêm yết. SCD2 được exercise, không phải decorative.

**Cấu trúc:**
```sql
dim_ticker (
  ticker_key       INTEGER,      -- Surrogate key
  ticker_id        VARCHAR,      -- Business key
  ticker           VARCHAR,
  company_name     VARCHAR,
  exchange         VARCHAR,
  icb_l1, icb_l2  VARCHAR,
  valid_from_ts    TIMESTAMP,    -- SCD2: row effective from
  valid_to_ts      TIMESTAMP,    -- SCD2: row effective until (NULL = current)
  is_current       BOOLEAN       -- SCD2: current version flag
)
```

**Kết quả:** 475 SCD2 versions, 400 current rows.

**Bằng chứng:** [Figure 16](images/16_dbeaver_scd2.png) — Query hiển thị ACB và ACM có historical closed version (`valid_to_ts` populated, `is_current=false`) và open version (`valid_to_ts=NULL`, `is_current=true`)

### 11.3 Feature Tables — Timestamps (2 điểm)

Tất cả feature tables có 2 timestamp columns:

| Column | Ý nghĩa |
|--------|---------|
| `event_timestamp` | Feature as-of time (cho point-in-time joins) |
| `created_ts` | Computation/materialization time (cho dedup, keep latest) |

**Feature tables:**

| Table | Grain | Features | Refresh |
|-------|-------|----------|---------|
| `feat_ticker_daily` | (ticker_id, trade_date) | return_5d, volatility_20d, ma20_gap, foreign_net_ratio_10d | Daily after close |
| `feat_stream_intraday` | (ticker_id, event_timestamp) | volume_5m, trade_count_30m | 1–5 min during trading |
| `feat_ticker_unified` | (ticker_id, event_timestamp) | Join daily + intraday features | 15 min / EOD |

**Bằng chứng:** [Figure 17](images/17_dbeaver_feature_timestamps.png) — `DESCRIBE feat_ticker_daily` hiển thị cả 2 timestamp columns

### 11.4 Dim–Fact Relationships (2 điểm)

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

### 11.5 Naming Convention (2 điểm)

Prefixes nhất quán toàn bộ pipeline: `raw_` (Bronze), `stg_` (Silver), `dim_`/`fact_`/`obt_`/`feat_`/`agg_`/`ml_` (Gold). Dataset naming phản ánh grain: `fact_daily_price`, `feat_stream_intraday`, `ml_ticker_training`.

---

## 12. Novel Ideas

| Rubric Item | Điểm |
|-------------|------|
| Idea 1 — Document + Proof | 5 |
| Idea 2 — Document + Proof | 5 |

### 12.1 Idea 1: Trino Federated Query Engine (5 điểm)

**Ý tưởng:** Trino làm single SQL interface query **xuyên suốt** Delta Lake (MinIO) và PostgreSQL, cho phép cross-source JOINs không cần ETL data movement.

**Tại sao beyond EDAI:** M8 đề cập storage concepts ở mức tên gọi; M11 đề cập OLAP engines ở mức khái niệm — không module nào cover triển khai thực tế federated query engine với catalog configuration, cross-source optimization, và schema discovery từ Delta + JDBC.

**Implementation:**
- Trino container trong `docker-compose.yml` (port 8083)
- Catalog files: `docker/trino/catalog/delta.properties` (MinIO-backed Delta) + `postgres.properties` (vendor_db)
- Query examples: `scripts/trino_examples.sql`

**Measured proof:**

| Query | Result |
|-------|--------|
| `SELECT count(*) FROM postgres.public.tickers` | 400 |
| `SELECT count(*) FROM delta.gold_stock.fact_daily_price` | 72,000 |
| Delta daily fact JOIN PostgreSQL tickers | **72,000 rows federated** |
| `delta.gold_stock.fact_intraday_trade` | 165,668 |
| `delta.gold_stock.feat_stream_intraday` | 17,845 |

**Bằng chứng:** [Figure 19](images/19_trino_federated_join.png) — DBeaver SQL hiển thị cross-catalog join, `federated_rows = 72,000`

### 12.2 Idea 2: Versioned Data Contracts (5 điểm)

**Ý tưởng:** Key Bronze/Silver/Gold/feature datasets có versioned JSON Schema contracts định nghĩa required columns và types. Bronze job validate data với contract trước khi ghi Delta. Column populated nhưng không có trong contract → "contract violation" (khác với planned v1→v2 evolution).

**Tại sao beyond EDAI:** M5 cover pytest/TDD; M12 đề cập "data validation fundamentals" ở mức khái niệm — không module nào cover triển khai production-grade contract enforcement pattern với JSON Schema như lightweight alternative cho Schema Registry, versioned contract evolution, và integration với ingestion pipeline.

**Implementation:**
- 6 contract files: `contracts/raw_ohlcv_daily.v1.json`, `.v2.json`, `raw_market_events.v1.json`, `stg_trades.v1.json`, `stg_quotes.v1.json`, `feat_stream_intraday.v1.json`
- `jobs/bronze/offline.py:validate_contract()` — reject missing required fields và populated unregistered fields
- `load_contract(version)` — đọc đúng contract file theo `_schema_version`
- `autoMerge.enabled=false` — ngăn silent schema merge

**Proof:**
1. 2 physical contract files cho OHLCV v1 (7 cột) và v2 (9 cột)
2. `validate_contract()` được gọi trong DP1 ingest trước khi ghi Bronze
3. DP1 Figure 06 hiển thị `validate_contract` gate success trong Airflow
4. Unit tests exercise contract loading/enforcement

**Evidence gap:** Không có Airflow task-log screenshot của deliberate failing contract.

---

## 13. Bảng Tổng Hợp Evidence Gaps

Những rubric items còn thiếu visual proof cần bổ sung:

| # | Rubric Area | Missing Evidence | Mức độ |
|---|-------------|-----------------|--------|
| 1 | Docker | Screenshot so sánh image size baseline vs optimized | **Cần capture** |
| 2 | Spark | Baseline vs optimized Spark UI (Jobs/Stages/SQL) comparison | **Có một phần** — có script capture nhưng cần xác nhận screenshots |
| 3 | Lakehouse storage | Before/after screenshot của compaction/Z-order (file count, query duration) | **Cần capture** |
| 4 | DataHub | DP1/DP2/DP3 lineage, validation, và contract UI screenshots | **Cần capture** — DataHub Quickstart cần được deploy và ingest manifest |
| 5 | Schema design | Full all-zone DBeaver ER diagram (Bronze + Silver + Gold) | **Có một phần** — cần bổ sung Bronze/Silver tables |
| 6 | Novel Idea 2 | Airflow task-log screenshot của deliberate contract failure | **Cần capture** |
| 7 | Streaming | DataHub validation/contract cho streaming tables | **Not implemented** — contract files exist nhưng streaming jobs không load chúng |

### Verified Evidence Inventory

| Figures | Document | What They Prove | Status |
|---------|----------|-----------------|--------|
| 01–05 | `generator.md` | Offline/stream quality, config, MinIO landing | **Captured** |
| 06–09 | `orchestration_governance.md` | Airflow DP1–DP3 task order/success | **Captured** |
| 10–14 | `processing_jobs.md` | Flink graph, checkpoints, metrics, backpressure, code | **Captured** |
| 15–17 | `schema_design.md` | Partial Gold ER, SCD2 rows, feature timestamps | **Partial captured** |
| 18 | `storage_optimization.md` | PostgreSQL index-backed lookup | **Captured** |
| 19 | `novel_ideas.md` | Trino cross-catalog federation | **Captured** |

---

## Phụ Lục: Các File Quan Trọng

| File | Vai trò |
|------|---------|
| `config/generator.yaml` | Generator configuration (data problems, seed, volumes) |
| `config/drift.yaml` | Drift simulation (Scenario A: volatility regime shift) |
| `contracts/*.json` | Versioned JSON data contracts |
| `dags/*.py` | Airflow orchestration DAGs |
| `jobs/bronze/offline.py` | Spark Bronze ingestion + contract validation |
| `jobs/silver/daily.py` | Spark Silver dedup + harmonization |
| `jobs/silver/stream.py` | Spark Silver streaming dedup + quarantine |
| `jobs/gold/dimensions.py` | SCD2 dim_ticker + static dimensions |
| `jobs/gold/facts.py` | fact_daily_price + fact_intraday_trade |
| `jobs/gold/features.py` | feat_ticker_daily + feat_ticker_unified |
| `jobs/gold/labels.py` | ml_ticker_label (3-day price-up) |
| `jobs/gold/drift.py` | PSI computation + drift alerts + training table |
| `jobs/gold/maintenance.py` | Delta OPTIMIZE + Z-order |
| `jobs/flink/silver_stream.py` | PyFlink dedup + watermark + windows |
| `jobs/spark_session.py` | Shared Spark session with all optimizations |
| `generator/main.py` | Generator entry point |
| `generator/generators.py` | Offline + streaming generation logic |
| `generator/problems.py` | Data problem injection logic |
| `generator/writers.py` | Landing persistence + quality report |
| `scripts/spark_ui_capture.py` | Baseline vs optimized Spark capture runner |
| `scripts/trino_examples.sql` | Federated query examples |
| `scripts/init_vendor_db.sql` | PostgreSQL schema + indexes |

---

> **Ghi chú:** Tài liệu này map từng rubric item đến bằng chứng cụ thể trong project. Các evidence gaps được liệt kê rõ ràng, không fabricate placeholder screenshots. Reference execution dùng seed 42, 400 tickers, 180 trading days.
