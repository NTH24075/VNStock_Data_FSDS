# Kiến trúc hệ thống VNStock Data Pipeline

Tài liệu này mô tả kiến trúc được suy ra từ source code và configuration đang
có trong repository. Các số liệu chạy trong `docs/evidence/` chỉ được dùng để
đối chiếu, không thay thế code/config làm nguồn sự thật.

Quy ước mức độ xác nhận:

- **High**: có implementation và cấu hình kết nối trực tiếp trong repository.
- **Medium**: component/cấu hình tồn tại nhưng một phần vòng đời cần thao tác
  thủ công hoặc không có dependency tự động.
- **Low**: chỉ có tài liệu, ví dụ sử dụng hoặc bằng chứng gián tiếp.
- **Inferred**: suy luận hợp lý từ nhiều file nhưng không được khai báo trực tiếp.
- **Unverified**: repository không chứa đủ automation để xác nhận end-to-end.
- **TBD**: chưa có bằng chứng để điền giá trị.

## A. Architecture summary

Hệ thống mô phỏng dữ liệu thị trường chứng khoán Việt Nam và xử lý dữ liệu theo
hai nhánh:

1. **Batch**: generator tạo OHLCV, foreign flow, corporate actions và financial
   ratios. Dữ liệu được ghi vào local landing Parquet; một bản được mirror lên
   MinIO, còn ticker/corporate action được ghi vào PostgreSQL. Các Airflow DAG
   chạy Spark để ingest, kiểm tra contract, deduplicate và xây dựng các Delta
   table theo Bronze → Silver → Gold, sau đó tạo dimension, fact, OBT, feature,
   label và bảng theo dõi drift.
2. **Streaming**: generator phát JSON event vào Kafka và đồng thời lưu JSONL để
   replay/audit. Nhánh Spark Structured Streaming ghi Kafka payload vào Bronze
   Delta, tạo Silver trades/quotes/quarantine rồi tạo intraday feature trong
   Gold Delta. Một ứng dụng PyFlink độc lập cũng đọc trực tiếp cùng Kafka topic,
   deduplicate theo event time, tạo quarantine và cửa sổ volume 5 phút, nhưng
   ghi JSONL vào `data/flink`; code hiện tại không nối output PyFlink này vào
   Delta Silver/Gold.

Lakehouse thực thi nằm trên bind mount `./data`, dùng Delta Lake cho
Bronze/Silver/Gold. Gold có thể được mirror bằng một one-shot `minio/mc` job vào
bucket `delta-lake` để Trino đọc qua Delta connector; Trino đồng thời có
PostgreSQL connector để chạy federated SQL. Hive Metastore cung cấp endpoint
metadata cho Trino, nhưng repository chưa có script tự động đăng ký các Gold
location/table vào metastore.

Các pattern chính:

- Medallion/lakehouse: Bronze → Silver → Gold trên Delta Lake.
- Hybrid batch + streaming với chung business domain và Kafka replay.
- Orchestration bằng Airflow `LocalExecutor`; Spark là compute engine chính.
- Event-time processing, watermark, stateful deduplication và checkpointing.
- Star schema + SCD Type 2 + OBT + offline/streaming feature tables.
- Versioned JSON data contracts, explicit schema evolution và quality gates.
- Federated query qua Trino; static dataset lineage publish qua DataHub.
- Snapshot rebuild cho phần lớn batch table (`overwrite`), append/checkpoint
  cho streaming và operational metadata.

Không tìm thấy business HTTP API, REST/gRPC endpoint, cache, AMQP queue,
Schema Registry, Kubernetes/Helm manifest, Prometheus/Grafana, distributed
tracing hoặc alert-notification integration trong repository hiện tại.

## B. Component inventory

| Component | Type | Responsibility | Inputs | Outputs | Port | Evidence | Confidence |
|---|---|---|---|---|---|---|---|
| Data Generator | On-demand Python container / producer | Tạo deterministic offline datasets, reference rows, stream events, quality report và data-problem scenarios | Frozen ticker seed; `config/generator.yaml`; `config/drift.yaml` | Local Parquet; MinIO objects; PostgreSQL rows; Kafka JSON events; local JSONL | — | `generator/main.py`; `generator/generators.py`; `generator/writers.py`; `Dockerfile`; `docker-compose.yml` | High |
| Frozen ticker seed / optional seed fetcher | Repository dataset / optional external-source adapter | Cung cấp ticker reference cố định; `fetch.py` có thể gọi `vnstock` để làm mới seed ngoài runtime chính | `vnstock` listing API khi chạy thủ công | `generator/seed/tickers_reference.json` | — | `generator/seed/fetch.py`; `generator/seed/__init__.py`; `Dockerfile` | High |
| PostgreSQL `vendor_db` | Relational source + Airflow metadata DB | Lưu `tickers`, `corporate_actions` và metadata của Airflow; phục vụ JDBC/SQL/federated query | Generator SQL writes; Airflow metadata writes | JDBC rows cho Spark; catalog rows cho Trino; SQL result cho client | `5432` internal; `5433` host | `docker-compose.yml`; `scripts/init_vendor_db.sql`; `generator/writers.py`; `dags/bronze_offline_ingest.py`; `docker/trino/catalog/postgres.properties` | High |
| Kafka broker | Streaming message broker | Vận chuyển ticker-keyed market events; topic được generator tạo/cấu hình | JSON `trade`, `quote`, `index_update` events | Records cho Spark Structured Streaming và PyFlink | `29092` internal; `9092` host | `docker-compose.yml`; `generator/writers.py`; `jobs/bronze/stream.py`; `jobs/flink/silver_stream.py` | High |
| ZooKeeper | Kafka coordination service | Broker membership và coordination cho Kafka 7.6 deployment | Kafka coordination requests | Coordination metadata | `2181` internal | `docker-compose.yml` | High |
| Shared project data volume | Bind-mounted filesystem / lakehouse storage | Chia sẻ landing, Delta tables, checkpoints, replay và Flink files giữa host và containers | Generator, Spark jobs, Flink sinks | `data/landing`, `data/events`, `data/{bronze,silver,gold}`, `data/flink` | — | `docker-compose.yml`; `generator/writers.py`; `jobs/**`; `dags/**` | High |
| MinIO | S3-compatible object storage | Lưu mirror vendor landing và mirror Gold để query | Generator uploads; `minio-sync-gold` mirror | `landing-vendor-offline/*`; `delta-lake/gold/*` cho Trino | `9000` API; `9001` UI | `docker-compose.yml`; `generator/writers.py`; `docker/trino/catalog/delta.properties` | High |
| MinIO bucket initializer | One-shot utility container | Tạo hai bucket nếu chưa tồn tại | MinIO health; access-key env names | `landing-vendor-offline`; `delta-lake` | — | `docker-compose.yml` service `minio-create-buckets` | High |
| Gold sync tool | Profile-gated one-shot utility | Mirror local `./data/gold` vào MinIO | Local Gold Delta files | `delta-lake/gold` objects | — | `docker-compose.yml` service `minio-sync-gold`; `Makefile` target `sync-gold` | High |
| Spark master + worker | Distributed batch/stream compute cluster | Chạy Bronze/Silver/Gold/feature transformations, JDBC reads, Delta reads/writes và Structured Streaming | Landing Parquet; PostgreSQL JDBC; Kafka; local Delta | Bronze/Silver/Gold Delta tables và streaming checkpoints | `7077` RPC; `8080` master UI | `docker-compose.yml`; `docker/spark/Dockerfile`; `jobs/spark_session.py`; `jobs/**` | High |
| Spark capture driver | Optional profile-gated Spark client | Chạy workload so sánh baseline/optimized và giữ driver UI để thu evidence | Gold `fact_daily_price`, `dim_ticker` | UI/runtime evidence; không tạo production dataset | `4040` host | `docker-compose.yml` service `spark-capture`; `scripts/spark_ui_capture.py`; `Makefile` | High |
| Flink JobManager + TaskManager | Streaming compute cluster | Chạy PyFlink Kafka source, event-time watermark, dedup, late routing và window volume | Kafka `stock_market_events_v3` | JSONL `stg_events`, `stg_events_quarantine`, `feat_stream_volume_5m` | `8081` JobManager UI/REST | `docker-compose.yml`; `docker/flink/Dockerfile`; `jobs/flink/silver_stream.py` | High |
| Airflow webserver + scheduler | Orchestrator using `LocalExecutor` | Schedule batch DAGs; start/wait Spark streaming jobs; monitor Flink qua REST; lưu run/task state | DAG files; env configuration; contracts; PostgreSQL metadata | Task control, logs, Spark submissions, Flink health checks, metadata state | `8080` internal webserver; `8082` host | `docker-compose.yml`; `docker/airflow/Dockerfile`; `dags/*.py` | High |
| JSON contract registry | Repository metadata / governance artifact | Định nghĩa versioned schemas và được một số batch DAG/job dùng làm quality gate | `contracts/*.json` | Pass/fail contract validation | — | `contracts/*.json`; `jobs/bronze/offline.py`; `dags/bronze_offline_ingest.py`; `dags/feat_daily_job.py`; `dags/gold_dimensions_and_facts.py` | High |
| Hive Metastore | Metadata service for Trino Delta catalog | Cung cấp Thrift metastore endpoint và S3A configuration | Table/location registration (**Unverified automation**) | Schema/location metadata cho Trino | `9083` internal và host | `docker-compose.yml`; `docker/hive/hive-site.xml`; `docker/trino/catalog/delta.properties` | Medium |
| Trino | Federated SQL query engine | Query Gold Delta objects qua Hive/MinIO và PostgreSQL reference data | MinIO Delta objects; Hive metadata; PostgreSQL rows; analyst SQL | SQL result sets | `8080` internal; `8083` host | `docker-compose.yml`; `docker/trino/catalog/*.properties`; `scripts/trino_examples.sql` | Medium |
| SQL client (DBeaver or equivalent) | External/manual consumer | Gửi SQL tới Trino hoặc trực tiếp PostgreSQL | User SQL | Query results / execution plans | — | `scripts/trino_examples.sql`; `docs/storage_optimization.md`; `docs/novel_ideas.md` | Medium |
| DataHub CLI + lineage manifest | Manual governance publisher | Đọc static dataset-lineage file và publish qua DataHub REST | `datahub/lineage.yml`; `datahub/recipe.yml` | Dataset lineage events | — | `datahub/lineage.yml`; `datahub/recipe.yml`; `datahub/README.md` | Medium |
| DataHub Quickstart | Separate governance deployment, không thuộc default Compose | Hiển thị dataset-level lineage | DataHub CLI REST ingest | DataHub UI lineage | `8084` mapped GMS; `9002` UI (documented) | `datahub/README.md`; `datahub/recipe.yml` | Low |

Lưu ý về inventory:

- `raw_tickers` được DP1 ghi vào Bronze, nhưng `dim_ticker` hiện đọc frozen seed
  JSON kết hợp với `stg_daily_price`; không có code downstream đọc
  `raw_tickers`.
- `raw_financial_ratios` được ingest nhưng chưa có consumer downstream.
- `raw_corporate_actions` được `fact_daily_price` đọc trực tiếp từ Bronze; không
  có implementation tạo `stg_corporate_actions`.
- `get_spark_with_minio()` tồn tại nhưng không được gọi; production jobs hiện
  ghi Delta vào shared local volume, không ghi trực tiếp S3A/MinIO.

## C. System architecture diagram

```mermaid
flowchart LR
    subgraph SOURCE["Nguồn và phát sinh dữ liệu"]
        direction TB
        SEED["Frozen ticker seed<br/>generator/seed/*.json"]
        GEN["Data Generator<br/>offline | stream | all"]
        PG[("PostgreSQL vendor_db<br/>tickers + corporate_actions<br/>internal :5432 / host :5433")]
        KAFKA["Kafka broker<br/>stock_market_events_v3<br/>internal :29092 / host :9092"]
        ZK["ZooKeeper<br/>internal :2181"]
    end

    subgraph ORCH["Orchestration và compute"]
        direction TB
        AIRFLOW["Airflow webserver + scheduler<br/>LocalExecutor · host :8082"]
        SPARK["Spark master + worker<br/>RPC :7077 · UI :8080<br/>batch + Structured Streaming"]
        FLINK["Flink JobManager + TaskManager<br/>REST/UI :8081<br/>PyFlink streaming"]
    end

    subgraph LAKE["Shared data volume / Lakehouse"]
        direction TB
        LAND[("data/landing<br/>Parquet vendor drops")]
        REPLAY[("data/events<br/>hourly JSONL replay")]
        DELTA[("data/bronze · data/silver · data/gold<br/>Delta tables + checkpoints")]
        FLINKFILES[("data/flink<br/>JSONL Silver/quarantine/5-min volume")]
    end

    subgraph OBJECT["Object storage và serving"]
        direction TB
        SYNC["minio-sync-gold<br/>one-shot mc mirror"]
        MINIO[("MinIO<br/>landing-vendor-offline<br/>delta-lake/gold<br/>API :9000 · UI :9001")]
        HIVE["Hive Metastore<br/>Thrift :9083"]
        TRINO["Trino<br/>Delta + PostgreSQL catalogs<br/>host :8083"]
        CLIENT["External SQL client<br/>DBeaver or equivalent"]
    end

    subgraph GOV["Contracts và governance"]
        direction TB
        CONTRACTS["Versioned JSON contracts<br/>contracts/*.json"]
        LINEAGE["Static lineage manifest<br/>datahub/lineage.yml"]
        DHCLI["DataHub CLI ingest<br/>manual host process"]
        DATAHUB["DataHub Quickstart<br/>separate deployment<br/>GMS :8084 · UI :9002"]
    end

    SEED -->|"B0 ticker reference"| GEN
    GEN -->|"B1 Parquet: OHLCV, foreign flow, actions, ratios"| LAND
    GEN -->|"B2 mirror vendor Parquet: landing-vendor-offline"| MINIO
    GEN -->|"B3 SQL refresh: tickers + corporate_actions"| PG
    LAND -->|"B4 recursive Parquet read + schema merge"| SPARK
    PG -->|"B5 JDBC: tickers + corporate_actions"| SPARK
    SPARK -->|"B6 overwrite Delta snapshots: Bronze → Silver → Gold"| DELTA
    DELTA -->|"B7 read local Gold Delta files"| SYNC
    SYNC -->|"B8 mc mirror: delta-lake/gold"| MINIO

    GEN -->|"S1 ticker-keyed JSON events"| KAFKA
    GEN -->|"S1b hourly JSONL replay/audit"| REPLAY
    KAFKA -->|"S2 Spark Kafka source + offsets"| SPARK
    SPARK -->|"S3 append Delta: raw events → trades/quotes/features"| DELTA
    KAFKA -->|"S4 PyFlink source: earliest offsets"| FLINK
    FLINK -->|"S5 checkpointed JSONL: events/quarantine/5-min volume"| FLINKFILES

    MINIO -->|"C1 read Gold Delta objects"| TRINO
    PG -->|"C2 PostgreSQL connector rows"| TRINO
    CLIENT -->|"C3a SQL request"| TRINO
    TRINO -->|"C3b federated result set"| CLIENT
    CLIENT -->|"C4a direct vendor SQL / EXPLAIN"| PG
    PG -->|"C4b result or execution plan"| CLIENT

    HIVE -.->|"M1 schema + table/location metadata"| TRINO
    CONTRACTS -.->|"M2 selected batch contract validation"| AIRFLOW
    AIRFLOW -.->|"M3 schedule/run Spark work"| SPARK
    AIRFLOW -.->|"M4 Flink REST health + running-job check"| FLINK
    AIRFLOW -.->|"M5a write DAG/run/task metadata"| PG
    PG -.->|"M5b read orchestration state"| AIRFLOW
    ZK -.->|"M6a broker coordination metadata"| KAFKA
    KAFKA -.->|"M6b heartbeats/coordination requests"| ZK
    LINEAGE -.->|"M7 static dataset edges"| DHCLI
    DHCLI -.->|"M8 REST ingest"| DATAHUB

    classDef source fill:#E3F2FD,stroke:#1565C0,color:#0D47A1,stroke-width:1.5px;
    classDef orchestration fill:#F3E5F5,stroke:#7B1FA2,color:#4A148C,stroke-width:1.5px;
    classDef processing fill:#FFF3E0,stroke:#EF6C00,color:#E65100,stroke-width:1.5px;
    classDef storage fill:#E8F5E9,stroke:#2E7D32,color:#1B5E20,stroke-width:1.5px;
    classDef query fill:#E0F2F1,stroke:#00796B,color:#004D40,stroke-width:1.5px;
    classDef governance fill:#ECEFF1,stroke:#546E7A,color:#263238,stroke-width:1.5px;
    classDef external fill:#FFFDE7,stroke:#9E9D24,color:#827717,stroke-width:1.5px;

    class SEED,GEN,PG,KAFKA,ZK source;
    class AIRFLOW orchestration;
    class SPARK,FLINK processing;
    class LAND,REPLAY,DELTA,FLINKFILES,SYNC,MINIO storage;
    class HIVE,TRINO query;
    class CONTRACTS,LINEAGE,DHCLI,DATAHUB governance;
    class CLIENT external;

    linkStyle 0,1,2,3,4,5,6,7,8 stroke:#1565C0,stroke-width:2px,color:#1565C0;
    linkStyle 9,10,11,12,13,14 stroke:#F4511E,stroke-width:2px,color:#F4511E;
    linkStyle 15,16,17,18,19,20 stroke:#2E7D32,stroke-width:2px,color:#2E7D32;
    linkStyle 21,22,23,24,25,26,27,28,29,30 stroke:#78909C,stroke-width:1.5px,color:#546E7A;
```

Màu đường nối:

- **Xanh dương (`B*`)**: batch/file/JDBC/Delta snapshot flow.
- **Cam (`S*`)**: streaming event flow.
- **Xanh lá (`C*`)**: query/serving flow.
- **Xám nét đứt (`M*`)**: metadata, orchestration, coordination hoặc governance;
  không phải business-data payload.

### C.1 Phạm vi của sơ đồ

- Box là deployable service hoặc logical storage boundary có thật trong
  code/config. Delta Lake được biểu diễn là storage format trên shared volume,
  không phải service độc lập.
- Spark batch và Spark Structured Streaming dùng chung cluster box nhưng có
  flow riêng.
- PyFlink là nhánh xử lý song song từ Kafka. Không có mũi tên từ
  `data/flink` sang Delta vì repository chưa có adapter cho bước đó.
- DataHub nằm ngoài default Compose. Mũi tên governance chỉ mô tả recipe thủ
  công đã có, không khẳng định một continuous lineage pipeline.

## D. Data flow catalog

### D.1 Batch flow

| ID | From → To | Protocol / interface | Input | Output | Evidence | Confidence |
|---|---|---|---|---|---|---|
| B0 | Frozen seed → Generator | Local JSON file read | Ticker reference records | Selected ticker universe | `generator/seed/__init__.py`; `generator/main.py`; `generator/generators.py` | High |
| B1 | Generator → `data/landing` | Local filesystem, PyArrow/Pandas Parquet | Generated OHLCV, foreign flow, corporate actions, financial ratios | `run_date=...` Parquet; OHLCV partitioned by schema version/trade date | `generator/main.py`; `generator/writers.py` | High |
| B2 | Generator → MinIO | S3-compatible HTTP through MinIO Python client | Mỗi Parquet file vừa tạo | `landing-vendor-offline/<object path>` | `generator/writers.py`; `docker-compose.yml` | High |
| B3 | Generator → PostgreSQL | SQLAlchemy/psycopg2 over PostgreSQL protocol | Ticker and corporate-action DataFrames | Refreshed `tickers`, `corporate_actions` rows | `generator/writers.py`; `scripts/init_vendor_db.sql` | High |
| B4 | Local landing → Spark | Filesystem Parquet read with `mergeSchema=true` | Vendor Parquet drops | Spark DataFrames for Bronze ingestion | `jobs/bronze/offline.py`; `dags/bronze_offline_ingest.py` | High |
| B5 | PostgreSQL → Spark | JDBC (`org.postgresql.Driver`) | `tickers`, `corporate_actions` | `raw_tickers`, `raw_corporate_actions` Delta; action Parquet fallback | `dags/bronze_offline_ingest.py`; `jobs/spark_session.py` | High |
| B6 | Spark → local Delta lakehouse | Delta Lake transaction protocol over filesystem | Landing/Bronze/Silver/Gold inputs | Bronze, Silver, Gold, features, labels, drift and ops tables | `jobs/bronze`; `jobs/silver`; `jobs/gold`; `dags/*.py` | High |
| B7–B8 | Local Gold → MinIO | Read-only bind mount + `mc mirror` | `./data/gold` Delta objects | `delta-lake/gold` mirror | `docker-compose.yml`; `Makefile` | High |

Batch layer outputs confirmed by implementation:

| Layer | Primary outputs |
|---|---|
| Bronze | `raw_ohlcv_daily_v1`, `raw_ohlcv_daily_v2`, `raw_foreign_flow`, `raw_corporate_actions`, `raw_financial_ratios`, `raw_tickers` |
| Silver | `stg_daily_price`, `stg_foreign_flow` |
| Gold/model | `dim_date`, `dim_ticker`, `dim_industry`, `dim_exchange`, `dim_session`, `fact_daily_price`, optional `fact_foreign_flow`, optional `fact_intraday_trade`, `obt_ticker_daily_performance` |
| Feature/ML/monitoring | `feat_ticker_daily`, `feat_ticker_unified`, `ml_ticker_label`, `ml_ticker_training`, conditional `agg_feature_health_daily`, conditional `feature_drift_alerts`, `ops_pipeline_run`, CSV drift report |

### D.2 Streaming flow

| ID | From → To | Protocol / interface | Input | Output | Evidence | Confidence |
|---|---|---|---|---|---|---|
| S1 | Generator → Kafka | Kafka protocol via `kafka-python`; JSON value, ticker key | `trade`, `quote`, `index_update` events | Topic `stock_market_events_v3`, 4 partitions, configured `retention.ms=-1` | `generator/main.py`; `generator/writers.py`; `config/generator.yaml` | High |
| S1b | Generator → replay store | Local JSONL write | Cùng generated events | `data/events/YYYY-MM-DD/HH/events.jsonl` | `generator/main.py`; `generator/writers.py` | High |
| S2 | Kafka → Spark Bronze stream | Spark Kafka source | Raw Kafka key/value, topic, partition, offset, timestamp | `raw_market_events` Delta + checkpoint | `jobs/bronze/stream.py`; `dags/bronze_stream_ingest.py` | High |
| S3a | Bronze Delta → Spark Silver stream | Delta Structured Streaming | `payload_json` + Kafka metadata | `stg_trades`, `stg_quotes`, `stg_events_quarantine` Delta | `jobs/silver/stream.py`; `dags/silver_stream.py` | High |
| S3b | `stg_trades` → Spark feature stream | Delta Structured Streaming | Deduplicated trade events | `feat_stream_intraday` Delta + checkpoint | `jobs/features/stream.py`; `dags/feat_stream_job.py` | High |
| S4–S5 | Kafka → PyFlink → `data/flink` | Flink Kafka connector + checkpointed `FileSink` | Raw JSON events | `stg_events`, `stg_events_quarantine`, `feat_stream_volume_5m` JSONL | `jobs/flink/silver_stream.py`; `docker-compose.yml` | High |

Streaming semantics:

- Spark Bronze mặc định đọc offset `latest`; DAG có thể truyền
  `starting_offsets` và `available_now`.
- PyFlink đọc `earliest`, dùng consumer group có thể cấu hình, parallelism 4,
  checkpoint mỗi 30 giây và 60-second bounded out-of-orderness watermark.
- Spark Silver dùng `dropDuplicatesWithinWatermark(["event_id"])`; record có
  `created_ts - event_ts > watermark_sec` được đánh `_late_flag`.
- PyFlink giữ event đầu tiên cho mỗi `event_id` bằng keyed state. State không có
  TTL trong code hiện tại.
- Spark Silver chỉ materialize accepted `trade` và `quote`; accepted
  `index_update` không có downstream Delta table.
- Flink giữ mọi accepted event trong `stg_events`, nhưng output là JSONL, không
  phải Delta table.

### D.3 Query and serving flow

| ID | From → To | Protocol | Data/result | Evidence | Confidence |
|---|---|---|---|---|---|
| C1 | MinIO → Trino | S3-compatible HTTP via Delta connector | Gold Delta data files under `delta-lake/gold` | `docker/trino/catalog/delta.properties`; `docker-compose.yml` | Medium |
| M1 | Hive Metastore → Trino | Thrift | Table schema and location metadata | `docker/trino/catalog/delta.properties`; `docker-compose.yml` | Medium |
| C2 | PostgreSQL → Trino | JDBC/PostgreSQL connector | `vendor_db` catalog rows | `docker/trino/catalog/postgres.properties` | High |
| C3 | SQL client ↔ Trino | Trino HTTP/SQL | Federated SQL and result sets | `scripts/trino_examples.sql`; `docs/novel_ideas.md` | Medium |
| C4 | SQL client ↔ PostgreSQL | PostgreSQL wire protocol | Direct SQL, results, `EXPLAIN` plans | `docs/storage_optimization.md`; `scripts/init_vendor_db.sql` | Medium |

Trino table registration is **Unverified** as automation: examples assume
catalog/schema/table names such as `delta.gold_stock.fact_daily_price`, nhưng
không có DDL hoặc bootstrap script tạo schema và gọi
`system.register_table`. Evidence documents report a successful manual setup.

### D.4 Metadata, governance and control flow

| ID | From → To | Interface | Payload | Evidence | Confidence |
|---|---|---|---|---|---|
| M2 | Contracts → Airflow/Spark tasks | Local JSON file read | Required/properties rules for selected datasets | `jobs/bronze/offline.py`; relevant batch DAGs | High |
| M3 | Airflow → Spark | PythonOperator invokes PySpark; Spark client connects to `spark://spark-master:7077` | Job configuration and execution control | `dags/*.py`; `jobs/spark_session.py`; `docker-compose.yml` | High |
| M4 | Airflow → Flink | HTTP GET to Flink REST | `/overview`, `/jobs/overview` health/job state | `dags/flink_silver_stream.py` | High |
| M5 | Airflow ↔ PostgreSQL | SQLAlchemy/PostgreSQL | DAG run, task instance and scheduler metadata | `docker-compose.yml` Airflow database connection | High |
| M6 | Kafka ↔ ZooKeeper | ZooKeeper coordination | Membership, heartbeats and broker/topic coordination | `docker-compose.yml` | High |
| M7–M8 | Lineage manifest → DataHub CLI → DataHub | Local YAML then REST | Static dataset-to-dataset lineage edges | `datahub/lineage.yml`; `datahub/recipe.yml`; `datahub/README.md` | Medium |

Contract boundary:

- Confirmed enforcement: offline OHLCV v1/v2 before Bronze write; optional
  batch contracts checked in Airflow for `dim_ticker`, `fact_daily_price` và
  `feat_ticker_daily`.
- **Unverified/Not implemented**: streaming jobs do not load
  `raw_market_events`, `stg_trades`, `stg_quotes`,
  `stg_events_quarantine` or `feat_stream_intraday` contract files even though
  those contracts exist.
- DataHub recipe publishes dataset-level lineage only; it does not publish
  Airflow runs, runtime lineage, contracts, assertions or schema entities.

## E. Orchestration and schedules

| DAG | Schedule/trigger | Main input → output | Retry policy | Explicit dependency boundary |
|---|---|---|---|---|
| `bronze_offline_ingest` | `30 15 * * *` | Local landing + PostgreSQL → Bronze Delta | 3 retries, 5 min | Internal ingest tasks → contract validation → quality validation |
| `silver_daily` | `0 16 * * *` | Bronze daily tables → Silver daily tables | 3 retries, 5 min | Dedup branches → uniqueness → validation fan-out |
| `gold_dimensions_and_facts` | `30 16 * * *` | Silver + Bronze actions → dimensions/facts/OBT | 2 retries, 10 min | Dimensions → facts → OBT → validations → ops metadata |
| `feat_daily_job` | `0 17 * * *` | Gold facts → daily/unified features, labels, drift, training | 2 retries, 10 min | Feature and label branches converge at training |
| `bronze_stream_ingest` | `@once` | Kafka → Bronze Delta | 2 retries, 2 min | One long-running/available-now task |
| `silver_stream` | `@once` | Bronze stream Delta → Silver stream Delta | 2 retries, 2 min | One task starts three streaming queries |
| `feat_stream_job` | `@once` | Silver trades → Gold stream features | 2 retries, 2 min | One streaming query task |
| `flink_silver_stream` | `@once` | Flink REST state → validation result | 2 retries, 2 min | Health check → exactly-one-running-job check |
| `delta_maintenance` | `0 3 * * 0` | Gold Delta → compacted/Z-ordered Gold Delta | 1 retry, 10 min | One maintenance task |

Các batch DAG chỉ được xếp lịch lệch nhau 30 phút. Không có
`ExternalTaskSensor`, Airflow Dataset, `TriggerDagRunOperator` hoặc một parent
DAG nối DP1 → DP2 → DP3. Vì vậy cross-DAG dependency là **Inferred by schedule,
not enforced**. Tương tự, Airflow Flink DAG chỉ monitor; submit/cancel
application được thực hiện thủ công bằng `make flink-submit` hoặc Flink CLI.

## F. Storage, network and deployment topology

### F.1 Persistent and bind-mounted storage

| Storage | Mount / location | Producers | Consumers |
|---|---|---|---|
| `postgres_data` named volume | PostgreSQL `/var/lib/postgresql/data` | Generator, Airflow | Spark JDBC, Trino, SQL client, Airflow |
| `minio_data` named volume | MinIO `/data` | Generator upload, bucket init, Gold sync | Trino Delta connector |
| `hive_metastore_data` named volume | Hive `/opt/hive/data/warehouse` | Hive Metastore | Hive Metastore |
| Shared `./data` bind mounts | Paths vary by service | Generator, Spark, Flink | Spark, Airflow, sync/capture tools, host |
| Source/config bind mounts | `jobs`, `dags`, `config`, `contracts`, seed | Host repository | Spark, Airflow, Flink |

Không có custom `networks:` block. Tất cả Compose services dùng default Compose
network và resolve nhau bằng service DNS names như `postgres`, `minio`,
`kafka`, `spark-master`, `flink-jobmanager`, `hive-metastore`.

### F.2 Declared startup dependencies

| Service | Declared dependency |
|---|---|
| Generator | Healthy PostgreSQL, MinIO and Kafka |
| MinIO bucket init / Gold sync | Healthy MinIO |
| Kafka | ZooKeeper |
| Spark worker | Spark master |
| Spark capture | Spark master + worker |
| Flink TaskManager | Flink JobManager |
| Airflow webserver/scheduler | Healthy PostgreSQL |
| Hive Metastore | None declared |
| Trino | None declared |

`Trino → Hive → MinIO` startup order therefore không được Compose enforce.
Healthcheck cũng không được khai báo cho Spark, Flink, Airflow, Hive hoặc Trino.

### F.3 Environment configuration surface

`.env.example` chỉ khai báo tên biến `MINIO_ACCESS_KEY` và
`MINIO_SECRET_KEY`. Code/Compose còn đọc các tên biến sau:

`BRONZE_DIR`, `CONTRACTS_DIR`, `CORPORATE_ACTIONS_PATH`, `DATA_DIR`,
`DATA_ROOT`, `DRIFT_REPORT_PATH`, `DRIFT_START_DATE`, `FLINK_KAFKA_GROUP_ID`,
`FLINK_OUTPUT_DIR`, `FLINK_REST_URL`, `GOLD_DIR`, `KAFKA_BROKER`,
`KAFKA_TOPIC`, `MINIO_ACCESS_KEY`, `MINIO_ENDPOINT`, `MINIO_SECRET_KEY`,
`POSTGRES_DSN`, `SEED_PATH`, `SILVER_DIR`, `SPARK_DRIVER_HOST`,
`SPARK_IVY_DIR`, `SPARK_MASTER_URL`, `STREAM_TICKER_COUNT`.

Tài liệu này chỉ liệt kê tên biến, không đọc hoặc hiển thị giá trị từ `.env`.

## G. Observability and operations

### Implemented

- Python `logging` và Airflow task logs cho generator/jobs.
- Spark master UI `:8080`; optional capture driver UI `:4040`.
- Flink REST/UI `:8081`, checkpoint metrics và Airflow health/job monitor.
- Airflow UI `:8082` và PostgreSQL-backed orchestration state.
- MinIO console `:9001`.
- `ops_pipeline_run` Delta table được append sau Gold DAG thành công.
- `agg_feature_health_daily`, conditional `feature_drift_alerts` và CSV drift
  report cho feature PSI.
- Container healthchecks cho PostgreSQL, MinIO và Kafka.

### Not found / TBD

- Metrics collector/exporter, Prometheus, Grafana: **TBD / not present**.
- Distributed tracing/OpenTelemetry/Jaeger/Zipkin: **TBD / not present**.
- Central log aggregation: **TBD / not present**.
- Email/Slack/PagerDuty/webhook alert delivery: **TBD / not present**.
- Automated freshness/SLO monitoring: design notes exist, runtime service
  **not present**.
- Automated quarantine replay/reprocessing: **not present**.

## H. Confirmed gaps, inferred links and inconsistencies

| Item | Status | Why |
|---|---|---|
| Bronze batch reads MinIO landing directly | **Not implemented** | `read_landing_parquet()` glob-reads `data/landing`; MinIO receives a mirror only |
| Spark writes production Delta directly to MinIO | **Not implemented** | Jobs use local paths; `get_spark_with_minio()` has no caller |
| Gold mirror to MinIO | **Confirmed but manual/profile-gated** | `minio-sync-gold` runs only through tools profile/Make target |
| Hive/Trino Gold registration | **Unverified automation** | Connector config exists; no schema/table registration bootstrap is present |
| Trino federated query capability | **Configured; manual setup evidence** | Both connector files and SQL examples exist; runtime lifecycle is not automated |
| Explicit DP1 → DP2 → DP3 DAG dependency | **Not implemented** | Only cron ordering exists across DAGs |
| PyFlink output feeds Spark/Delta/Gold | **Not implemented** | Flink sinks JSONL under `data/flink`; no downstream reader exists |
| Flink application submission by Airflow | **Not implemented** | Airflow DAG only calls REST for health and running-job validation |
| Streaming JSON contracts enforced at runtime | **Not implemented** | Contract files exist but streaming code never loads them |
| `raw_tickers` feeds `dim_ticker` | **Not implemented** | Gold dimension reads seed JSON, not Bronze `raw_tickers` |
| Financial ratios reach Silver/Gold | **Not implemented** | Flow ends at `raw_financial_ratios` |
| Index events reach a Spark Silver table | **Not implemented** | Accepted index events are neither trades nor quotes |
| Corporate-action point-in-time correctness | **Partial** | `fact_daily_price` applies future ex-date factors but does not use `announced_ts` knowledge time |
| Batch tables are incremental MERGE/upsert | **Not implemented** | Current code uses Delta `overwrite` for most batch outputs |
| DataHub runtime lineage/contracts/assertions | **Not implemented** | Recipe publishes static dataset edges only |
| DataHub availability in default stack | **Not present** | Quickstart is documented as a separate deployment |

## I. Evidence precedence

Khi source/config và tài liệu cũ khác nhau, kiến trúc này ưu tiên:

1. `docker-compose.yml`, Dockerfiles và mounted paths.
2. `generator/`, `jobs/`, `dags/`, `scripts/`.
3. `contracts/`, Trino/Hive/DataHub configuration.
4. Tests và current data artifacts để kiểm chứng.
5. README/docs/evidence như nguồn mô tả phụ.

Ví dụ, README mô tả một số batch layer là append/incremental merge và Bronze
đọc MinIO, nhưng implementation hiện tại dùng local landing + Delta
`overwrite`. Tài liệu này ghi theo implementation và đánh dấu rõ các bước chỉ
được cấu hình hoặc thực hiện thủ công.
