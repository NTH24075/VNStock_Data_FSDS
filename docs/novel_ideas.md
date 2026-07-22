# Novel Ideas

> 10 points total (5 each). Document idea + proof it worked.
> Ideas must go beyond EDAI curriculum (M1-M12).

---

## Idea 1: Trino Federated Query Engine — Cross-Source SQL Without Data Movement

**What:** Trino acts as a single SQL query interface across **Delta Lake on MinIO** (object storage) and **PostgreSQL** (relational database), enabling cross-source JOINs without ETL data movement. Trino catalog files are configured in `docker/trino/catalog/`:
- `delta.properties` — connects to Delta Lake tables stored on MinIO (S3-compatible)
- `postgres.properties` — connects to PostgreSQL `vendor_db` for reference data

**Why beyond EDAI:** M8 covers storage concepts (lakehouse, Delta, Trino at name level) and M11 mentions OLAP engines at concept level (`📖 mức khái niệm`) — but neither module covers actually deploying, configuring, and querying a federated query engine across heterogeneous sources. This implementation exercises: catalog configuration, cross-source query optimization, schema discovery from Delta + JDBC, and practical query patterns that differ from single-source Spark SQL.

**Implementation:**
- Trino container in `docker-compose.yml` (port 8083)
- Catalog files: `docker/trino/catalog/delta.properties` (MinIO-backed Delta) + `postgres.properties` (vendor_db)
- Query examples: `scripts/trino_examples.sql` demonstrates cross-source queries (e.g., JOIN Delta Gold tables with PostgreSQL ticker reference)
- Gold layer tables are exposed through Trino for BI tool access (Superset/Metabase)
- Benefit: analysts query Gold data via SQL without needing Spark; Trino optimizes reads at the Delta file level (predicate pushdown, partition pruning)

**Proof:** [screenshot] Run Trino queries from `scripts/trino_examples.sql` via Trino CLI or DBeaver connected to Trino (port 8083). Show:
1. `SHOW CATALOGS` — showing both `delta` and `postgres` catalogs
2. Cross-source JOIN query between `delta.gold_stock.fact_daily_price` and `postgres.vendor_db.tickers`
3. Query plan (`EXPLAIN`) showing partition pruning and file-level skipping

---

## Idea 2: Versioned Data Contracts with JSON Schema — Catch Unannounced Schema Changes Before They Corrupt Bronze

**What:** Every Bronze table has a **versioned JSON Schema contract** (`contracts/*.v1.json`, `.v2.json`) that defines required columns, types, and nullability per `_schema_version`. The Bronze ingestion job validates incoming data against the contract version matching its `_schema_version` tag *before* writing to Delta. A column present in the data but absent from any known contract version (an unannounced vendor change) fails the run — this is a **contract violation**, distinct from a planned schema evolution (v1→v2).

**Why beyond EDAI:** M5 covers software testing (pytest, TDD) and M12 mentions "data validation fundamentals: circuit breaker, validation frameworks, data contracts" at concept level — but neither covers implementing a production-grade contract enforcement pattern. This idea demonstrates: JSON Schema as a lightweight alternative to Schema Registry (no Kafka dependency), versioned contract evolution, the distinction between "evolution" (planned) and "violation" (unplanned), and integration with the ingestion pipeline as a quality gate.

**Implementation:**
- Contract files: `contracts/raw_ohlcv_daily.v1.json` (6 required columns), `.v2.json` (8 required columns — added `value`, `foreign_room`)
- `jobs/bronze/offline.py`: `validate_contract(df, contract, version)` raises `ValueError` on missing required columns
- `load_contract(version)` reads the correct contract file per `_schema_version`
- Generator stamps `_schema_version` per row based on `schema_change_date` — old partitions get v1, new partitions get v2
- Delta table property `autoMerge.enabled = false` enforces that every schema change must go through the contract path
- `contracts/raw_market_events.v1.json`, `stg_trades.v1.json`, `stg_quotes.v1.json`, `feat_stream_intraday.v1.json` extend the pattern to the streaming path

**Proof:** [screenshot] Show:
1. Contract files in `contracts/` folder listing
2. Bronze ingestion log showing validation pass: "v1: X rows OK, v2: Y rows OK"
3. Deliberate contract violation test: modify generator to add an unregistered column, show ingestion fails with "Contract v1 violation: missing required columns {'new_column'}" in Airflow task log
