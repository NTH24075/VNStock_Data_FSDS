# Rubric evidence checklist

This file maps the marking rubric to reproducible project evidence. The
reference execution below was verified on 2026-07-23 with seed 42. Nineteen
real screenshots are embedded beside their explanations in the topic
documents. Evidence gaps remain explicit; the project does not fabricate
placeholder screenshots.

## Scope boundary

The implementation deliberately stays inside Kỳ 1, Modules 1–12 of the
knowledge checklist: Linux, Python, databases, validation, containers,
ingestion, storage, Spark, Flink/streaming, consumption and Airflow
orchestration. ML serving, Kubernetes, Kubeflow, IaC and CI/CD from Kỳ 2 are
out of scope.

## Verified reference execution

### Offline DP1–DP3

| Stage | Successful Airflow run | Duration | Verified output |
|---|---|---:|---:|
| Bronze offline | `manual__2026-07-23T17:26:00+00:00` | 312.8 s | 73,440 OHLCV input rows |
| Silver daily | `manual__2026-07-23T17:30:00+00:00` | 388.5 s | 72,000 deduplicated price rows |
| Gold dimensions/facts | `manual__2026-07-23T17:40:00+00:00` | 446.6 s | 72,000 daily facts and OBT rows |
| DP3 features/labels/drift | `manual__2026-07-23T17:50:00+00:00` | 1,262.6 s | 72,000 features; 70,800 labels/training rows |

Other checked Gold results:

- `dim_date`: 263 rows.
- `dim_ticker`: 475 SCD2 versions, 400 current rows.
- Label positive rate: 38.4%.
- `agg_feature_health_daily`: 12 rows and 6 alerts.
- Volatility PSI after the configured regime change: 7.9772 and 17.4301.
- Reviewer-readable output:
  [`../data/gold/drift_validation_report.csv`](../data/gold/drift_validation_report.csv).

### Streaming path

| Stage | Successful run/job | Duration/state | Verified output |
|---|---|---:|---:|
| Bronze Kafka replay | `manual__2026-07-23T18:18:00+00:00` | 125.7 s | 197,925 raw events |
| Silver Spark stream | `manual__2026-07-23T18:21:00+00:00` | 148.5 s | 165,668 trades; 23,358 quotes |
| Gold stream features | `manual__2026-07-23T18:24:00+00:00` | 248.9 s | 17,845 feature windows |
| Flink Airflow monitor | `manual__2026-07-23T19:36:00+00:00` | 60.4 s | cluster healthy; v10 is sole running job |
| Flink application | `377ab5e5eb029d8f905db390c54549b6` | `RUNNING`, 12/12 tasks | 195,000 deduped events; 17,845 five-minute windows |

Bronze event distribution was 168,102 trades, 23,756 quotes and 6,067 index
updates. Silver removed duplicate trade/quote keys. No quarantine table was
created because the reference replay contained no schema-invalid records.
`fact_intraday_trade` contains 165,668 rows. Stream features cover 09:05–14:40
and contain 2,667 burst flags.

The final Kafka topic uses four ticker-keyed partitions and
`retention.ms=-1`. This preserves the deterministic historical event
timestamps and lets all four Flink source subtasks consume in parallel. The
v10 source read all 197,925 records from offset zero; keyed state removed
exactly 2,925 replays. Its trade-window input was 165,668, matching the
independent Spark Silver result, and its committed filesystem output was
195,000 Silver events plus 17,845 windows. Quarantine remained empty because
the generated 5–45 second delays are inside the configured 60-second
watermark/grace policy. All four final output watermarks were
`2025-10-09 14:43:58 UTC`; 23/23 checkpoints completed with zero failures.

The three Spark streaming DAGs accept `{"available_now": true}` for a finite,
reviewable Kafka replay. Their normal default remains continuous processing
with persistent Delta checkpoints.

### Serving and federation

Gold Delta data was mirrored to `s3://delta-lake/gold` in MinIO and registered
in the Hive Metastore. Trino verification returned:

| Query | Result |
|---|---:|
| `SELECT count(*) FROM postgres.public.tickers` | 400 |
| `SELECT count(*) FROM delta.gold_stock.fact_daily_price` | 72,000 |
| Delta daily fact joined to PostgreSQL tickers | 72,000 |
| `delta.gold_stock.fact_intraday_trade` | 165,668 |
| `delta.gold_stock.feat_stream_intraday` | 17,845 |

The executable examples are in
[`../scripts/trino_examples.sql`](../scripts/trino_examples.sql).

### Automated verification

```text
Integration: 1 passed
Unit tests (Spark-enabled): 43 passed
Total:       44 passed
Host fast suite: 30 passed, 13 Spark-dependent skipped
Ruff:        All checks passed
git diff --check: clean
```

The two interrupted Bronze attempts at 18:15 and 18:16 were explicitly marked
failed after recovery. The successful 18:18 replay is retained as the evidence
run, so Airflow has no falsely running job from the interrupted session.

## Reproducible outputs

- Generator quality: [`evidence/generator_quality_report.md`](evidence/generator_quality_report.md)
- Flink v10 counters and reproduction:
  [`evidence/flink_v10_verification.md`](evidence/flink_v10_verification.md)
- Docker size measurement and method: [`docker.md`](docker.md)
- Spark/Flink implementation and commands: [`processing_jobs.md`](processing_jobs.md)
- Storage implementation: [`storage_optimization.md`](storage_optimization.md)
- DP1/DP2/DP3 and contracts: [`orchestration_governance.md`](orchestration_governance.md)
- Full schema model: [`schema_design.md`](schema_design.md)
- Novel ideas: [`novel_ideas.md`](novel_ideas.md)

## Rubric-to-evidence map

| Rubric area | Evidence and status |
|---|---|
| README and deployment diagram | **Documented:** root contents, deployable-unit Mermaid diagram, numbered data/control flows and repository tree |
| Docker | **Partial:** Compose, multi-stage Dockerfile and recorded 195,099,639 → 158,641,463 bytes (18.7%); no screenshot |
| Offline generator problems | **Captured:** Figures 01/03 show VN30 skew, 400 tickers, 72,000 composite keys, physical v1/v2 semantics and 2% duplicates |
| Streaming generator problems | **Captured:** Figure 02 shows ×25 bursts, late arrivals, 1.5% duplicate IDs and 5,243 events/min peak |
| Landing storage | **Captured:** Figures 04/05 show MinIO schema-version and trade-date partitions |
| Spark processing | **Partial:** capture runner, AQE/broadcast/dedup/schema code and Airflow integration exist; baseline/optimized Spark UI images absent |
| Flink processing | **Captured:** Figures 10–14 show RUNNING graph, checkpoints, records, final-state backpressure and event-time window code |
| Lakehouse storage | **Partial:** partition/compaction/Z-order code exists; no before/after measurement screenshot |
| PostgreSQL storage | **Captured:** Figure 18 shows `Index Scan`, condition, buffers and planning/execution time; secondary indexes remain code evidence |
| DP1/DP2/DP3 | **Captured:** Figures 06–09 show successful scheduled Bronze, Silver, Gold and Feature graphs |
| Governance | **Partial:** versioned contracts and DataHub dataset-lineage recipe exist; no DataHub UI proof and no published assertion/contract entities |
| Schema design | **Partial captured:** Figures 15–17 show a partial Gold ER, populated SCD2 fields and feature timestamps; ER does not show all zones/tables |
| Novel idea 1 | **Captured:** Figure 19 shows a 72,000-row Trino join across Delta and PostgreSQL |
| Novel idea 2 | **Partial:** executable versioned JSON contracts plus successful DP1 gate; no deliberate failure/task-log screenshot |

## Screenshot inventory

| Figures | Topic document | What they prove |
|---|---|---|
| 01–05 | [`generator.md`](generator.md) | offline/stream quality, config and MinIO landing layout |
| 06–09 | [`orchestration_governance.md`](orchestration_governance.md) | Airflow DP1, DP2 Silver/Gold and DP3 task order/success |
| 10–14 | [`processing_jobs.md`](processing_jobs.md) | Flink graph, checkpoints, metrics, drained-state backpressure and code |
| 15–17 | [`schema_design.md`](schema_design.md) | partial Gold relationships, SCD2 rows and feature timestamps |
| 18 | [`storage_optimization.md`](storage_optimization.md) | PostgreSQL index-backed lookup |
| 19 | [`novel_ideas.md`](novel_ideas.md) | Trino cross-catalog federation result |

The deterministic reference run uses seed 42, 400 tickers, 180 trading days
and the schema boundary 2025-07-01. The Airflow screenshots show scheduled
runs at 15:30/16:00/16:30/17:00 UTC; the separate manual run IDs in the
reference-execution table are retained as textual execution records and are
not assigned to those screenshots.

## Uncaptured rubric evidence

- Docker baseline/optimized terminal or Docker Desktop image comparison.
- Spark driver UI baseline and optimized SQL/Stages comparison on the same
  workload.
- Delta compaction/Z-order before/after file and query statistics.
- DataHub DP1–DP3 lineage, validation and contract/schema entities.
- A full all-zone DBeaver ER diagram.
- A deliberate contract-failure Airflow task log.
