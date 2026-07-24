"""Persist generated data to vendor-like file, object, stream, and SQL stores."""

import json
import logging
import os
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd

logger = logging.getLogger(__name__)


def _upload_to_minio(local_path: str, bucket: str, object_name: str) -> bool:
    """Upload one local file to MinIO and report whether the upload succeeded."""
    try:
        from minio import Minio

        raw_endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        parsed_endpoint = urlparse(
            raw_endpoint if "://" in raw_endpoint else f"http://{raw_endpoint}"
        )
        endpoint = parsed_endpoint.netloc or parsed_endpoint.path
        access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=parsed_endpoint.scheme == "https",
        )
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
        client.fput_object(bucket, object_name, local_path)
        logger.info("Uploaded to MinIO %s/%s", bucket, object_name)
        return True
    except ImportError:
        logger.warning("minio package not installed — skipping MinIO upload")
    except Exception as e:
        logger.error("MinIO upload failed: %s", e)
    return False


def write_parquet_landing(
    df: pd.DataFrame,
    table_name: str,
    run_date: str,
    base_dir: str = "data/landing",
) -> str:
    """Write a deterministic Parquet vendor drop and mirror it to MinIO."""
    landing_path = Path(base_dir) / f"run_date={run_date}" / table_name
    landing_path.mkdir(parents=True, exist_ok=True)
    out_file = landing_path / "part-00000.parquet"
    df.to_parquet(out_file, index=False)
    _upload_to_minio(
        str(out_file),
        "landing-vendor-offline",
        f"run_date={run_date}/{table_name}/part-00000.parquet",
    )
    return str(out_file)


def write_ohlcv_landing(
    df: pd.DataFrame,
    run_date: str,
    base_dir: str = "data/landing",
) -> list[str]:
    """Write physical v1/v2 OHLCV schemas partitioned by version and trade date."""
    required = {"_schema_version", "trade_date"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"OHLCV schema-evolution columns missing: {sorted(missing)}")

    root = Path(base_dir) / f"run_date={run_date}" / "ohlcv_daily"
    # A previous flat-file run must not be mixed with the physical v1/v2
    # partitions, otherwise a recursive Bronze read would count both layouts.
    for stale_file in root.glob("*.parquet"):
        stale_file.unlink()
    outputs = []
    for (version, trade_date), partition in df.groupby(
        ["_schema_version", "trade_date"],
        sort=True,
    ):
        version = int(version)
        vendor_partition = partition.copy()
        if version == 1:
            vendor_partition = vendor_partition.drop(
                columns=["value", "foreign_room"],
                errors="ignore",
            )
        partition_path = root / f"schema_version={version}" / f"trade_date={trade_date}"
        partition_path.mkdir(parents=True, exist_ok=True)
        output = partition_path / "part-00000.parquet"
        vendor_partition.drop(
            columns=["trade_date", "_schema_version"],
        ).to_parquet(output, index=False)
        outputs.append(str(output))
        object_name = output.relative_to(Path(base_dir)).as_posix()
        _upload_to_minio(str(output), "landing-vendor-offline", object_name)
    return outputs


def write_jsonl(
    events: list[dict],
    td_date,
    base_dir: str = "data/events",
) -> list[str]:
    """Write idempotent hourly JSONL replay files and return their paths."""
    d = td_date.isoformat() if hasattr(td_date, "isoformat") else str(td_date)
    grouped: dict[str, list[dict]] = defaultdict(list)
    for event in events:
        ts = event.get("event_timestamp", "")
        hour = ts[11:13] if len(ts) >= 13 else "00"
        grouped[hour].append(event)

    outputs = []
    for hour, hourly_events in sorted(grouped.items()):
        p = Path(base_dir) / d[:10] / hour
        p.mkdir(parents=True, exist_ok=True)
        output = p / "events.jsonl"
        with output.open("w", encoding="utf-8") as file_handle:
            for event in hourly_events:
                file_handle.write(json.dumps(event, default=str) + "\n")
        outputs.append(str(output))
    return outputs


def write_to_kafka(
    events: list[dict],
    topic: str,
    bootstrap_servers: str | None = None,
    partitions: int = 4,
    retention_ms: int = -1,
) -> bool:
    """Create the replay topic, then publish ticker-keyed events to Kafka."""
    bootstrap_servers = bootstrap_servers or os.getenv(
        "KAFKA_BROKER",
        "localhost:9092",
    )
    try:
        from kafka import KafkaProducer
        from kafka.admin import (
            ConfigResource,
            ConfigResourceType,
            KafkaAdminClient,
            NewPartitions,
            NewTopic,
        )
        from kafka.errors import TopicAlreadyExistsError

        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id="vnstock-generator-topic-init",
        )
        try:
            admin.create_topics(
                [
                    NewTopic(
                        name=topic,
                        num_partitions=partitions,
                        replication_factor=1,
                        topic_configs={"retention.ms": str(retention_ms)},
                    )
                ]
            )
            logger.info(
                "Created Kafka topic '%s' with %d partitions and retention.ms=%d",
                topic,
                partitions,
                retention_ms,
            )
        except TopicAlreadyExistsError:
            description = admin.describe_topics([topic])[0]
            current_partitions = len(description["partitions"])
            if current_partitions < partitions:
                admin.create_partitions({topic: NewPartitions(partitions)})
                logger.info(
                    "Expanded Kafka topic '%s' from %d to %d partitions",
                    topic,
                    current_partitions,
                    partitions,
                )
            elif current_partitions > partitions:
                logger.warning(
                    "Kafka topic '%s' already has %d partitions (configured %d)",
                    topic,
                    current_partitions,
                    partitions,
                )
            else:
                logger.info(
                    "Kafka topic '%s' already has %d partitions",
                    topic,
                    current_partitions,
                )
            admin.alter_configs(
                [
                    ConfigResource(
                        ConfigResourceType.TOPIC,
                        topic,
                        configs={"retention.ms": str(retention_ms)},
                    )
                ]
            )
            logger.info(
                "Applied retention.ms=%d to Kafka topic '%s'",
                retention_ms,
                topic,
            )
        finally:
            admin.close()

        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks="all",
            retries=5,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda value: value.encode("utf-8"),
        )
        for event in events:
            event_time = datetime.fromisoformat(
                str(event["event_timestamp"]).replace("Z", "+00:00")
            )
            if event_time.tzinfo is None:
                event_time = event_time.replace(tzinfo=timezone.utc)
            producer.send(
                topic,
                key=event.get("ticker") or "INDEX",
                value=event,
                timestamp_ms=int(event_time.timestamp() * 1000),
            )
        producer.flush()
        logger.info("Published %d events to Kafka topic '%s'", len(events), topic)
        return True
    except ImportError:
        logger.warning("kafka-python not installed — skipping Kafka publish, JSONL file-sink only")
    except Exception as e:
        logger.error("Kafka publish failed: %s — events written to JSONL file-sink only", e)
    return False


def write_to_postgres(
    df: pd.DataFrame,
    table_name: str,
    dsn: str | None = None,
) -> bool:
    """Refresh a pre-created vendor reference table without duplicating rows."""
    if dsn is None:
        dsn = os.getenv("POSTGRES_DSN", "postgresql://vendor:vendor@localhost:5433/vendor_db")
    try:
        from sqlalchemy import create_engine, inspect, text

        engine = create_engine(dsn.replace("postgresql://", "postgresql+psycopg2://"))
        if not inspect(engine).has_table(table_name):
            raise RuntimeError(
                f"Table {table_name!r} is missing; run scripts/init_vendor_db.sql first."
            )
        with engine.begin() as connection:
            connection.execute(text(f'TRUNCATE TABLE "{table_name}" CASCADE'))
        df.to_sql(table_name, engine, if_exists="append", index=False)
        logger.info("Wrote %d rows to vendor_db.%s", len(df), table_name)
        return True
    except ImportError:
        logger.warning("psycopg2/sqlalchemy not installed — skipping PostgreSQL write")
    except Exception as e:
        logger.error("PostgreSQL write failed: %s", e)
    return False


def generate_quality_report(
    ohlcv: pd.DataFrame,
    events: list[dict],
    cfg: dict,
    schema_change_date,
    tickers_df=None,
    output_path: str = "data/quality_report.md",
):
    """Auto-generate quality report covering all rubric items."""
    rng = cfg.get("random_seed", "N/A")
    dup_rate_offline = cfg.get("duplicate_rate_offline", 0)
    dup_rate_stream = cfg.get("duplicate_rate_stream", 0)
    late_rate = cfg.get("late_arrival_rate", 0)
    burst_mult = cfg.get("burst_multiplier", 1)
    base_epm = cfg.get("base_events_per_min", 0)

    # ── Skew: VN30 + Industry ──
    if tickers_df is not None and "vn30" in tickers_df.columns:
        vn30_ids = set(tickers_df[tickers_df["vn30"]]["ticker_id"])
        vn30_vol = ohlcv[ohlcv["ticker_id"].isin(vn30_ids)]["volume"].sum()
        non_vn30_vol = ohlcv[~ohlcv["ticker_id"].isin(vn30_ids)]["volume"].sum()
    else:
        top30 = ohlcv.groupby("ticker_id")["volume"].sum().nlargest(30).index
        vn30_vol = ohlcv[ohlcv["ticker_id"].isin(top30)]["volume"].sum()
        non_vn30_vol = ohlcv[~ohlcv["ticker_id"].isin(top30)]["volume"].sum()
    total_vol = ohlcv["volume"].sum()

    industry_shares = cfg.get("industry_value_share", {})
    industry_lines = []
    for ind, share in industry_shares.items():
        industry_lines.append(f"  - {ind}: {share * 100:.0f}%")
    if not industry_lines:
        industry_lines.append("  (no industry config)")

    # ── Cardinality ──
    n_tickers = ohlcv["ticker_id"].nunique()
    n_keys = ohlcv.groupby(["ticker_id", "trade_date"]).ngroups
    n_dates = ohlcv["trade_date"].nunique()

    # ── Schema evolution ──
    v1_rows = (ohlcv.get("_schema_version", pd.Series([1] * len(ohlcv))) == 1).sum()
    v2_rows = (ohlcv.get("_schema_version", pd.Series([1] * len(ohlcv))) == 2).sum()
    froom_null = ohlcv["foreign_room"].isna().sum()
    value_null = ohlcv["value"].isna().sum()

    # ── Duplicate rate (offline) ──
    total_rows = len(ohlcv)
    dup_count = total_rows - ohlcv.drop_duplicates(subset=["ticker_id", "trade_date"]).shape[0]
    dup_pct = dup_count / max(total_rows, 1) * 100

    # ── Streaming quality ──
    n_events = len(events)
    if n_events > 0:
        late_events = sum(
            1 for e in events if e.get("event_timestamp", "") != e.get("created_ts", "")
        )
        event_ids = [e.get("event_id") for e in events]
        dup_stream = n_events - len(set(event_ids))
        # Burst profile: count events per minute
        minute_buckets = Counter()
        for e in events:
            ts = e.get("event_timestamp", "")
            if len(ts) >= 16:
                minute_buckets[ts[:16]] += 1
        peak_epm = max(minute_buckets.values()) if minute_buckets else 0
        avg_epm = n_events / max(len(minute_buckets), 1)
    else:
        late_events, dup_stream, peak_epm, avg_epm = 0, 0, 0, 0

    # ── Data volume & format ──
    lines = [
        "# Data Quality Report",
        "",
        "## Generator Configuration",
        f"- n_tickers: {cfg.get('n_tickers', 'N/A')}",
        f"- vn30_count: {cfg.get('vn30_count', 'N/A')}",
        f"- days_history: {cfg.get('days_history', 'N/A')}",
        f"- random_seed: {rng}",
        f"- trading_calendar: {cfg.get('trading_calendar', 'N/A')}",
        f"- schema_change_date: {schema_change_date}",
        f"- price_limit: HOSE ±{cfg.get('price_limit_pct', {}).get('HOSE', 'N/A')} | HNX ±{cfg.get('price_limit_pct', {}).get('HNX', 'N/A')} | UPCOM ±{cfg.get('price_limit_pct', {}).get('UPCOM', 'N/A')}",
        "",
        "## Data Volume & Format",
        f"- Offline rows (OHLCV): {total_rows:,}",
        "- Offline format: Parquet (snappy), partitioned by trade_date",
        "- Offline tables: ohlcv_daily, foreign_flow_daily, corporate_actions, financial_ratios",
        f"- Streaming events: {n_events:,}",
        f"- Streaming format: JSON (Kafka topic: {cfg.get('kafka_topic', 'stock_market_events_v3')}) + JSONL file-sink",
        f"- Trading days in window: {n_dates}",
        "",
        "## Skew Distribution",
        f"- VN30 volume share: {vn30_vol / max(total_vol, 1) * 100:.1f}% ({vn30_vol:,.0f} / {total_vol:,.0f})",
        f"- Non-VN30 volume share: {non_vn30_vol / max(total_vol, 1) * 100:.1f}% ({non_vn30_vol:,.0f} / {total_vol:,.0f})",
        f"- Total volume: {total_vol:,}",
        "- Industry value share (configured):",
        *industry_lines,
        "",
        "## Cardinality",
        f"- approx_count_distinct(ticker_id): {n_tickers}",
        f"- approx_count_distinct(ticker_id, trade_date): {n_keys}",
        f"- approx_count_distinct(trade_date): {n_dates}",
        f"- High-cardinality composite key: ticker_id × trade_date = {n_keys} unique pairs",
        "",
        "## Schema Evolution",
        f"- Schema change date: {schema_change_date}",
        f"- Rows with _schema_version=1 (pre-change): {v1_rows}",
        f"- Rows with _schema_version=2 (post-change): {v2_rows}",
        f"- foreign_room IS NULL (v1 partitions, missing by design): {froom_null}",
        f"- value IS NULL (v1 partitions, missing by design): {value_null}",
        "",
        "## Duplicate Rate (Offline)",
        f"- Configured duplicate rate: {dup_rate_offline * 100:.1f}%",
        f"- Rows before dedup: {total_rows:,}",
        f"- Duplicate rows detected: {dup_count}",
        f"- Duplicate rate (actual): {dup_pct:.1f}%",
        "- Dedup key: (ticker_id, trade_date)",
        "",
        "## Streaming Quality",
        f"- Total events: {n_events:,}",
        f"- Late arrivals: {late_events} / {n_events} ({late_events / max(n_events, 1) * 100:.1f}%)",
        f"- Configured late rate: {late_rate * 100:.1f}% (delay {cfg.get('late_delay_sec_min_max', [0, 0])[0]}-{cfg.get('late_delay_sec_min_max', [0, 0])[1]}s)",
        f"- Duplicate events (stream): {dup_stream} / {n_events} ({dup_stream / max(n_events, 1) * 100:.1f}%)",
        f"- Configured stream dup rate: {dup_rate_stream * 100:.1f}%",
        "",
        "## Streaming Burst Profile",
        f"- Baseline events/min: {base_epm}",
        f"- Burst multiplier: ×{burst_mult} (ATO/ATC: {cfg.get('burst_windows', ['N/A'])[0]} & {cfg.get('burst_windows', [None, 'N/A'])[1] if len(cfg.get('burst_windows', [])) > 1 else ''})",
        f"- Peak events/min: {peak_epm:,}",
        f"- Average events/min: {avg_epm:.0f}",
        f"- Peak-to-average ratio: {peak_epm / max(avg_epm, 1):.1f}×",
        "",
        f"*Generated with seed={rng}*",
    ]
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        f.write("\n".join(lines))
    logger.info("Quality report -> %s", output_path)
