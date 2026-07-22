"""Output writers — Parquet to MinIO, JSON to Kafka + file-sink, PostgreSQL reference tables."""

import json
import os
from pathlib import Path
from typing import Optional

import pandas as pd


def write_parquet_landing(df: pd.DataFrame, table_name: str, run_date: str, base_dir: str = "data/landing"):
    """Write Parquet files simulating a vendor file drop in landing-vendor-offline/."""
    landing_path = Path(base_dir) / f"run_date={run_date}" / table_name
    landing_path.mkdir(parents=True, exist_ok=True)
    out_file = landing_path / f"part-00000.parquet"
    df.to_parquet(out_file, index=False)
    return str(out_file)


def write_jsonl(events: list[dict], td_date, base_dir: str = "data/events"):
    """Write events as JSONL file-sink (vendor audit log), partitioned by date/hour."""
    d = td_date if hasattr(td_date, "isoformat") else str(td_date)
    for event in events:
        ts = event.get("event_timestamp", "")
        hour = ts[11:13] if len(ts) >= 13 else "00"
        p = Path(base_dir) / d[:10] / hour
        p.mkdir(parents=True, exist_ok=True)
        with open(p / "events.jsonl", "a") as f:
            f.write(json.dumps(event, default=str) + "\n")


def write_to_kafka(events: list[dict], topic: str, bootstrap_servers: str = "localhost:9092"):
    """Publish events to Kafka topic. No-op if kafka-python is unavailable."""
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        )
        for event in events:
            producer.send(topic, value=event)
        producer.flush()
        print(f"Published {len(events)} events to Kafka topic '{topic}'")
    except ImportError:
        print("kafka-python not installed — skipping Kafka publish, JSONL file-sink only")
    except Exception as e:
        print(f"Kafka publish failed: {e} — events written to JSONL file-sink only")


def write_to_postgres(df: pd.DataFrame, table_name: str, dsn: Optional[str] = None):
    """Write reference tables to vendor_db PostgreSQL. No-op if psycopg2 unavailable or no DSN."""
    if dsn is None:
        dsn = os.getenv("POSTGRES_DSN", "postgresql://vendor:vendor@localhost:5432/vendor_db")
    try:
        import psycopg2
        from sqlalchemy import create_engine
        engine = create_engine(dsn.replace("postgresql://", "postgresql+psycopg2://"))
        df.to_sql(table_name, engine, if_exists="append", index=False)
        print(f"Wrote {len(df)} rows to vendor_db.{table_name}")
    except ImportError:
        print("psycopg2/sqlalchemy not installed — skipping PostgreSQL write")
    except Exception as e:
        print(f"PostgreSQL write failed: {e}")


def generate_quality_report(
    ohlcv: pd.DataFrame,
    events: list[dict],
    cfg: dict,
    schema_change_date,
    tickers_df=None,
    output_path: str = "data/quality_report.md",
):
    """Auto-generate quality report with skew, cardinality, schema evolution, dedup rates."""
    # VN30 volume share from ticker data (or fallback to top 30 by volume)
    if tickers_df is not None and "vn30" in tickers_df.columns:
        vn30_ids = set(tickers_df[tickers_df["vn30"]]["ticker_id"])
        vn30_vol = ohlcv[ohlcv["ticker_id"].isin(vn30_ids)]["volume"].sum()
    else:
        top30 = ohlcv.groupby("ticker_id")["volume"].sum().nlargest(30).index
        vn30_vol = ohlcv[ohlcv["ticker_id"].isin(top30)]["volume"].sum()
    total_vol = ohlcv["volume"].sum()

    n_events = len(events)
    late_events = sum(1 for e in events if e.get("event_timestamp", "") != e.get("created_ts", ""))
    dup_stream = n_events - len(set(e.get("event_id") for e in events)) if n_events > 0 else 0

    lines = [
        "# Data Quality Report",
        "",
        "## Skew Distribution",
        f"- VN30 volume share: {vn30_vol / max(total_vol, 1) * 100:.1f}%",
        f"- Total volume: {total_vol:,}",
        "",
        "## Cardinality",
        f"- Unique ticker_id: {ohlcv['ticker_id'].nunique()}",
        f"- Unique (ticker_id, trade_date): {ohlcv.groupby(['ticker_id', 'trade_date']).ngroups}",
        "",
        "## Schema Evolution",
        f"- Schema change date: {schema_change_date}",
        f"- Rows with _schema_version=1: {(ohlcv.get('_schema_version', pd.Series([1]*len(ohlcv))) == 1).sum()}",
        f"- Rows with _schema_version=2: {(ohlcv.get('_schema_version', pd.Series([1]*len(ohlcv))) == 2).sum()}",
        f"- foreign_room null (v1 partitions): {ohlcv['foreign_room'].isna().sum()}",
        "",
        "## Stream Quality",
        f"- Total events: {n_events}",
        f"- Late arrivals: {late_events} ({late_events / max(n_events, 1) * 100:.1f}%)",
        f"- Duplicate events: {dup_stream} ({dup_stream / max(n_events, 1) * 100:.1f}%)",
        "",
        f"*Generated with seed={cfg['random_seed']}*",
    ]
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        f.write("\n".join(lines))
    print(f"Quality report → {output_path}")
