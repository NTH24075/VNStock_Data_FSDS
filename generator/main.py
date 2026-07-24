"""Generator CLI — produces offline Parquet + streaming JSON events + reference tables.

Usage:
    python -m generator.main --mode offline  --config config/generator.yaml
    python -m generator.main --mode stream   --config config/generator.yaml
    python -m generator.main --mode all      --config config/generator.yaml
"""

import argparse
import json
import logging
import os
from datetime import datetime
from pathlib import Path

import yaml

from generator.generators import Generator
from generator.problems import (
    apply_volume_skew,
    inject_duplicates,
    inject_stream_duplicates,
    tag_schema_version,
)
from generator.writers import (
    generate_quality_report,
    write_jsonl,
    write_ohlcv_landing,
    write_parquet_landing,
    write_to_kafka,
    write_to_postgres,
)

logger = logging.getLogger(__name__)


def load_config(config_path: str) -> dict:
    """Load generator YAML and normalize the coursework drift key names."""
    with open(config_path, encoding="utf-8") as file_handle:
        cfg = yaml.safe_load(file_handle)
    drift_path = Path(config_path).parent / "drift.yaml"
    if drift_path.exists():
        with drift_path.open(encoding="utf-8") as file_handle:
            raw_drift = yaml.safe_load(file_handle)
        cfg["drift"] = {
            "enabled": raw_drift.get("drift_enabled", False)
            and raw_drift.get("scenario_A_volatility_regime", False),
            "start_date": raw_drift.get("drift_start_date"),
            "mode": raw_drift.get("drift_mode", "gradual"),
            "ramp_trading_days": raw_drift.get("drift_ramp_trading_days", 10),
            "volatility_multiplier": raw_drift.get("volatility_multiplier", 1.0),
            "smallcap_amplifier": raw_drift.get("smallcap_amplifier", 1.0),
        }
    return cfg


def load_seed(seed_path: str = None) -> list[dict]:
    """Load ticker reference seed. Falls back to built-in minimal seed."""
    if seed_path is None:
        seed_path = Path(__file__).parent / "seed" / "tickers_reference.json"

    if os.path.exists(seed_path):
        with open(seed_path, encoding="utf-8") as file_handle:
            data = json.load(file_handle)
        if data:
            return data

    logger.warning("No seed file found. Generating minimal mock seed (20 tickers) for testing.")
    return _mock_seed()


def _mock_seed() -> list[dict]:
    """Minimal 20-ticker seed for testing without vnstock."""
    tickers = []
    exchanges = ["HOSE"] * 12 + ["HNX"] * 5 + ["UPCOM"] * 3
    industries = [
        ("Financials", "Banking"),
        ("Financials", "Securities"),
        ("Real Estate", "Residential"),
        ("Real Estate", "Industrial"),
        ("Consumer Staples", "Food & Beverage"),
        ("Materials", "Steel"),
        ("Energy", "Oil & Gas"),
        ("Information Technology", "Software"),
        ("Health Care", "Pharmaceuticals"),
        ("Industrials", "Construction"),
    ]
    for i in range(20):
        ticker = f"MOCK{i:02d}"
        tickers.append(
            {
                "ticker_id": ticker,
                "ticker": ticker,
                "company_name": f"Mock Company {i:02d}",
                "exchange": exchanges[i % len(exchanges)],
                "icb_l1": industries[i % len(industries)][0],
                "icb_l2": industries[i % len(industries)][1],
                "listing_date": "2024-01-01",
                "is_active": True,
            }
        )
    return tickers


def run_offline(cfg: dict, gen: Generator):
    """Generate offline historical data → Parquet landing + PostgreSQL."""
    logger.info("Generating OHLCV...")
    ohlcv = gen.generate_ohlcv()
    ohlcv = tag_schema_version(ohlcv, gen.schema_change_date)
    ohlcv = apply_volume_skew(ohlcv, gen.tickers, cfg)
    dup_rate = cfg.get("duplicate_rate_offline", 0.02)
    ohlcv = inject_duplicates(ohlcv, dup_rate, ["ticker_id", "trade_date"], gen.rng)

    logger.info("Generating foreign flow...")
    foreign_flow = gen.generate_foreign_flow(ohlcv)
    foreign_flow = inject_duplicates(foreign_flow, dup_rate, ["ticker_id", "trade_date"], gen.rng)

    logger.info("Generating corporate actions...")
    corp_actions = gen.generate_corporate_actions()

    logger.info("Generating financial ratios...")
    fin_ratios = gen.generate_financial_ratios(ohlcv)

    run_date = datetime.now().strftime("%Y-%m-%d")

    logger.info("Writing Parquet to landing...")
    write_ohlcv_landing(ohlcv, run_date)
    write_parquet_landing(foreign_flow, "foreign_flow_daily", run_date)
    write_parquet_landing(corp_actions, "corporate_actions", run_date)
    write_parquet_landing(fin_ratios, "financial_ratios", run_date)

    logger.info("Writing reference tables to vendor_db...")
    write_to_postgres(
        gen.tickers.rename(columns={"icb_l1": "icb_industry_l1", "icb_l2": "icb_industry_l2"}),
        "tickers",
    )
    write_to_postgres(corp_actions, "corporate_actions")

    logger.info("Generating quality report...")
    generate_quality_report(
        ohlcv,
        [],
        cfg,
        gen.schema_change_date,
        tickers_df=gen.tickers,
        output_path=cfg.get("quality_report_path", "data/quality_report.md"),
    )

    logger.info("Offline generation complete.")
    return ohlcv, foreign_flow, corp_actions, fin_ratios


def run_stream(cfg: dict, gen: Generator, ohlcv=None):
    """Generate streaming events → Kafka + JSONL file-sink."""
    if ohlcv is None:
        logger.info("Regenerating OHLCV for stream consistency...")
        ohlcv = gen.generate_ohlcv()
        ohlcv = tag_schema_version(ohlcv, gen.schema_change_date)
        ohlcv = apply_volume_skew(ohlcv, gen.tickers, cfg)
        ohlcv = inject_duplicates(
            ohlcv,
            cfg.get("duplicate_rate_offline", 0.02),
            ["ticker_id", "trade_date"],
            gen.rng,
        )

    logger.info("Generating streaming events for %d trading days...", len(gen.trading_days))
    events = gen.generate_streaming_events(ohlcv)

    dup_rate = cfg.get("duplicate_rate_stream", 0.015)
    events = inject_stream_duplicates(events, dup_rate, gen.rng)
    events.sort(key=lambda event: event["created_ts"])

    kafka_topic = cfg.get("kafka_topic", "stock_market_events_v3")
    logger.info("Publishing %d events...", len(events))

    write_to_kafka(
        events,
        kafka_topic,
        partitions=int(cfg.get("kafka_partitions", 4)),
        retention_ms=int(cfg.get("kafka_retention_ms", -1)),
    )

    logger.info("Writing JSONL file-sink...")
    for td in gen.trading_days:
        td_str = (
            td if isinstance(td, str) else td.isoformat() if hasattr(td, "isoformat") else str(td)
        )
        day_events = [e for e in events if e["event_timestamp"][:10] == td_str[:10]]
        if day_events:
            write_jsonl(day_events, td_str)

    generate_quality_report(
        ohlcv,
        events,
        cfg,
        gen.schema_change_date,
        tickers_df=gen.tickers,
        output_path=cfg.get("quality_report_path", "data/quality_report.md"),
    )
    logger.info("Streaming generation complete. %d events produced.", len(events))


def main():
    """Parse CLI arguments and execute the selected generation mode."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="VN Stock Market Data Generator")
    parser.add_argument("--mode", choices=["offline", "stream", "all"], required=True)
    parser.add_argument("--config", default="config/generator.yaml")
    args = parser.parse_args()

    cfg = load_config(args.config)
    seed = load_seed()
    gen = Generator(cfg, seed)

    if args.mode in ("offline", "all"):
        ohlcv, _, _, _ = run_offline(cfg, gen)

    if args.mode in ("stream", "all"):
        run_stream(cfg, gen, ohlcv if args.mode == "all" else None)


if __name__ == "__main__":
    main()
