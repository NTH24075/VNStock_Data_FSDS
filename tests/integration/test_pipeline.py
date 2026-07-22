"""Integration test: full pipeline Generator → Bronze → Silver → Gold → Drift (PySpark)."""

import os
import sys
import tempfile
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture(scope="module")
def spark():
    try:
        from pyspark.sql import SparkSession
        session = (
            SparkSession.builder.appName("test_pipeline")
            .master("local[1]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
            .getOrCreate()
        )
        yield session
        session.stop()
    except Exception:
        pytest.skip("Java/Spark unavailable")


def _run_generator(spark, data_dir, n_tickers=10, days_history=60):
    """Run generator in offline mode with a small config."""
    from datetime import date, timedelta

    from generator.calendar import generate_trading_calendar
    from generator.generators import Generator
    from generator.problems import apply_volume_skew, inject_duplicates, tag_schema_version
    from generator.writers import write_parquet_landing

    cfg = {
        "n_tickers": n_tickers,
        "vn30_count": 3,
        "vn30_volume_share": 0.80,
        "days_history": days_history,
        "random_seed": 42,
        "schema_change_date": "2025-07-01",
        "duplicate_rate_offline": 0.02,
    }

    schema_change_date = date.fromisoformat(cfg["schema_change_date"])
    half_days = cfg["days_history"] // 2
    start_date = schema_change_date - timedelta(days=half_days * 2)

    seed = []
    for i in range(cfg["n_tickers"]):
        seed.append({
            "ticker_id": f"T{i:03d}",
            "ticker": f"T{i:03d}",
            "company_name": f"Test Company {i}",
            "exchange": "HOSE",
            "icb_l1": "Financials",
            "icb_l2": "Banking",
            "listing_date": "2024-01-01",
            "is_active": True,
        })

    gen = Generator(cfg, seed)
    gen.trading_days = generate_trading_calendar(start_date, cfg["days_history"])

    ohlcv = gen.generate_ohlcv()
    ohlcv = tag_schema_version(ohlcv, schema_change_date)
    ohlcv = apply_volume_skew(ohlcv, gen.tickers, cfg)
    ohlcv = inject_duplicates(ohlcv, 0.02, ["ticker_id", "trade_date"], gen.rng)

    foreign_flow = gen.generate_foreign_flow(ohlcv)
    foreign_flow = inject_duplicates(foreign_flow, 0.02, ["ticker_id", "trade_date"], gen.rng)

    corp_actions = gen.generate_corporate_actions()
    fin_ratios = gen.generate_financial_ratios(ohlcv)

    from datetime import datetime
    run_date = datetime.now().strftime("%Y-%m-%d")
    os.makedirs(data_dir, exist_ok=True)

    write_parquet_landing(ohlcv, "ohlcv_daily", run_date, base_dir=data_dir)
    write_parquet_landing(foreign_flow, "foreign_flow_daily", run_date, base_dir=data_dir)
    write_parquet_landing(corp_actions, "corporate_actions", run_date, base_dir=data_dir)
    write_parquet_landing(fin_ratios, "financial_ratios", run_date, base_dir=data_dir)

    return {"ohlcv": len(ohlcv), "foreign_flow": len(foreign_flow),
            "corp_actions": len(corp_actions), "fin_ratios": len(fin_ratios)}


class TestPipelineEndToEnd:
    def test_full_pipeline(self, spark, tmp_path):
        tmpdir = str(tmp_path)
        landing_dir = os.path.join(tmpdir, "landing")
        bronze_dir = os.path.join(tmpdir, "bronze")
        silver_dir = os.path.join(tmpdir, "silver")
        gold_dir = os.path.join(tmpdir, "gold")

        # 1. Generator
        gen_result = _run_generator(spark, landing_dir, n_tickers=10, days_history=60)
        assert gen_result["ohlcv"] > 0

        # 2. Bronze — read landing parquet + write delta
        from jobs.bronze.offline import add_ingest_metadata, read_landing_parquet, validate_contract
        from datetime import datetime
        import json
        batch_id_str = datetime.now().strftime("%Y%m%d_%H%M%S")

        df_ohlcv = read_landing_parquet(spark, landing_dir, "ohlcv_daily")
        assert df_ohlcv.count() > 0

        for v in [1, 2]:
            contract_path = PROJECT_ROOT / "contracts" / f"raw_ohlcv_daily.v{v}.json"
            with open(contract_path) as f:
                contract = json.load(f)
            df_v = df_ohlcv.filter(f"_schema_version = {v}")
            cnt = df_v.count()
            if cnt == 0:
                continue
            validate_contract(df_v, contract, v)
            df_v = add_ingest_metadata(df_v, batch_id_str, v)
            df_v.write.format("delta").mode("overwrite").save(
                os.path.join(bronze_dir, f"raw_ohlcv_daily_v{v}")
            )

        assert os.path.exists(os.path.join(bronze_dir, "raw_ohlcv_daily_v1"))

        # 3. Silver
        from jobs.silver.daily import dedup, read_bronze_ohlcv, validate_domain

        df = read_bronze_ohlcv(spark, bronze_dir)
        assert df.count() > 0
        df = dedup(df)
        df = validate_domain(df)
        df.write.format("delta").mode("overwrite").save(
            os.path.join(silver_dir, "stg_daily_price")
        )
        assert os.path.exists(os.path.join(silver_dir, "stg_daily_price"))

        # 4. Gold
        from jobs.gold.dimensions import (
            build_dim_date, build_dim_ticker, read_silver_daily,
        )
        from jobs.gold.facts import build_fact_daily_price
        from jobs.gold.features import build_feat_ticker_daily
        from jobs.gold.labels import build_ml_ticker_label
        from jobs.gold.obt import build_obt

        df_silver = read_silver_daily(spark, silver_dir)

        build_dim_date(spark, df_silver, gold_dir)
        build_dim_ticker(spark, df_silver, gold_dir)
        build_fact_daily_price(df_silver, gold_dir)
        assert os.path.exists(os.path.join(gold_dir, "fact_daily_price"))

        build_obt(spark, gold_dir)
        build_feat_ticker_daily(spark, gold_dir)
        build_ml_ticker_label(spark, gold_dir)

        assert os.path.exists(os.path.join(gold_dir, "obt_ticker_daily_performance"))
        assert os.path.exists(os.path.join(gold_dir, "feat_ticker_daily"))
        assert os.path.exists(os.path.join(gold_dir, "ml_ticker_label"))

        # 5. Drift
        from jobs.gold.drift import build_agg_feature_health, build_ml_ticker_training

        build_agg_feature_health(spark, gold_dir)
        assert os.path.exists(os.path.join(gold_dir, "agg_feature_health_daily"))

        build_ml_ticker_training(spark, gold_dir)
        assert os.path.exists(os.path.join(gold_dir, "ml_ticker_training"))

        # 6. Verify
        training = spark.read.format("delta").load(os.path.join(gold_dir, "ml_ticker_training"))
        assert training.count() > 0
        assert "label" in training.columns
        assert any(c.startswith("f_") for c in training.columns)
