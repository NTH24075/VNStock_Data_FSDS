"""Gold modeling — re-exports from jobs.gold.* submodules."""

import argparse

from jobs.gold.dimensions import (
    build_dim_date,
    build_dim_exchange,
    build_dim_industry,
    build_dim_session,
    build_dim_ticker,
    get_spark,
    read_silver_daily,
)
from jobs.gold.facts import build_fact_daily_price, build_fact_intraday_trade
from jobs.gold.features import build_feat_ticker_daily, build_feat_ticker_unified
from jobs.gold.labels import build_ml_ticker_label
from jobs.gold.metadata import write_pipeline_run_metadata
from jobs.gold.obt import build_obt


def main():
    parser = argparse.ArgumentParser(description="Gold modeling")
    parser.add_argument("--silver-dir", default="data/silver")
    parser.add_argument("--gold-dir", default="data/gold")
    args = parser.parse_args()

    spark = get_spark()

    print("\n=== Gold: Reading Silver ===")
    df = read_silver_daily(spark, args.silver_dir)
    print(f"  {df.count()} rows from stg_daily_price")

    print("\n=== Gold: Dimensions ===")
    build_dim_date(spark, df, args.gold_dir)
    build_dim_ticker(spark, df, args.gold_dir)

    print("\n=== Gold: Facts ===")
    build_fact_daily_price(spark, df, args.gold_dir)

    print("\n=== Gold: OBT ===")
    build_obt(spark, args.gold_dir)

    print("\n=== Gold: Features ===")
    build_feat_ticker_daily(spark, args.gold_dir)

    print("\n=== Gold: Labels ===")
    build_ml_ticker_label(spark, args.gold_dir)

    spark.stop()
    print("\nGold modeling complete.")


if __name__ == "__main__":
    main()
