"""Gold modeling — Silver Delta → Gold Delta (PySpark).

SCD2 dims, facts with adj_close, OBT, feature tables (feat_ticker_daily),
label table (ml_ticker_label).

Every feature table row carries event_timestamp + created_ts.

Usage:
    python jobs/gold_model.py --silver-dir data/silver --gold-dir data/gold
"""

import argparse
import os

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def get_spark(app_name: str = "gold_model") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def read_silver_daily(spark: SparkSession, silver_dir: str) -> DataFrame:
    path = os.path.join(silver_dir, "stg_daily_price")
    if not os.path.exists(path):
        raise FileNotFoundError(f"No Silver data at {path}")
    return spark.read.format("delta").load(path)


# =========================================================================
# Dimensions
# =========================================================================


def build_dim_date(spark: SparkSession, df: DataFrame, gold_dir: str):
    dates = df.select("trade_date").distinct().orderBy("trade_date")
    dim = dates.select(
        F.xxhash64(F.col("trade_date").cast("string")).alias("date_key"),
        F.col("trade_date").cast("string").alias("date_key_str"),
        F.col("trade_date").alias("calendar_date"),
        (~F.dayofweek(F.col("trade_date")).isin([1, 7])).alias("is_trading_day"),
        F.when(F.dayofweek(F.col("trade_date")).isin([1, 7]), F.lit("weekend")).otherwise(F.lit(None)).cast("string").alias("holiday_name"),
    )
    out = os.path.join(gold_dir, "dim_date")
    dim.write.format("delta").mode("overwrite").save(out)
    trading = dim.filter(F.col("is_trading_day")).count()
    nontrading = dim.filter(~F.col("is_trading_day")).count()
    print(f"  dim_date: {dim.count()} rows ({trading} trading, {nontrading} non-trading) → {out}")


def build_dim_ticker(spark: SparkSession, df: DataFrame, gold_dir: str) -> DataFrame:
    # Read ticker seed for real company_name, exchange, ICB classification
    seed_path = os.getenv(
        "SEED_PATH",
        os.path.join(os.path.dirname(__file__), "..", "generator", "seed", "tickers_reference.json"),
    )
    tickers = df.select("ticker_id").distinct().orderBy("ticker_id")

    if os.path.exists(seed_path):
        seed_df = spark.read.option("multiline", "true").json(seed_path)
        tickers = tickers.join(seed_df, on="ticker_id", how="left")
    else:
        tickers = (
            tickers
            .withColumn("ticker", F.col("ticker_id"))
            .withColumn("company_name", F.lit("Unknown"))
            .withColumn("exchange", F.lit("HOSE"))
            .withColumn("icb_l1", F.lit(None).cast("string"))
            .withColumn("icb_l2", F.lit(None).cast("string"))
        )

    dim = tickers.select(
        F.xxhash64(F.col("ticker_id")).alias("ticker_key"),
        F.col("ticker_id"),
        F.coalesce(F.col("ticker"), F.col("ticker_id")).alias("ticker"),
        F.coalesce(F.col("company_name"), F.lit("Unknown")).alias("company_name"),
        F.coalesce(F.col("exchange"), F.lit("HOSE")).alias("exchange"),
        F.lit("active").alias("listing_status"),
        F.col("icb_l1"),
        F.col("icb_l2"),
        F.current_timestamp().alias("valid_from_ts"),
        F.lit(None).cast("timestamp").alias("valid_to_ts"),
        F.lit(True).alias("is_current"),
    )
    out = os.path.join(gold_dir, "dim_ticker")
    dim.write.format("delta").mode("overwrite").save(out)
    print(f"  dim_ticker (SCD2): {dim.count()} rows → {out}")
    return dim


def build_dim_industry(spark: SparkSession, gold_dir: str):
    """Seed static ICB industry dimension."""
    rows = [(1, "BANK", "L1", "Ngân hàng"),
            (2, "REAL", "L1", "Bất động sản"),
            (3, "SEC", "L1", "Chứng khoán"),
            (4, "FOOD", "L1", "Thực phẩm & đồ uống"),
            (5, "OIL", "L1", "Dầu khí"),
            (6, "STEEL", "L1", "Thép"),
            (7, "TECH", "L1", "Công nghệ"),
            (8, "RETAIL", "L1", "Bán lẻ"),
            (9, "UTIL", "L1", "Tiện ích"),
            (10, "OTHER", "L1", "Khác")]
    dim = spark.createDataFrame(rows, ["industry_key", "icb_code", "icb_level", "icb_name"])
    out = os.path.join(gold_dir, "dim_industry")
    dim.write.format("delta").mode("overwrite").save(out)
    print(f"  dim_industry: {dim.count()} rows → {out}")


def build_dim_exchange(spark: SparkSession, gold_dir: str):
    """Seed static exchange dimension."""
    rows = [(1, "HOSE", 0.07),
            (2, "HNX", 0.10),
            (3, "UPCOM", 0.15)]
    dim = spark.createDataFrame(rows, ["exchange_key", "exchange_code", "price_limit_pct"])
    out = os.path.join(gold_dir, "dim_exchange")
    dim.write.format("delta").mode("overwrite").save(out)
    print(f"  dim_exchange: {dim.count()} rows → {out}")


def build_dim_session(spark: SparkSession, gold_dir: str):
    """Seed static session type dimension."""
    rows = [(1, "ATO"),
            (2, "CONTINUOUS"),
            (3, "ATC"),
            (4, "PUT_THROUGH")]
    dim = spark.createDataFrame(rows, ["session_key", "session_type"])
    out = os.path.join(gold_dir, "dim_session")
    dim.write.format("delta").mode("overwrite").save(out)
    print(f"  dim_session: {dim.count()} rows → {out}")


# =========================================================================
# Facts
# =========================================================================


def build_fact_daily_price(spark: SparkSession, df: DataFrame, gold_dir: str):
    # Join with dim_ticker and dim_date for surrogate keys per star-schema design
    dim_ticker_path = os.path.join(gold_dir, "dim_ticker")
    dim_date_path = os.path.join(gold_dir, "dim_date")

    if os.path.exists(dim_ticker_path):
        dim_ticker = spark.read.format("delta").load(dim_ticker_path).select("ticker_key", "ticker_id")
        df = df.join(dim_ticker, on="ticker_id", how="left")

    if os.path.exists(dim_date_path):
        dim_date = spark.read.format("delta").load(dim_date_path).select("date_key", "calendar_date")
        df = df.join(dim_date, df.trade_date == dim_date.calendar_date, how="left").drop("calendar_date")

    cols = ["ticker_id", "trade_date", "open", "high", "low", "close",
            "volume", "value", "foreign_room", "_schema_version"]
    if "ticker_key" in df.columns:
        cols.insert(0, "ticker_key")
    if "date_key" in df.columns:
        cols.insert(1 if "ticker_key" in df.columns else 0, "date_key")

    fact = df.select(*cols).withColumn("adj_close", F.col("close"))
    fact = fact.withColumn("_event_timestamp", F.current_timestamp())

    out = os.path.join(gold_dir, "fact_daily_price")
    fact.write.format("delta").mode("overwrite").save(out)
    print(f"  fact_daily_price: {fact.count()} rows → {out}")


# =========================================================================
# OBT
# =========================================================================


def build_obt(spark: SparkSession, gold_dir: str):
    fact_path = os.path.join(gold_dir, "fact_daily_price")
    dim_ticker_path = os.path.join(gold_dir, "dim_ticker")
    fact = spark.read.format("delta").load(fact_path)

    # Read dim_ticker for company_name, exchange, icb
    ticker_cols = ["ticker", "company_name", "exchange", "icb_l1", "icb_l2"]
    if os.path.exists(dim_ticker_path):
        dim_t = spark.read.format("delta").load(dim_ticker_path).select(["ticker_id"] + ticker_cols)
        fact = fact.join(dim_t, on="ticker_id", how="left")

    window = Window.partitionBy("ticker_id").orderBy("trade_date")

    # Volume MA20
    ma20_vol = F.avg("volume").over(window.rowsBetween(-19, 0))

    obt = (
        fact
        .withColumn("prev_close", F.lag("close").over(window))
        .withColumn("close_5d_ago", F.lag("close", 5).over(window))
        .withColumn(
            "pct_change_1d",
            F.when(
                F.col("prev_close").isNotNull() & (F.col("prev_close") > 0),
                (F.col("close") - F.col("prev_close")) / F.col("prev_close") * 100,
            ),
        )
        .withColumn(
            "pct_change_5d",
            F.when(
                F.col("close_5d_ago").isNotNull() & (F.col("close_5d_ago") > 0),
                (F.col("close") - F.col("close_5d_ago")) / F.col("close_5d_ago") * 100,
            ),
        )
        .withColumn("volume_vs_ma20_ratio", F.col("volume") / ma20_vol)
        .withColumn("price_limit_hit_flag", F.abs(F.col("pct_change_1d")) >= 7)
        .withColumn("is_vn30", F.lit(False))
    )

    # Add company_name, exchange, icb from dim if available
    select_cols = [
        "ticker_id", "trade_date",
        F.col("close"), F.col("adj_close"),
        F.col("pct_change_1d"), F.col("pct_change_5d"),
        F.col("volume"), F.col("volume_vs_ma20_ratio"),
        F.col("value"), F.col("foreign_room"),
        F.col("price_limit_hit_flag"), F.col("is_vn30"),
    ]
    for c in ticker_cols:
        if c in obt.columns:
            select_cols.append(F.col(c))

    result = obt.select(*select_cols)

    out = os.path.join(gold_dir, "obt_ticker_daily_performance")
    result.write.format("delta").mode("overwrite").save(out)
    print(f"  obt_ticker_daily_performance: {result.count()} rows → {out}")


def build_fact_intraday_trade(spark: SparkSession, gold_dir: str):
    """Stub for intraday trade facts — populated by Flink micro-batch jobs."""
    schema = "trade_id STRING, ticker_id STRING, trade_date DATE, price DOUBLE, quantity BIGINT, trade_value DOUBLE, ticker_key BIGINT, date_key BIGINT, session_key INT, _event_timestamp TIMESTAMP"
    empty = spark.createDataFrame([], schema=schema)
    out = os.path.join(gold_dir, "fact_intraday_trade")
    empty.write.format("delta").mode("overwrite").save(out)
    print(f"  fact_intraday_trade: stub created → {out}")


# =========================================================================
# Feature Tables
# =========================================================================


def build_feat_ticker_daily(spark: SparkSession, gold_dir: str):
    fact_path = os.path.join(gold_dir, "fact_daily_price")
    ff_path = os.path.join(gold_dir, "fact_foreign_flow")
    fact = spark.read.format("delta").load(fact_path)

    window = Window.partitionBy("ticker_id").orderBy("trade_date")
    return_1d = (
        (F.col("close") - F.lag("close").over(window))
        / F.lag("close").over(window)
    )

    df = fact.withColumn("return_1d", return_1d)

    # f_ticker_return_5d
    df = df.withColumn(
        "f_ticker_return_5d",
        (F.col("close") - F.lag("close", 5).over(window))
        / F.lag("close", 5).over(window),
    )

    # f_ticker_volatility_20d
    df = df.withColumn(
        "f_ticker_volatility_20d",
        F.when(
            F.row_number().over(window) >= 5,
            F.stddev("return_1d").over(window.rowsBetween(-19, 0)),
        ),
    )

    # f_ticker_ma20_gap
    ma20 = F.avg("close").over(window.rowsBetween(-19, 0))
    df = df.withColumn("f_ticker_ma20_gap", (F.col("close") - ma20) / ma20)

    # f_ticker_foreign_net_ratio_10d — net foreign buy / total traded value, 10-day window
    if os.path.exists(ff_path):
        ff = spark.read.format("delta").load(ff_path)
        has_ff_cols = all(c in ff.columns for c in ["foreign_buy_value", "foreign_sell_value"])
        if has_ff_cols:
            ff = ff.withColumn("net_foreign", F.col("foreign_buy_value") - F.col("foreign_sell_value"))
            df = df.join(
                ff.select("ticker_id", "trade_date", "net_foreign"),
                on=["ticker_id", "trade_date"], how="left",
            )
            net_f_win = Window.partitionBy("ticker_id").orderBy("trade_date").rowsBetween(-9, 0)
            value_win = Window.partitionBy("ticker_id").orderBy("trade_date").rowsBetween(-9, 0)
            net_f_sum = F.sum(F.coalesce(F.col("net_foreign"), F.lit(0))).over(net_f_win)
            total_v = F.sum(F.coalesce(F.col("value"), F.lit(0))).over(value_win)
            df = df.withColumn(
                "f_ticker_foreign_net_ratio_10d",
                F.when(total_v > 0, net_f_sum / total_v),
            )
        else:
            df = df.withColumn("f_ticker_foreign_net_ratio_10d", F.lit(None))
    else:
        df = df.withColumn("f_ticker_foreign_net_ratio_10d", F.lit(None))

    now = F.current_timestamp()
    feat = df.select(
        F.col("ticker_id"),
        F.col("trade_date"),
        F.col("trade_date").cast("timestamp").alias("event_timestamp"),
        now.alias("created_ts"),
        F.col("f_ticker_return_5d"),
        F.col("f_ticker_volatility_20d"),
        F.col("f_ticker_ma20_gap"),
        F.col("f_ticker_foreign_net_ratio_10d"),
    )

    out = os.path.join(gold_dir, "feat_ticker_daily")
    feat.write.format("delta").mode("overwrite").save(out)
    print(f"  feat_ticker_daily: {feat.count()} rows, columns={feat.columns} → {out}")


def build_feat_ticker_unified(spark: SparkSession, gold_dir: str):
    """Stub for unified feature table — point-in-time join of daily + intraday features.

    Populated when feat_stream_intraday is available (Flink streaming).
    Currently mirrors feat_ticker_daily as a placeholder.
    """
    feat_daily_path = os.path.join(gold_dir, "feat_ticker_daily")
    if not os.path.exists(feat_daily_path):
        print("  No feat_ticker_daily — skipping feat_ticker_unified")
        return
    feat = spark.read.format("delta").load(feat_daily_path)
    unified = feat.select(
        "ticker_id", "trade_date", "event_timestamp", "created_ts",
        F.col("f_ticker_return_5d"),
        F.col("f_ticker_volatility_20d"),
        F.col("f_ticker_ma20_gap"),
    )
    out = os.path.join(gold_dir, "feat_ticker_unified")
    unified.write.format("delta").mode("overwrite").save(out)
    print(f"  feat_ticker_unified: {unified.count()} rows → {out}")


# =========================================================================
# Data Governance — Pipeline Run Metadata
# =========================================================================


def write_pipeline_run_metadata(spark: SparkSession, gold_dir: str, pipeline: str, status: str,
                                input_rows: int = None, output_rows: int = None,
                                error_summary: str = None):
    """Persist pipeline run metadata to ops_pipeline_run Gold table (02 §7.3)."""
    from datetime import datetime as dt

    run_id = f"{pipeline}_{dt.now().strftime('%Y%m%d_%H%M%S')}"
    row = [(run_id, pipeline, dt.now().isoformat(), dt.now().isoformat(),
            status, input_rows, output_rows, error_summary)]
    schema = "run_id STRING, pipeline STRING, start_time STRING, end_time STRING, status STRING, input_rows BIGINT, output_rows BIGINT, error_summary STRING"
    meta = spark.createDataFrame(row, schema=schema)
    out = os.path.join(gold_dir, "ops_pipeline_run")
    meta.write.format("delta").mode("append").save(out)
    print(f"  ops_pipeline_run: {pipeline} → {status} (run_id={run_id})")


# =========================================================================
# Labels
# =========================================================================


def build_ml_ticker_label(spark: SparkSession, gold_dir: str):
    fact_path = os.path.join(gold_dir, "fact_daily_price")
    fact = spark.read.format("delta").load(fact_path)

    window = Window.partitionBy("ticker_id").orderBy("trade_date")
    df = fact.withColumn("close_t3", F.lead("adj_close", 3).over(window))
    labels = df.filter(F.col("close_t3").isNotNull())

    labels = labels.withColumn(
        "label",
        (F.col("close_t3") > F.col("adj_close") * 1.01).cast("int"),
    )

    now = F.current_timestamp()
    result = labels.select(
        F.col("ticker_id"),
        F.col("trade_date"),
        F.col("trade_date").cast("timestamp").alias("event_timestamp"),
        now.alias("created_ts"),
        F.col("label"),
    )

    out = os.path.join(gold_dir, "ml_ticker_label")
    result.write.format("delta").mode("overwrite").save(out)

    pos_rate = result.filter(F.col("label") == 1).count() / max(result.count(), 1) * 100
    print(f"  ml_ticker_label: {result.count()} rows, positive rate={pos_rate:.1f}% → {out}")


# =========================================================================
# Main
# =========================================================================


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
