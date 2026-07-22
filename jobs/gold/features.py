"""Gold feature tables — feat_ticker_daily, feat_ticker_unified."""

import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def get_spark(app_name: str = "gold_features") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


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

    df = df.withColumn(
        "f_ticker_return_5d",
        (F.col("close") - F.lag("close", 5).over(window))
        / F.lag("close", 5).over(window),
    )

    df = df.withColumn(
        "f_ticker_volatility_20d",
        F.when(
            F.row_number().over(window) >= 5,
            F.stddev("return_1d").over(window.rowsBetween(-19, 0)),
        ),
    )

    ma20 = F.avg("close").over(window.rowsBetween(-19, 0))
    df = df.withColumn("f_ticker_ma20_gap", (F.col("close") - ma20) / ma20)

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
    print(f"  feat_ticker_daily: {feat.count()} rows, columns={feat.columns} -> {out}")


def build_feat_ticker_unified(spark: SparkSession, gold_dir: str):
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
    print(f"  feat_ticker_unified: {unified.count()} rows -> {out}")
