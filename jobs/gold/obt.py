"""Gold OBT — obt_ticker_daily_performance."""

import logging
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def build_obt(spark: SparkSession, gold_dir: str):
    """Build a denormalized daily performance table for BI queries."""
    fact_path = os.path.join(gold_dir, "fact_daily_price")
    dim_ticker_path = os.path.join(gold_dir, "dim_ticker")
    fact = spark.read.format("delta").load(fact_path)

    ticker_cols = [
        "ticker",
        "company_name",
        "exchange",
        "icb_l1",
        "icb_l2",
        "is_vn30",
    ]
    if os.path.exists(dim_ticker_path):
        dim_ticker = (
            spark.read.format("delta")
            .load(dim_ticker_path)
            .filter(F.col("is_current"))
            .select(["ticker_id", *ticker_cols])
        )
        fact = fact.join(F.broadcast(dim_ticker), on="ticker_id", how="left")

    foreign_flow_path = os.path.join(gold_dir, "fact_foreign_flow")
    if os.path.exists(foreign_flow_path):
        foreign_flow = (
            spark.read.format("delta")
            .load(foreign_flow_path)
            .select("ticker_id", "trade_date", "foreign_net_value")
        )
        fact = fact.join(
            foreign_flow,
            on=["ticker_id", "trade_date"],
            how="left",
        )
    else:
        fact = fact.withColumn(
            "foreign_net_value",
            F.lit(None).cast("double"),
        )

    window = Window.partitionBy("ticker_id").orderBy("trade_date")
    ma20_vol = F.avg("volume").over(window.rowsBetween(-19, 0))

    obt = (
        fact.withColumn("prev_close", F.lag("close").over(window))
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
        .withColumn(
            "_price_limit_pct",
            F.when(F.col("exchange") == "HNX", F.lit(10.0))
            .when(F.col("exchange") == "UPCOM", F.lit(15.0))
            .otherwise(F.lit(7.0)),
        )
        .withColumn(
            "price_limit_hit_flag",
            F.abs(F.col("pct_change_1d")) >= F.col("_price_limit_pct"),
        )
    )

    select_cols = [
        "ticker_id",
        "trade_date",
        F.col("close"),
        F.col("adj_close"),
        F.col("pct_change_1d"),
        F.col("pct_change_5d"),
        F.col("volume"),
        F.col("volume_vs_ma20_ratio"),
        F.col("value"),
        F.col("foreign_room"),
        F.col("foreign_net_value"),
        F.col("price_limit_hit_flag"),
    ]
    for c in ticker_cols:
        if c in obt.columns:
            select_cols.append(F.col(c))

    result = obt.select(*select_cols)

    out = os.path.join(gold_dir, "obt_ticker_daily_performance")
    (
        result.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("trade_date")
        .save(out)
    )
    logger.info("  obt_ticker_daily_performance: %d rows -> %s", result.count(), out)
    return result
