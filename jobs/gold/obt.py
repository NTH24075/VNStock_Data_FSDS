"""Gold OBT — obt_ticker_daily_performance."""

import logging
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from jobs.spark_session import get_spark

logger = logging.getLogger(__name__)


def build_obt(spark: SparkSession, gold_dir: str):
    fact_path = os.path.join(gold_dir, "fact_daily_price")
    dim_ticker_path = os.path.join(gold_dir, "dim_ticker")
    fact = spark.read.format("delta").load(fact_path)

    ticker_cols = ["ticker", "company_name", "exchange", "icb_l1", "icb_l2"]
    if os.path.exists(dim_ticker_path):
        dim_t = spark.read.format("delta").load(dim_ticker_path).select(["ticker_id"] + ticker_cols)
        fact = fact.join(dim_t, on="ticker_id", how="left")

    window = Window.partitionBy("ticker_id").orderBy("trade_date")
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
    logger.info("  obt_ticker_daily_performance: %d rows -> %s", result.count(), out)
