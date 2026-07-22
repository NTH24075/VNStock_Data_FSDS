"""Gold labels — ml_ticker_label (price_up_next_3d)."""

import logging
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from jobs.spark_session import get_spark

logger = logging.getLogger(__name__)


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
    logger.info("  ml_ticker_label: %d rows, positive rate=%.1f%% -> %s", result.count(), pos_rate, out)
