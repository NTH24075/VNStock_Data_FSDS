"""Gold labels — ml_ticker_label (price_up_next_3d)."""

import logging
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def build_ml_ticker_label(spark: SparkSession, gold_dir: str):
    """Build the next-three-trading-day direction label for coursework 03."""
    fact_path = os.path.join(gold_dir, "fact_daily_price")
    fact = spark.read.format("delta").load(fact_path)

    window = Window.partitionBy("ticker_id").orderBy("trade_date")
    df = fact.withColumn("close_t3", F.lead("adj_close", 3).over(window)).withColumn(
        "label_available_date",
        F.lead("trade_date", 3).over(window),
    )
    labels = df.filter(F.col("close_t3").isNotNull())

    labels = labels.withColumn(
        "label",
        (F.col("close_t3") > F.col("adj_close") * 1.01).cast("int"),
    )

    result = labels.select(
        F.col("ticker_id"),
        F.col("trade_date"),
        F.col("trade_date").cast("timestamp").alias("event_timestamp"),
        F.to_timestamp(
            F.concat_ws(" ", F.col("label_available_date").cast("string"), F.lit("15:10:00"))
        ).alias("created_ts"),
        F.col("label"),
    )

    out = os.path.join(gold_dir, "ml_ticker_label")
    (
        result.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(out)
    )

    pos_rate = result.filter(F.col("label") == 1).count() / max(result.count(), 1) * 100
    logger.info(
        "  ml_ticker_label: %d rows, positive rate=%.1f%% -> %s", result.count(), pos_rate, out
    )
