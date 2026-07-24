"""Run one reproducible Spark workload and keep its driver UI capture-ready."""

from __future__ import annotations

import argparse
import os
import time
from collections.abc import Sequence

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.spark_session import get_spark

CAPTURE_MODES = ("baseline", "optimized")


def capture_configs(
    mode: str,
    *,
    ui_port: int = 4040,
    shuffle_partitions: int = 32,
) -> dict[str, str]:
    """Return the only Spark SQL settings that differ between capture modes."""
    if mode not in CAPTURE_MODES:
        raise ValueError(f"mode must be one of {CAPTURE_MODES}, got {mode!r}")

    common = {
        "spark.ui.enabled": "true",
        "spark.ui.port": str(ui_port),
        "spark.ui.bindAddress": "0.0.0.0",
        "spark.ui.showConsoleProgress": "true",
        "spark.sql.ui.explainMode": "extended",
        "spark.sql.shuffle.partitions": str(shuffle_partitions),
        "spark.default.parallelism": str(shuffle_partitions),
        "spark.driver.bindAddress": "0.0.0.0",
        "spark.driver.host": os.getenv("SPARK_DRIVER_HOST", "spark-capture"),
        # Fixed internal ports make the client-mode driver reachable from workers.
        "spark.driver.port": "7078",
        "spark.blockManager.port": "7079",
    }
    profiles = {
        "baseline": {
            "spark.sql.adaptive.enabled": "false",
            "spark.sql.adaptive.coalescePartitions.enabled": "false",
            "spark.sql.adaptive.skewJoin.enabled": "false",
            "spark.sql.adaptive.forceOptimizeSkewedJoin": "false",
            "spark.sql.autoBroadcastJoinThreshold": "-1",
        },
        "optimized": {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.adaptive.forceOptimizeSkewedJoin": "true",
            # Low evidence thresholds make AQE decisions visible on the
            # coursework-sized deterministic dataset.
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "512KB",
            "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "2",
            "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256KB",
            "spark.sql.autoBroadcastJoinThreshold": "50MB",
        },
    }
    return {**common, **profiles[mode]}


def _run_action(
    spark: SparkSession,
    group_id: str,
    description: str,
    frame: DataFrame,
) -> list:
    """Collect a small result while assigning stable labels in Spark Jobs UI."""
    spark.sparkContext.setJobGroup(group_id, description, interruptOnCancel=True)
    spark.sparkContext.setJobDescription(description)
    spark.sparkContext.setLocalProperty("callSite.short", description)
    spark.sparkContext.setLocalProperty(
        "callSite.long",
        f"{description}\nReproducible Spark UI evidence workload",
    )
    started = time.monotonic()
    try:
        result = frame.collect()
    finally:
        # PySpark 3.5 exposes setJobGroup but not SparkContext.clearJobGroup.
        # Clear the same thread-local properties explicitly.
        spark.sparkContext.setLocalProperty("spark.jobGroup.id", None)
        spark.sparkContext.setLocalProperty("spark.job.description", None)
        spark.sparkContext.setLocalProperty("spark.job.interruptOnCancel", None)
        spark.sparkContext.setLocalProperty("callSite.short", None)
        spark.sparkContext.setLocalProperty("callSite.long", None)
    elapsed = time.monotonic() - started
    print(f"\n[{group_id}] finished in {elapsed:.2f}s; result rows={len(result)}")
    for row in result[:10]:
        print(f"  {row}")
    return result


def _print_plan_highlights(label: str, frame: DataFrame) -> None:
    """Print concise evidence tokens from an action's executed physical plan."""
    plan = frame._jdf.queryExecution().executedPlan().toString()
    tokens = (
        "AdaptiveSparkPlan",
        "BroadcastHashJoin",
        "SortMergeJoin",
        "AQEShuffleRead",
        "isSkew=true",
    )
    highlights = [
        line.strip() for line in plan.splitlines() if any(token in line for token in tokens)
    ]
    print(f"\nPlan highlights for {label}:")
    for line in highlights:
        print(f"  {line}")


def run_capture_workload(
    spark: SparkSession,
    *,
    data_root: str,
    scale: int,
    shuffle_partitions: int,
) -> None:
    """Execute identical skew, join and aggregation actions in both modes."""
    if scale < 1:
        raise ValueError("scale must be at least 1")

    fact_path = os.path.join(data_root, "gold", "fact_daily_price")
    dimension_path = os.path.join(data_root, "gold", "dim_ticker")
    missing = [path for path in (fact_path, dimension_path) if not os.path.isdir(path)]
    if missing:
        joined = ", ".join(missing)
        raise FileNotFoundError(
            f"Capture input is missing: {joined}. Run the offline Silver/Gold path first."
        )

    facts = (
        spark.read.format("delta")
        .load(fact_path)
        .select("ticker_id", "trade_date", "close", "volume")
    )
    dimensions = (
        spark.read.format("delta")
        .load(dimension_path)
        .filter(F.col("is_current"))
        .select("ticker_id", "exchange", "icb_l1")
    )

    # About 5% of ticker keys share HOT as their shuffle key. Those rows are
    # expanded 32x more than the remaining keys, producing one repeatable,
    # visibly skewed partition without changing source data between modes.
    is_hot = F.pmod(F.xxhash64("ticker_id"), F.lit(20)) == F.lit(0)
    expanded = (
        facts.withColumn(
            "partition_key",
            F.when(is_hot, F.lit("HOT")).otherwise(F.col("ticker_id")),
        )
        .withColumn(
            "_repeats",
            F.when(is_hot, F.lit(scale * 32)).otherwise(F.lit(scale)),
        )
        .withColumn("_copy_id", F.explode(F.sequence(F.lit(1), F.col("_repeats"))))
        .withColumn(
            "notional",
            F.col("close") * F.col("volume") / F.col("_repeats"),
        )
        .drop("_repeats")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    _run_action(
        spark,
        "00_materialize_same_input",
        "00 Materialize deterministic amplified fact input",
        expanded.agg(
            F.count("*").alias("expanded_rows"),
            F.countDistinct("ticker_id").alias("ticker_count"),
            F.sum(F.when(F.col("partition_key") == "HOT", 1).otherwise(0)).alias(
                "hot_rows"
            ),
        ),
    )

    skew_aggregation = (
        expanded.repartition(shuffle_partitions, "partition_key")
        .groupBy("partition_key")
        .agg(
            F.count("*").alias("event_count"),
            F.sum("notional").alias("total_notional"),
        )
        .orderBy(F.desc("event_count"))
        .limit(20)
    )
    _run_action(
        spark,
        "01_skewed_shuffle_aggregation",
        "01 Skewed shuffle aggregation (compare task duration and shuffle read)",
        skew_aggregation,
    )

    dimension_join = (
        expanded.repartition(shuffle_partitions, "ticker_id")
        .join(dimensions, on="ticker_id", how="inner")
        .groupBy("exchange", "icb_l1")
        .agg(
            F.count("*").alias("event_count"),
            F.sum("notional").alias("total_notional"),
        )
        .orderBy(F.desc("event_count"))
    )
    _run_action(
        spark,
        "02_dimension_join",
        "02 Dimension join (SortMerge baseline vs BroadcastHash optimized)",
        dimension_join,
    )

    risk_by_partition = (
        facts.select(
            F.when(is_hot, F.lit("HOT")).otherwise(F.col("ticker_id")).alias(
                "partition_key"
            )
        )
        .distinct()
        .withColumn(
            "risk_weight",
            F.when(F.col("partition_key") == "HOT", F.lit(1.5)).otherwise(F.lit(1.0)),
        )
    )
    skew_join_input = (
        expanded.select(
            "partition_key",
            "ticker_id",
            "trade_date",
            "_copy_id",
            "notional",
        )
        # Carry a deterministic, non-compressible value through the shuffle so
        # the coursework-sized HOT partition crosses AQE's byte threshold.
        .withColumn(
            "payload_token",
            F.sha2(
                F.concat_ws(
                    "|",
                    "ticker_id",
                    F.col("trade_date").cast("string"),
                    F.col("_copy_id").cast("string"),
                ),
                256,
            ),
        )
        .hint("merge")
    )
    skew_merge_join = (
        skew_join_input.join(
            risk_by_partition.hint("merge"),
            on="partition_key",
            how="inner",
        )
        .groupBy("partition_key")
        .agg(
            F.count("*").alias("event_count"),
            F.sum(F.col("notional") * F.col("risk_weight")).alias(
                "risk_adjusted_notional"
            ),
            F.sum(F.length("payload_token")).alias("shuffled_payload_bytes"),
        )
        .orderBy(F.desc("event_count"))
        .limit(20)
    )
    _run_action(
        spark,
        "03_skew_merge_join",
        "03 Forced merge join (inspect AQE skew partition handling)",
        skew_merge_join,
    )

    _print_plan_highlights("02_dimension_join", dimension_join)
    _print_plan_highlights("03_skew_merge_join", skew_merge_join)
    print("\nFinal physical plan for 02_dimension_join:")
    dimension_join.explain(mode="formatted")
    print("\nFinal physical plan for 03_skew_merge_join:")
    skew_merge_join.explain(mode="formatted")
    expanded.unpersist()


def _print_capture_summary(
    spark: SparkSession,
    *,
    mode: str,
    hold_seconds: int,
    scale: int,
) -> None:
    """Print the capture mode, relevant Spark settings, and UI hold duration."""
    keys = (
        "spark.sql.adaptive.enabled",
        "spark.sql.adaptive.coalescePartitions.enabled",
        "spark.sql.adaptive.skewJoin.enabled",
        "spark.sql.adaptive.forceOptimizeSkewedJoin",
        "spark.sql.autoBroadcastJoinThreshold",
        "spark.sql.shuffle.partitions",
    )
    print("\n" + "=" * 72)
    print(f"CAPTURE READY: {mode.upper()}")
    print(f"Application: {spark.sparkContext.appName}")
    print("Driver UI:   http://localhost:4040")
    print("Master UI:   http://localhost:8080")
    print(f"Scale:       {scale}")
    for key in keys:
        print(f"{key}={spark.conf.get(key)}")
    print(
        f"Driver will stay alive for {hold_seconds}s. "
        "Press Ctrl+C after screenshots are complete."
    )
    print("=" * 72, flush=True)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse capture mode and evidence sizing options."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=CAPTURE_MODES, required=True)
    parser.add_argument(
        "--hold-seconds",
        type=int,
        default=600,
        help="seconds to keep the completed driver's UI alive",
    )
    parser.add_argument(
        "--scale",
        type=int,
        default=4,
        help="cold-key row multiplier; hot keys use 32x this value",
    )
    parser.add_argument("--shuffle-partitions", type=int, default=32)
    parser.add_argument("--ui-port", type=int, default=4040)
    parser.add_argument("--data-root", default=os.getenv("DATA_ROOT", "/opt/project/data"))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    """Create the capture driver, execute its workload, then retain its UI."""
    args = parse_args(argv)
    if args.hold_seconds < 1:
        raise ValueError("--hold-seconds must be at least 1")
    configs = capture_configs(
        args.mode,
        ui_port=args.ui_port,
        shuffle_partitions=args.shuffle_partitions,
    )
    spark = get_spark(f"vnstock-ui-{args.mode}", configs)
    try:
        spark.sparkContext.setLogLevel("WARN")
        run_capture_workload(
            spark,
            data_root=args.data_root,
            scale=args.scale,
            shuffle_partitions=args.shuffle_partitions,
        )
        _print_capture_summary(
            spark,
            mode=args.mode,
            hold_seconds=args.hold_seconds,
            scale=args.scale,
        )
        time.sleep(args.hold_seconds)
    except KeyboardInterrupt:
        print("\nCapture stopped by user.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
