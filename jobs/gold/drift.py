"""Drift monitoring — PSI, feature health, drift alerts, training table."""

import argparse
import logging
import os
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd
import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from jobs.spark_session import get_spark

logger = logging.getLogger(__name__)


def load_drift_start_date(
    config_path: str = "config/drift.yaml",
    default: str = "2025-09-01",
) -> date:
    """Read the configured regime-change date used by the data generator."""
    configured = os.getenv("DRIFT_START_DATE")
    path = Path(config_path)
    if configured is None and path.exists():
        with path.open(encoding="utf-8") as file_handle:
            config = yaml.safe_load(file_handle) or {}
        configured = config.get("drift_start_date")
    return date.fromisoformat(str(configured or default))


def monitoring_windows(
    dates: list[date],
    drift_start_date: date,
    baseline_size: int = 30,
    stride: int = 10,
) -> tuple[list[date], list[date]]:
    """Return the pre-drift baseline and sampled post-drift monitoring dates."""
    pre_drift = [value for value in sorted(dates) if value < drift_start_date]
    baseline = pre_drift[-baseline_size:]
    post_drift = [value for value in sorted(dates) if value >= drift_start_date]
    return baseline, post_drift[::stride]


def compute_psi(expected: pd.Series, actual: pd.Series, bins: int = 10) -> float:
    """Calculate population stability index over shared histogram bins."""
    combined = pd.concat([expected.dropna(), actual.dropna()])
    if len(combined) < bins * 2:
        return 0.0

    bin_edges = np.histogram_bin_edges(combined, bins=bins)
    e_hist, _ = np.histogram(expected.dropna(), bins=bin_edges)
    a_hist, _ = np.histogram(actual.dropna(), bins=bin_edges)

    eps = 1e-6
    e_dist = e_hist / max(e_hist.sum(), 1) + eps
    a_dist = a_hist / max(a_hist.sum(), 1) + eps

    psi = np.sum((a_dist - e_dist) * np.log(a_dist / e_dist))
    return float(max(0, psi))


def build_agg_feature_health(
    spark: SparkSession,
    gold_dir: str,
    drift_start_date: date | None = None,
):
    """Aggregate feature statistics and PSI values by monitoring date."""
    feat_path = os.path.join(gold_dir, "feat_ticker_daily")
    if not os.path.exists(feat_path):
        logger.info("  No feature data — skipping feature health")
        return

    feat_df = spark.read.format("delta").load(feat_path)
    fact_path = os.path.join(gold_dir, "fact_daily_price")
    if not os.path.exists(fact_path):
        logger.info("  No fact data — skipping feature health")
        return

    fact = spark.read.format("delta").load(fact_path)
    dates = sorted(
        [r.trade_date for r in fact.select("trade_date").distinct().orderBy("trade_date").collect()]
    )
    drift_start_date = drift_start_date or load_drift_start_date()
    baseline, monitoring_dates = monitoring_windows(dates, drift_start_date)
    baseline_dates = set(baseline)
    if len(baseline) < 30 or not monitoring_dates:
        logger.info(
            "  Need 30 pre-drift dates and post-drift observations — "
            "found %d baseline, %d monitoring dates",
            len(baseline),
            len(monitoring_dates),
        )
        return

    feature_cols = [
        "f_ticker_return_5d",
        "f_ticker_volatility_20d",
        "f_ticker_ma20_gap",
    ]

    feat_pd = feat_df.toPandas()

    rows = []
    for feature in feature_cols:
        if feature not in feat_pd.columns:
            continue
        baseline_vals = feat_pd[feat_pd["trade_date"].isin(baseline_dates)][feature]
        if len(baseline_vals.dropna()) < 10:
            continue

        rows.append(
            {
                "monitoring_date": baseline[-1],
                "feature_name": feature,
                "mean_value": round(baseline_vals.mean(), 6),
                "psi_vs_baseline": 0.0,
                "alert_flag": False,
            }
        )
        for td in monitoring_dates:
            day_vals = feat_pd[feat_pd["trade_date"] == td][feature]
            if len(day_vals.dropna()) < 10:
                continue
            psi = compute_psi(baseline_vals, day_vals)
            rows.append(
                {
                    "monitoring_date": td,
                    "feature_name": feature,
                    "mean_value": round(day_vals.mean(), 6),
                    "psi_vs_baseline": round(psi, 4),
                    "alert_flag": psi > 0.15,
                }
            )

    if not rows:
        logger.info("  Not enough data for feature health — skipping")
        return

    health_pd = pd.DataFrame(rows)
    health = spark.createDataFrame(health_pd)
    out = os.path.join(gold_dir, "agg_feature_health_daily")
    (
        health.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(out)
    )

    alerts = health_pd[health_pd["alert_flag"]]
    n_alerts = len(alerts)
    logger.info(
        "  agg_feature_health_daily: %d rows, %d alerts -> %s", len(health_pd), n_alerts, out
    )

    if n_alerts > 0:
        alert_pd = pd.DataFrame(
            {
                "alert_date": [datetime.now().date()] * n_alerts,
                "feature_name": alerts["feature_name"].values,
                "psi_value": alerts["psi_vs_baseline"].values,
                "action": ["Volatility regime shift suspected; evaluate model performance"]
                * n_alerts,
            }
        )
        alert_df = spark.createDataFrame(alert_pd)
        alert_out = os.path.join(gold_dir, "feature_drift_alerts")
        (
            alert_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(alert_out)
        )
        logger.info("  feature_drift_alerts: %d rows -> %s", n_alerts, alert_out)

    return health


def build_ml_ticker_training(spark: SparkSession, gold_dir: str):
    """Join point-in-time unified features with their future labels."""
    label_path = os.path.join(gold_dir, "ml_ticker_label")
    feat_path = os.path.join(gold_dir, "feat_ticker_unified")
    if not os.path.exists(feat_path):
        feat_path = os.path.join(gold_dir, "feat_ticker_daily")

    if not os.path.exists(label_path) or not os.path.exists(feat_path):
        logger.info("  Missing label or feature data — skipping training table")
        return

    labels = spark.read.format("delta").load(label_path)
    feats = spark.read.format("delta").load(feat_path)

    training = labels.join(
        feats.drop("event_timestamp", "created_ts"), on=["ticker_id", "trade_date"], how="inner"
    )

    feat_cols = [c for c in feats.columns if c.startswith("f_")]
    select_cols = ["ticker_id", "trade_date", "event_timestamp", "created_ts", "label"] + feat_cols
    result = training.select(*select_cols)

    out = os.path.join(gold_dir, "ml_ticker_training")
    (
        result.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(out)
    )

    pos_rate = result.filter(F.col("label") == 1).count() / max(result.count(), 1) * 100
    logger.info(
        "  ml_ticker_training: %d rows, positive rate=%.1f%% -> %s", result.count(), pos_rate, out
    )


def generate_drift_report(spark: SparkSession, gold_dir: str):
    """Export feature-health rows with reviewer-friendly drift statuses."""
    health_path = os.path.join(gold_dir, "agg_feature_health_daily")
    if not os.path.exists(health_path):
        return
    health = spark.read.format("delta").load(health_path)
    report_pd = health.toPandas()

    def drift_status(psi):
        """Map PSI to the configured severity band."""
        if psi < 0.05:
            return "baseline"
        elif psi < 0.15:
            return "detected"
        elif psi < 0.20:
            return "strong"
        return "alert"

    report_pd["drift_status"] = report_pd["psi_vs_baseline"].apply(drift_status)
    out = Path(
        os.getenv(
            "DRIFT_REPORT_PATH",
            str(Path(gold_dir) / "drift_validation_report.csv"),
        )
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    report_pd.to_csv(out, index=False)
    logger.info("Drift validation report -> %s", out)


def main():
    """Run drift aggregation, training-table build, and CSV export."""
    parser = argparse.ArgumentParser(description="Drift monitoring and training table")
    parser.add_argument("--gold-dir", default="data/gold")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    spark = get_spark()

    logger.info("=== Drift: Feature Health ===")
    build_agg_feature_health(spark, args.gold_dir)

    logger.info("=== Training Table ===")
    build_ml_ticker_training(spark, args.gold_dir)

    logger.info("=== Drift Report ===")
    generate_drift_report(spark, args.gold_dir)

    spark.stop()
    logger.info("Drift monitoring complete.")


if __name__ == "__main__":
    main()
