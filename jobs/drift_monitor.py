"""Drift monitoring — PSI computation vs baseline, feature health, drift alerts.

Computes Population Stability Index (PSI) for monitored features against a
pre-drift baseline window, populates agg_feature_health_daily and
feature_drift_alerts Gold tables.

Usage:
    python jobs/drift_monitor.py --gold-dir data/gold
"""

import argparse
import os
from datetime import datetime

import numpy as np
import pandas as pd

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def get_spark(app_name: str = "drift_monitor") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def compute_psi(expected: pd.Series, actual: pd.Series, bins: int = 10) -> float:
    """Population Stability Index between expected and actual distributions."""
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


def build_agg_feature_health(spark: SparkSession, gold_dir: str):
    feat_path = os.path.join(gold_dir, "feat_ticker_daily")
    if not os.path.exists(feat_path):
        print("  No feature data — skipping feature health")
        return

    feat_df = spark.read.format("delta").load(feat_path)
    fact_path = os.path.join(gold_dir, "fact_daily_price")
    if not os.path.exists(fact_path):
        print("  No fact data — skipping feature health")
        return

    fact = spark.read.format("delta").load(fact_path)
    dates = sorted([r.trade_date for r in fact.select("trade_date").distinct().orderBy("trade_date").collect()])
    baseline_dates = set(dates[:30])

    feature_cols = [
        "f_ticker_return_5d", "f_ticker_volatility_20d", "f_ticker_ma20_gap",
    ]

    # Collect data to pandas for PSI (numpy-based)
    feat_pd = feat_df.toPandas()

    rows = []
    for feature in feature_cols:
        if feature not in feat_pd.columns:
            continue
        baseline_vals = feat_pd[feat_pd["trade_date"].isin(baseline_dates)][feature]
        if len(baseline_vals.dropna()) < 10:
            continue

        post_dates = [d for d in dates if d not in baseline_dates]
        for td in post_dates[::10]:
            day_vals = feat_pd[feat_pd["trade_date"] == td][feature]
            if len(day_vals.dropna()) < 10:
                continue
            psi = compute_psi(baseline_vals, day_vals)
            rows.append({
                "monitoring_date": td,
                "feature_name": feature,
                "mean_value": round(day_vals.mean(), 6),
                "psi_vs_baseline": round(psi, 4),
                "alert_flag": psi > 0.15,
            })

    if not rows:
        print("  Not enough data for feature health — skipping")
        return

    health_pd = pd.DataFrame(rows)
    health = spark.createDataFrame(health_pd)
    out = os.path.join(gold_dir, "agg_feature_health_daily")
    health.write.format("delta").mode("overwrite").save(out)

    alerts = health_pd[health_pd["alert_flag"]]
    n_alerts = len(alerts)
    print(f"  agg_feature_health_daily: {len(health_pd)} rows, {n_alerts} alerts → {out}")

    if n_alerts > 0:
        alert_pd = pd.DataFrame({
            "alert_date": [datetime.now().date()] * n_alerts,
            "feature_name": alerts["feature_name"].values,
            "psi_value": alerts["psi_vs_baseline"].values,
            "action": ["Volatility regime shift suspected; evaluate model performance"] * n_alerts,
        })
        alert_df = spark.createDataFrame(alert_pd)
        alert_out = os.path.join(gold_dir, "feature_drift_alerts")
        alert_df.write.format("delta").mode("overwrite").save(alert_out)
        print(f"  feature_drift_alerts: {n_alerts} rows → {alert_out}")

    return health


def build_ml_ticker_training(spark: SparkSession, gold_dir: str):
    label_path = os.path.join(gold_dir, "ml_ticker_label")
    feat_path = os.path.join(gold_dir, "feat_ticker_daily")

    if not os.path.exists(label_path) or not os.path.exists(feat_path):
        print("  Missing label or feature data — skipping training table")
        return

    labels = spark.read.format("delta").load(label_path)
    feats = spark.read.format("delta").load(feat_path)

    training = labels.join(feats, on=["ticker_id", "trade_date"], how="inner")

    feat_cols = [c for c in feats.columns if c.startswith("f_")]
    select_cols = ["ticker_id", "trade_date", "event_timestamp", "created_ts", "label"] + feat_cols
    result = training.select(*select_cols)

    out = os.path.join(gold_dir, "ml_ticker_training")
    result.write.format("delta").mode("overwrite").save(out)

    pos_rate = result.filter(F.col("label") == 1).count() / max(result.count(), 1) * 100
    print(f"  ml_ticker_training: {result.count()} rows, positive rate={pos_rate:.1f}% → {out}")


def generate_drift_report(spark: SparkSession, gold_dir: str):
    health_path = os.path.join(gold_dir, "agg_feature_health_daily")
    if not os.path.exists(health_path):
        return
    health = spark.read.format("delta").load(health_path)
    report_pd = health.toPandas()

    def drift_status(psi):
        if psi < 0.05:
            return "baseline"
        elif psi < 0.15:
            return "detected"
        elif psi < 0.20:
            return "strong"
        return "alert"

    report_pd["drift_status"] = report_pd["psi_vs_baseline"].apply(drift_status)
    out = "data/drift_validation_report.csv"
    report_pd.to_csv(out, index=False)
    print(f"\nDrift validation report → {out}")


def main():
    parser = argparse.ArgumentParser(description="Drift monitoring and training table")
    parser.add_argument("--gold-dir", default="data/gold")
    args = parser.parse_args()

    spark = get_spark()

    print("\n=== Drift: Feature Health ===")
    build_agg_feature_health(spark, args.gold_dir)

    print("\n=== Training Table ===")
    build_ml_ticker_training(spark, args.gold_dir)

    print("\n=== Drift Report ===")
    generate_drift_report(spark, args.gold_dir)

    spark.stop()
    print("Drift monitoring complete.")


if __name__ == "__main__":
    main()
