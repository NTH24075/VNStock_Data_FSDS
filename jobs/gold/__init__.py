"""Gold modeling jobs.

SCD2 dimensions (dim_ticker), facts (fact_daily_price, fact_foreign_flow,
fact_intraday_trade), OBT (obt_ticker_daily_performance), feature tables
(feat_ticker_daily, feat_stream_intraday, feat_ticker_unified), labels
(ml_ticker_label), training table (ml_ticker_training), and drift monitoring
(agg_feature_health_daily, feature_drift_alerts).

Every feature table carries:
  - event_timestamp — feature as-of time (for point-in-time joins)
  - created_ts       — computation time (for dedup, keep latest)

Based on schema_design.md §2-6 and generator.md §6.

Jobs:
  - gold_dimensions.py           — SCD2 merge dim_ticker (valid_from_ts, valid_to_ts, is_current),
                                    seed static dims (dim_date, dim_industry, dim_exchange, dim_session)
  - gold_facts.py                — adj_close with versioned adjustment factors, fact_daily_price,
                                    fact_foreign_flow incremental merge
  - gold_intraday.py             — fact_intraday_trade micro-batch merge
  - gold_obt.py                  — obt_ticker_daily_performance rebuild
  - gold_labels.py               — ml_ticker_label (price_up_next_3d with structural created_ts delay),
                                    ml_ticker_training (point-in-time join)
  - gold_drift_monitoring.py     — agg_feature_health_daily (daily PSI vs baseline, alert if > 0.15),
                                    feature_drift_alerts
"""
