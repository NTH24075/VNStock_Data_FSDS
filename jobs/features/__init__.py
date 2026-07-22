"""Feature computation jobs.

All feature tables carry:
  - event_timestamp — feature as-of time (for point-in-time joins)
  - created_ts       — computation time (for dedup, keep latest)

Jobs:
  - feat_daily_job.py           — feat_ticker_daily (offline windows over trading days)
  - feat_stream_job.py          — feat_stream_intraday (Flink rolling windows)
  - feat_unified_job.py         — feat_ticker_unified (point-in-time join of daily + intraday)

Based on schema_design.md §6.
"""
