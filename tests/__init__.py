"""Tests — unit and integration tests for generator and pipeline jobs.

M5 Validation: pytest + cov + mock.
Kỳ 2 extras (mutmut, hypothesis, locust) added when M15/M23 covered.

Test structure:
  unit/
    test_generator/          — price model, event generation, data problems
    test_silver/             — dedup logic, schema harmonization
    test_gold/               — SCD2 merge, adj_close, feature windows
  integration/
    test_bronze_ingest.py    — landing → Bronze with fixture data
    test_silver_pipeline.py  — Bronze → Silver with injected problems
  fixtures/
    sample_ohlcv.parquet
    sample_events.jsonl
"""
