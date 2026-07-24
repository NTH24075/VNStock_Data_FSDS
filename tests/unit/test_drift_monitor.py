from datetime import date, timedelta

import numpy as np
import pandas as pd

from jobs.gold.drift import compute_psi, monitoring_windows


class TestComputePsi:
    def test_identical_distributions_zero_psi(self):
        data = np.random.default_rng(42).normal(0, 1, 1000)
        expected = pd.Series(data)
        actual = pd.Series(data)
        psi = compute_psi(expected, actual, bins=10)
        assert psi == 0.0

    def test_different_distributions_positive_psi(self):
        rng = np.random.default_rng(42)
        expected = pd.Series(rng.normal(0, 1, 1000))
        actual = pd.Series(rng.normal(1, 2, 1000))
        psi = compute_psi(expected, actual, bins=10)
        assert psi > 0.0

    def test_small_sample_returns_zero(self):
        expected = pd.Series([1.0, 2.0])
        actual = pd.Series([3.0, 4.0])
        psi = compute_psi(expected, actual, bins=10)
        assert psi == 0.0

    def test_with_nan_values(self):
        rng = np.random.default_rng(42)
        data = list(rng.normal(0, 1, 900)) + [np.nan] * 100
        expected = pd.Series(data)
        actual = pd.Series(data)
        psi = compute_psi(expected, actual, bins=10)
        assert psi == 0.0


def test_monitoring_windows_uses_immediate_pre_drift_baseline():
    start = date(2025, 9, 1)
    dates = [start + timedelta(days=offset) for offset in range(-40, 21)]

    baseline, monitoring = monitoring_windows(dates, start, baseline_size=30, stride=10)

    assert baseline == dates[10:40]
    assert monitoring == [start, start + timedelta(days=10), start + timedelta(days=20)]
