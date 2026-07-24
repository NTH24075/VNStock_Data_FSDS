"""Tests for the reproducible Spark UI capture profiles."""

import pytest

from scripts.spark_ui_capture import capture_configs, parse_args


def test_baseline_disables_all_compared_optimizations():
    config = capture_configs("baseline")

    assert config["spark.sql.adaptive.enabled"] == "false"
    assert config["spark.sql.adaptive.coalescePartitions.enabled"] == "false"
    assert config["spark.sql.adaptive.skewJoin.enabled"] == "false"
    assert config["spark.sql.adaptive.forceOptimizeSkewedJoin"] == "false"
    assert config["spark.sql.autoBroadcastJoinThreshold"] == "-1"


def test_optimized_enables_all_compared_optimizations():
    config = capture_configs("optimized")

    assert config["spark.sql.adaptive.enabled"] == "true"
    assert config["spark.sql.adaptive.coalescePartitions.enabled"] == "true"
    assert config["spark.sql.adaptive.skewJoin.enabled"] == "true"
    assert config["spark.sql.adaptive.forceOptimizeSkewedJoin"] == "true"
    assert config["spark.sql.autoBroadcastJoinThreshold"] == "50MB"


def test_both_modes_use_same_ui_and_partition_settings():
    baseline = capture_configs("baseline", ui_port=4050, shuffle_partitions=24)
    optimized = capture_configs("optimized", ui_port=4050, shuffle_partitions=24)

    for key in ("spark.ui.port", "spark.sql.shuffle.partitions", "spark.default.parallelism"):
        assert baseline[key] == optimized[key]


def test_cli_requires_a_known_mode():
    with pytest.raises(SystemExit):
        parse_args(["--mode", "unknown"])
