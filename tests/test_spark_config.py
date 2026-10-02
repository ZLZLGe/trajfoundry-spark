from __future__ import annotations

import pytest

from trajfoundry.spark.config import SparkNormalizeConfig


def _config(**overrides: object) -> SparkNormalizeConfig:
    values: dict[str, object] = {
        "input_uri": "s3://agent-trajectory/lakehouse/token-plan/masked-raw/v002/dt=2026-09-16/",
        "output_uri": "s3://agent-trajectory/lakehouse/token-plan/normalized/v005/dt=2026-09-16/",
        "prefix_shards": ("00/", "01/"),
    }
    values.update(overrides)
    return SparkNormalizeConfig(**values)


def test_initial_profile_is_cluster_mode_and_has_bounded_s3_concurrency() -> None:
    config = _config(endpoint_url="http://ceph.example")

    settings = config.spark_conf()

    assert settings["spark.submit.deployMode"] == "cluster"
    assert settings["spark.executor.instances"] == "24"
    assert settings["spark.executor.cores"] == "4"
    assert settings["spark.executor.memoryOverhead"] == "8g"
    assert settings["spark.sql.adaptive.enabled"] == "true"
    assert settings["spark.dynamicAllocation.enabled"] == "false"
    assert settings["spark.hadoop.fs.s3a.connection.maximum"] == "32"
    assert settings["spark.hadoop.fs.s3a.threads.max"] == "16"
    assert settings["spark.hadoop.fs.s3a.fast.upload.buffer"] == "disk"
    assert settings["spark.hadoop.fs.s3a.endpoint"] == "http://ceph.example"
    assert settings["spark.hadoop.fs.s3a.path.style.access"] == "true"
    assert settings["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "false"
    assert config.total_executor_cores == 96


def test_config_requires_explicit_prefix_shards_to_avoid_driver_listing() -> None:
    with pytest.raises(ValueError, match="prefix_shards"):
        SparkNormalizeConfig(
            input_uri="s3://bucket/input",
            output_uri="s3://bucket/output",
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("input_uri", "/local/input", "input_uri"),
        ("output_uri", "/local/output", "output_uri"),
        ("input_format", "unknown", "unsupported input format"),
        ("prefix_shards", ("/absolute/",), "relative prefixes"),
        ("executor_cores", 0, "executor_cores"),
        ("shuffle_partitions", -1, "shuffle_partitions"),
    ],
)
def test_config_rejects_unsafe_or_invalid_values(
    field: str, value: object, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        _config(**{field: value})


def test_config_accepts_all_supported_input_formats() -> None:
    for input_format in ("tokenplan", "freerouter", "deepinfra", "sxf"):
        assert _config(input_format=input_format).input_format == input_format
