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


def test_defaults_preserve_submit_resources_and_client_mode() -> None:
    config = _config(endpoint_url="http://ceph.example")

    settings = config.spark_conf()

    assert settings == {}
    assert config.total_executor_cores is None
    assert config.max_output_objects is None
    assert config.max_partition_objects == 20_000_000
    assert config.max_input_object_bytes == 2 * 1024**3
    assert config.scope_memory_bytes == 256 * 1024**2
    assert config.max_scope_snapshots == 10_000
    assert config.max_scope_summaries == 1_000_000
    assert config.max_scope_summary_bytes == 512 * 1024**2
    assert config.max_routing_bytes == 4 * 1024**3
    assert config.max_routing_snapshots == 2_000_000
    assert config.max_root_bytes == 2 * 1024**3
    assert config.max_prefix_shards == 1_000_000


def test_only_explicit_resource_overrides_are_emitted() -> None:
    config = _config(executor_instances=1, executor_cores=2, shuffle_partitions=3)
    assert config.spark_conf() == {
        "spark.executor.instances": "1",
        "spark.executor.cores": "2",
        "spark.sql.shuffle.partitions": "3",
    }
    assert config.total_executor_cores == 2


def test_config_requires_explicit_or_automatic_prefix_shards() -> None:
    with pytest.raises(ValueError, match="prefix_shards"):
        SparkNormalizeConfig(
            input_uri="s3://bucket/input/",
            output_uri="s3://bucket/output/",
        )
    config = SparkNormalizeConfig(
        input_uri="s3://bucket/input/",
        output_uri="s3://bucket/output/",
        auto_prefix_shards=True,
    )
    assert config.auto_prefix_shards is True


def test_config_rejects_mixing_explicit_and_automatic_prefix_discovery() -> None:
    with pytest.raises(ValueError, match="omitted"):
        _config(prefix_shards=("user/",), auto_prefix_shards=True)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("input_uri", "/local/input", "input_uri"),
        ("output_uri", "relative/output", "absolute local path"),
        ("input_format", "unknown", "unsupported input format"),
        ("prefix_shards", ("/absolute/",), "relative prefixes"),
        ("executor_cores", 0, "executor_cores"),
        ("shuffle_partitions", -1, "shuffle_partitions"),
        ("max_output_objects", 0, "max_output_objects"),
        ("max_prefix_shards", 0, "max_prefix_shards"),
        ("max_partition_objects", -1, "max_partition_objects"),
        ("input_credentials_path", "relative/credentials.json", "absolute path"),
        ("output_credentials_path", "relative/credentials.json", "absolute path"),
        ("prefix_shards", ("../",), "relative prefixes"),
        ("prefix_shards", ("47/", "47/"), "repeat or overlap"),
        ("prefix_shards", ("47", "477100353/"), "repeat or overlap"),
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


def test_credentials_path_is_not_a_spark_configuration_value() -> None:
    config = _config(
        input_credentials_path="/shared/private/credentials.json",
        output_credentials_path="/shared/private/output.json",
    )
    assert config.input_credentials_path == "/shared/private/credentials.json"
    assert config.output_credentials_path == "/shared/private/output.json"
    assert config.spark_conf() == {}


def test_config_accepts_unlimited_total_output_objects() -> None:
    assert _config(max_output_objects=None).max_output_objects is None
