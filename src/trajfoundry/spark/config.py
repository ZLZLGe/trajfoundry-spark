"""Configuration shared by the Spark normalizer and its scheduler wrapper."""

from __future__ import annotations

from dataclasses import dataclass, field
from itertools import pairwise
from pathlib import Path
from typing import Literal

from .storage import local_output_path

InputFormat = Literal["freerouter", "tokenplan", "sxf", "deepinfra"]


@dataclass(frozen=True, slots=True)
class SparkNormalizeConfig:
    """Validated configuration for one no-snapshot Spark normalization run.

    The config deliberately contains no credential values. Input credentials
    use the platform SDK chain or an executor-readable secure file. Resource
    and deploy settings belong to spark-submit; None preserves those settings.
    """

    input_uri: str
    output_uri: str
    input_format: InputFormat = "tokenplan"
    endpoint_url: str | None = None
    region_name: str = "us-east-1"
    prefix_shards: tuple[str, ...] = field(default_factory=tuple)
    # When enabled, the Spark job discovers immediate user-directory prefixes
    # below input_uri with one executor-side S3 delimiter listing.  Only the
    # bounded prefix names are collected by the driver; object keys/bodies are
    # still listed and read by the normal executor partitions.
    auto_prefix_shards: bool = False
    max_prefix_shards: int = 1_000_000
    input_credentials_path: str | None = None
    output_credentials_path: str | None = None
    retain_s3_auxiliary: bool = False
    # Explicit retry mode for an S3 prefix that already contains a prior
    # attempt. Existing objects are treated as idempotent when their bytes
    # match and are replaced when the new successful run produces different
    # bytes; the final manifest remains authoritative.
    allow_existing_output: bool = False

    driver_cores: int | None = None
    driver_memory: str | None = None
    driver_memory_overhead: str | None = None
    driver_max_result_size: str | None = None
    executor_instances: int | None = None
    executor_cores: int | None = None
    executor_memory: str | None = None
    executor_memory_overhead: str | None = None
    shuffle_partitions: int | None = None
    default_parallelism: int | None = None
    # The Driver-side inventory is disk-streamed, so the total output count is
    # unlimited by default.  A positive integer remains available for bounded
    # smoke tests or deployments that explicitly want an admission limit.
    max_output_objects: int | None = None
    max_partition_objects: int = 20_000_000
    max_input_object_bytes: int = 2 * 1024 * 1024 * 1024
    scope_memory_bytes: int = 256 * 1024 * 1024
    max_scope_snapshots: int = 10_000
    max_scope_summaries: int = 1_000_000
    max_scope_summary_bytes: int = 512 * 1024 * 1024
    max_routing_bytes: int = 4 * 1024 * 1024 * 1024
    max_routing_snapshots: int = 2_000_000
    max_root_bytes: int = 2 * 1024 * 1024 * 1024

    def __post_init__(self) -> None:
        if not self.input_uri.startswith("s3://"):
            raise ValueError("input_uri must be an s3:// URI")
        # Filesystem output is a Driver/Executor shared directory, not private
        # executor scratch. Workers verify the Driver's run marker at runtime.
        local_path = local_output_path(self.output_uri)
        if type(self.retain_s3_auxiliary) is not bool:
            raise ValueError("retain_s3_auxiliary must be a boolean")
        if type(self.allow_existing_output) is not bool:
            raise ValueError("allow_existing_output must be a boolean")
        if type(self.auto_prefix_shards) is not bool:
            raise ValueError("auto_prefix_shards must be a boolean")
        if self.retain_s3_auxiliary and local_path is not None:
            raise ValueError("retain_s3_auxiliary is only supported for S3 output")
        if self.allow_existing_output and local_path is not None:
            raise ValueError("allow_existing_output is only supported for S3 output")
        for name in ("input_credentials_path", "output_credentials_path"):
            value = getattr(self, name)
            if value is not None and not Path(value).is_absolute():
                raise ValueError(f"{name} must be an absolute path")
        if self.input_format not in {"freerouter", "tokenplan", "sxf", "deepinfra"}:
            raise ValueError(f"unsupported input format: {self.input_format!r}")
        for name in (
            "driver_cores",
            "executor_instances",
            "executor_cores",
            "shuffle_partitions",
            "default_parallelism",
        ):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or value <= 0):
                raise ValueError(f"{name} must be a positive integer")
        if self.max_output_objects is not None and (
            type(self.max_output_objects) is not int or self.max_output_objects <= 0
        ):
            raise ValueError("max_output_objects must be a positive integer or None")
        for name in (
            "max_partition_objects",
            "max_input_object_bytes",
            "scope_memory_bytes",
            "max_scope_snapshots",
            "max_scope_summaries",
            "max_scope_summary_bytes",
            "max_routing_bytes",
            "max_routing_snapshots",
            "max_root_bytes",
            "max_prefix_shards",
        ):
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if self.auto_prefix_shards and self.prefix_shards:
            raise ValueError(
                "prefix_shards must be omitted when auto_prefix_shards is enabled"
            )
        if not self.prefix_shards and not self.auto_prefix_shards:
            raise ValueError(
                "prefix_shards must be supplied; refusing a single Driver-side "
                "listing of a large S3 prefix; set auto_prefix_shards=True to "
                "discover immediate user prefixes on an executor"
            )
        validate_prefix_shards(self.prefix_shards)

    @property
    def total_executor_cores(self) -> int | None:
        if self.executor_instances is None or self.executor_cores is None:
            return None
        return self.executor_instances * self.executor_cores

    def spark_conf(self) -> dict[str, str]:
        """Return only explicitly requested overrides, for Client or Cluster mode."""

        settings = {
            "spark.driver.cores": self.driver_cores,
            "spark.driver.memory": self.driver_memory,
            "spark.driver.memoryOverhead": self.driver_memory_overhead,
            "spark.driver.maxResultSize": self.driver_max_result_size,
            "spark.executor.instances": self.executor_instances,
            "spark.executor.cores": self.executor_cores,
            "spark.executor.memory": self.executor_memory,
            "spark.executor.memoryOverhead": self.executor_memory_overhead,
            "spark.sql.shuffle.partitions": self.shuffle_partitions,
            "spark.default.parallelism": self.default_parallelism,
        }
        return {key: str(value) for key, value in settings.items() if value is not None}


def validate_prefix_shards(prefix_shards: tuple[str, ...] | list[str]) -> None:
    """Validate a resolved list of non-overlapping relative input prefixes."""

    if any(
        not isinstance(shard, str)
        or not shard
        or shard.startswith("/")
        or ".." in shard.split("/")
        or "\\" in shard
        for shard in prefix_shards
    ):
        raise ValueError("prefix_shards must contain non-empty relative prefixes")
    if len(set(prefix_shards)) != len(prefix_shards):
        raise ValueError("prefix_shards must not repeat or overlap")
    ordered = sorted(prefix_shards)
    if any(right.startswith(left) for left, right in pairwise(ordered)):
        raise ValueError("prefix_shards must not repeat or overlap")
