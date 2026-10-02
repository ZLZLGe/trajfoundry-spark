"""Configuration shared by the Spark normalizer and its scheduler wrapper."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

InputFormat = Literal["freerouter", "tokenplan", "sxf", "deepinfra"]


@dataclass(frozen=True, slots=True)
class SparkNormalizeConfig:
    """Validated configuration for one no-snapshot Spark normalization run.

    The config deliberately contains no credentials.  S3 credentials are
    supplied by the platform's Hadoop/Kubernetes credential provider.
    """

    input_uri: str
    output_uri: str
    input_format: InputFormat = "tokenplan"
    endpoint_url: str | None = None
    region_name: str = "us-east-1"
    prefix_shards: tuple[str, ...] = field(default_factory=tuple)

    driver_cores: int = 4
    driver_memory: str = "16g"
    driver_memory_overhead: str = "4g"
    driver_max_result_size: str = "1g"
    executor_instances: int = 24
    executor_cores: int = 4
    executor_memory: str = "32g"
    executor_memory_overhead: str = "8g"
    shuffle_partitions: int = 2048
    default_parallelism: int = 2048

    def __post_init__(self) -> None:
        if not self.input_uri.startswith("s3://"):
            raise ValueError("input_uri must be an s3:// URI")
        if not self.output_uri.startswith("s3://"):
            raise ValueError("output_uri must be an s3:// URI")
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
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if not self.prefix_shards:
            raise ValueError(
                "prefix_shards must be supplied; refusing a single Driver-side "
                "listing of a large S3 prefix"
            )
        if any(not shard or shard.startswith("/") for shard in self.prefix_shards):
            raise ValueError("prefix_shards must contain non-empty relative prefixes")

    @property
    def total_executor_cores(self) -> int:
        return self.executor_instances * self.executor_cores

    def spark_conf(self) -> dict[str, str]:
        """Return safe Spark/Hadoop settings for the initial production profile."""

        settings = {
            "spark.submit.deployMode": "cluster",
            "spark.driver.cores": str(self.driver_cores),
            "spark.driver.memory": self.driver_memory,
            "spark.driver.memoryOverhead": self.driver_memory_overhead,
            "spark.driver.maxResultSize": self.driver_max_result_size,
            "spark.executor.instances": str(self.executor_instances),
            "spark.executor.cores": str(self.executor_cores),
            "spark.executor.memory": self.executor_memory,
            "spark.executor.memoryOverhead": self.executor_memory_overhead,
            "spark.sql.shuffle.partitions": str(self.shuffle_partitions),
            "spark.default.parallelism": str(self.default_parallelism),
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.python.worker.reuse": "true",
            "spark.dynamicAllocation.enabled": "false",
            "spark.network.timeout": "600s",
            "spark.executor.heartbeatInterval": "60s",
            "spark.task.maxFailures": "4",
            "spark.hadoop.fs.s3a.connection.maximum": "32",
            "spark.hadoop.fs.s3a.threads.max": "16",
            "spark.hadoop.fs.s3a.fast.upload": "true",
            "spark.hadoop.fs.s3a.fast.upload.buffer": "disk",
        }
        if self.endpoint_url:
            settings["spark.hadoop.fs.s3a.endpoint"] = self.endpoint_url
            settings["spark.hadoop.fs.s3a.path.style.access"] = "true"
            settings["spark.hadoop.fs.s3a.connection.ssl.enabled"] = "false"
        return settings
