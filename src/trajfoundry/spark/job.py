"""Spark Cluster entrypoint for no-snapshot trajectory normalization."""

from __future__ import annotations

import hashlib
import logging
import tempfile
from collections import defaultdict
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

import orjson

from ..canonical import trajectory_id
from ..models import Snapshot
from ..quality import is_strict_sample
from ..s3 import S3Location, create_s3_client, parse_s3_uri
from .adapters import parse_payload
from .build import BuiltTrajectory, build_scope
from .config import SparkNormalizeConfig
from .output import (
    ManifestAccumulator,
    SparkObjectDescription,
    deterministic_s3_put,
    deterministic_s3_put_file,
    lineage_line,
    merge_lineage_parts,
    serialize_trajectory,
)
from .source import iter_shard_payloads

LOGGER = logging.getLogger(__name__)
_MAX_TRAJECTORY_BYTES = 512 * 1024 * 1024


def _read_partition(
    shards: Iterable[str],
    *,
    input_uri: str,
    input_format: str,
    endpoint_url: str | None,
    region_name: str,
) -> Iterator[tuple[str, bytes, str]]:
    for shard in shards:
        yield from iter_shard_payloads(
            input_uri,
            shard,
            input_format=input_format,
            endpoint_url=endpoint_url,
            region_name=region_name,
        )


def _parse_partition(
    records: Iterable[tuple[str, bytes, str]],
    *,
    input_format: str,
    input_counter: Any,
    error_counter: Any,
) -> Iterator[tuple[str, Any]]:
    for source_ref, payload, _listed_digest in records:
        input_counter.add(1)
        try:
            row = parse_payload(source_ref, payload, input_format=input_format)  # type: ignore[arg-type]
        except Exception as error:  # noqa: BLE001 - one malformed input is quarantined
            # Do not include payloads or exception details: they may contain user data.
            error_counter.add(1)
            yield "error", type(error).__name__
        else:
            yield "ok", row.as_tuple()


def _build_partition(
    rows: Iterable[tuple[tuple[str, str], Iterable[tuple[Any, ...]]]]
) -> Iterator[BuiltTrajectory]:
    for _scope, values in rows:
        snapshots: list[Snapshot] = []
        for value in values:
            snapshots.append(Snapshot.model_validate_json(value[-1]))
        yield from build_scope(snapshots)


def _publish_partition(
    partition_id: int,
    artifacts: Iterable[BuiltTrajectory],
    *,
    output_uri: str,
    endpoint_url: str | None,
    region_name: str,
) -> Iterator[dict[str, Any]]:
    ordered = sorted(artifacts, key=lambda item: trajectory_id(item.node))
    if not ordered:
        return
    location = parse_s3_uri(output_uri)
    client = create_s3_client(
        endpoint_url or "https://s3.amazonaws.com",
        region_name,
        max_pool_connections=16,
    )
    descriptors: list[dict[str, Any]] = []
    accepted = quarantined = duplicates = 0
    reason_counts: dict[str, int] = defaultdict(int)
    with tempfile.NamedTemporaryFile(
        prefix=f"trajfoundry-lineage-{partition_id}-", suffix=".jsonl"
    ) as lineage_file:
        for built in ordered:
            artifact = serialize_trajectory(
                built.node,
                built.origins,
                max_bytes=_MAX_TRAJECTORY_BYTES,
            )
            description = deterministic_s3_put(
                client,
                location,
                artifact.filename,
                artifact.payload,
            )
            descriptors.append(description.manifest_entry())
            lineage_file.write(lineage_line(artifact))
            if is_strict_sample(built.node):
                accepted += 1
            else:
                quarantined += 1
                if built.node.normalization_audit:
                    for reason in built.node.normalization_audit.reason_codes:
                        reason_counts[reason] += 1
            duplicates += max(0, len(built.origins) - 1)
        lineage_file.flush()
        # The part is transient and flat by design.  It is removed by the driver
        # after the canonical lineage.jsonl and manifest have been published.
        part_name = f"._lineage_part_{partition_id:06d}.jsonl"
        part_description = deterministic_s3_put_file(
            client,
            location,
            part_name,
            lineage_file.name,
        )
    yield {
        "partition_id": partition_id,
        "objects": descriptors,
        "lineage_part": part_description.manifest_entry(),
        "accepted": accepted,
        "quarantined": quarantined,
        "duplicates": duplicates,
        "reason_counts": dict(reason_counts),
    }


def _config_hash(config: SparkNormalizeConfig) -> str:
    payload = {
        "input_uri": config.input_uri,
        "output_uri": config.output_uri,
        "input_format": config.input_format,
        "prefix_shards": list(config.prefix_shards),
        "spark_conf": config.spark_conf(),
    }
    return hashlib.sha256(orjson.dumps(payload, option=orjson.OPT_SORT_KEYS)).hexdigest()


def _download_part(client: Any, location: S3Location, relative_path: str, destination: Path) -> None:
    response = client.get_object(Bucket=location.bucket, Key=location.key(relative_path))
    body = response["Body"]
    try:
        with destination.open("wb") as handle:
            while chunk := body.read(8 * 1024 * 1024):
                handle.write(chunk)
    finally:
        close = getattr(body, "close", None)
        if callable(close):
            close()


def _cleanup_stale_jsonl(
    client: Any,
    location: S3Location,
    current_paths: set[str],
) -> None:
    """Remove only top-level JSONL objects not declared by the new manifest."""

    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=location.bucket, Prefix=location.prefix):
        for item in page.get("Contents", ()) or ():
            key = item.get("Key")
            if not isinstance(key, str) or not key.startswith(location.prefix):
                continue
            relative = key[len(location.prefix) :]
            if "/" in relative or not relative.endswith(".jsonl"):
                continue
            if relative not in current_paths:
                client.delete_object(Bucket=location.bucket, Key=key)


def _merge_counts(accumulator: ManifestAccumulator, results: list[dict[str, Any]]) -> None:
    accumulator.stats.accepted += sum(int(item.get("accepted", 0)) for item in results)
    accumulator.stats.quarantined_trajectories += sum(
        int(item.get("quarantined", 0)) for item in results
    )
    accumulator.stats.duplicate_trajectories += sum(
        int(item.get("duplicates", 0)) for item in results
    )
    for item in results:
        for reason, count in item.get("reason_counts", {}).items():
            accumulator.stats.reason_counts[reason] = (
                accumulator.stats.reason_counts.get(reason, 0) + int(count)
            )


def run_spark_job(config: SparkNormalizeConfig, *, spark: Any | None = None) -> dict[str, Any]:
    """Run the distributed normalization and publish a flat v4 output set.

    The returned dictionary is intentionally small and JSON-serializable so a
    DolphinScheduler SHELL/Python wrapper can print it without collecting
    snapshots or trajectories on the driver.
    """

    try:
        from pyspark import SparkConf
        from pyspark.sql import SparkSession
    except ImportError as error:  # pragma: no cover - deployment packaging issue
        raise RuntimeError("Spark mode requires pyspark in the scheduler image") from error

    if spark is None:
        spark_conf = SparkConf().setAppName("trajfoundry-tokenplan-normalizer")
        for key, value in config.spark_conf().items():
            spark_conf.set(key, value)
        spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    context = spark.sparkContext

    shards = context.parallelize(
        list(config.prefix_shards),
        numSlices=min(len(config.prefix_shards), config.default_parallelism),
    )
    raw = shards.mapPartitions(
        lambda part: _read_partition(
            part,
            input_uri=config.input_uri,
            input_format=config.input_format,
            endpoint_url=config.endpoint_url,
            region_name=config.region_name,
        )
    )
    input_counter = context.longAccumulator("trajfoundry_input_objects")
    error_counter = context.longAccumulator("trajfoundry_parse_failures")
    parsed = raw.mapPartitions(
        lambda part: _parse_partition(
            part,
            input_format=config.input_format,
            input_counter=input_counter,
            error_counter=error_counter,
        )
    )
    rows = (
        parsed.filter(lambda item: item[0] == "ok")
        .map(lambda item: item[1])
        .map(lambda value: ((value[2], value[3]), value))
        .groupByKey(numPartitions=config.shuffle_partitions)
    )
    built = rows.mapPartitions(_build_partition)
    published = built.mapPartitionsWithIndex(
        lambda index, part: _publish_partition(
            index,
            part,
            output_uri=config.output_uri,
            endpoint_url=config.endpoint_url,
            region_name=config.region_name,
        )
    )
    results = published.collect()
    error_count = int(error_counter.value)
    input_count = int(input_counter.value)

    accumulator = ManifestAccumulator(
        input_root=config.input_uri,
        input_format=config.input_format,
        config_hash=_config_hash(config),
    )
    accumulator.stats.input_files = input_count
    accumulator.stats.skipped_inputs = error_count
    accumulator.stats.skip_reason_counts["parse_failure"] = error_count
    for item in results:
        for entry in item.get("objects", ()):
            accumulator.add_object(
                SparkObjectDescription(
                    path=entry["path"],
                    sha256=entry["sha256"],
                    bytes=int(entry["bytes"]),
                )
            )
        _merge_counts(accumulator, [item])

    output_location = parse_s3_uri(config.output_uri)
    client = create_s3_client(
        config.endpoint_url or "https://s3.amazonaws.com",
        config.region_name,
        max_pool_connections=16,
    )
    with tempfile.TemporaryDirectory(prefix="trajfoundry-lineage-") as temp_dir:
        part_paths: list[Path] = []
        part_descriptions = sorted(
            (item["lineage_part"] for item in results),
            key=lambda entry: entry["path"],
        )
        for offset, entry in enumerate(part_descriptions):
            path = Path(temp_dir) / f"part-{offset:06d}.jsonl"
            _download_part(client, output_location, entry["path"], path)
            part_paths.append(path)
        merged_path = Path(temp_dir) / "lineage.jsonl"
        merge_lineage_parts(part_paths, merged_path)
        lineage_description = deterministic_s3_put_file(
            client,
            output_location,
            "lineage.jsonl",
            merged_path,
        )
    accumulator.add_object(lineage_description)
    for entry in part_descriptions:
        client.delete_object(Bucket=output_location.bucket, Key=output_location.key(entry["path"]))

    manifest_payload = accumulator.manifest_bytes()
    manifest_description = deterministic_s3_put(
        client,
        output_location,
        "manifest.json",
        manifest_payload,
        content_type="application/json",
    )
    accumulator.add_object(manifest_description)
    _cleanup_stale_jsonl(
        client,
        output_location,
        {description.path for description in accumulator.objects},
    )
    # The manifest describes itself in the existing v4 exporter only when it
    # is in the file list; publish it last but do not recurse while hashing.
    return {
        "input_files": accumulator.stats.input_files,
        "parse_failures": error_count,
        "trajectories": accumulator.stats.accepted
        + accumulator.stats.quarantined_trajectories,
        "accepted": accumulator.stats.accepted,
        "quarantined_trajectories": accumulator.stats.quarantined_trajectories,
        "output_objects": len(accumulator.objects),
        "manifest_sha256": manifest_description.sha256,
    }


__all__ = ["run_spark_job"]
