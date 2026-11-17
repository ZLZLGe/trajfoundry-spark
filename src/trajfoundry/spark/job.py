"""Spark Client/Cluster entrypoint for no-snapshot trajectory normalization."""

from __future__ import annotations

import hashlib
import importlib
import logging
import os
import resource
import socket
import sqlite3
import tempfile
import time
import uuid
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator
from itertools import chain, groupby
from pathlib import Path
from typing import Any

import orjson
from pydantic import ValidationError

from ..feedback import unix_ms
from ..identity import aggregation_scope
from ..json_codec import dumps, loads
from ..models import Snapshot
from ..output_contract import project_quarantine_record
from ..pipeline import NORMALIZER_REVISION, UnsupportedCaptureError, _snapshot_record
from ..providers.anthropic import AnthropicCaptureError
from ..providers.chat import ChatCompletionsAdapterError
from ..providers.responses import ResponsesAdapterError
from ..quality import is_strict_sample
from ..s3 import S3Location, create_s3_client
from ..sources.deepinfra import DeepInfraError
from ..sources.sxf import SXFError
from ..sources.tokenplan import TokenPlanError
from .adapters import SnapshotUnicodeError, parse_payload
from .build import build_scope
from .config import SparkNormalizeConfig, validate_prefix_shards
from .manifest import DiskManifestAccumulator
from .output import (
    ManifestAccumulator,
    SparkObjectDescription,
    lineage_line,
    merge_lineage_parts,
    serialize_trajectory,
)
from .paths import DIAGNOSTICS_DIRECTORY
from .sort import repartition_and_sort
from .source import (
    discover_prefix_shards,
    iter_shard_payloads,
    list_shard_objects,
    read_listed_objects,
)
from .storage import create_output_store

LOGGER = logging.getLogger(__name__)
_MAX_TRAJECTORY_BYTES = 2 * 1024 * 1024 * 1024
_RUNTIME_SOURCE_HASH: str | None = None


def _source_package_hash() -> str:
    """Return a deterministic hash of the imported TrajFoundry Python source.

    A revision string alone cannot detect an executor that is running an old
    checkout with the same revision.  Hashing the package source catches that
    deployment drift without collecting source files or paths on the Driver.
    The result is cached per Python worker process because the handshake can
    use more than one partition.
    """

    global _RUNTIME_SOURCE_HASH
    if _RUNTIME_SOURCE_HASH is not None:
        return _RUNTIME_SOURCE_HASH
    package = importlib.import_module("trajfoundry")
    package_file = getattr(package, "__file__", None)
    if not package_file:
        raise RuntimeError("cannot fingerprint TrajFoundry source: package has no file")
    package_root = Path(package_file).resolve().parent
    digest = hashlib.sha256()
    source_files = sorted(
        path
        for path in package_root.rglob("*.py")
        if path.is_file() and not path.is_symlink()
    )
    if not source_files:
        raise RuntimeError(
            "cannot fingerprint TrajFoundry source: package contains no Python files"
        )
    for path in source_files:
        relative = path.relative_to(package_root).as_posix().encode()
        digest.update(relative)
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    _RUNTIME_SOURCE_HASH = digest.hexdigest()
    return _RUNTIME_SOURCE_HASH


def _runtime_identity() -> dict[str, str]:
    """Return the code identity used by Driver/Executor consistency checks."""

    return {
        "normalizer_revision": NORMALIZER_REVISION,
        "source_hash": _source_package_hash(),
    }


def _executor_runtime_identity(
    _part: Iterable[Any],
    expected_revision: str,
    expected_source_hash: str,
) -> Iterator[dict[str, Any]]:
    """Report executor code identity and fail on any local import mismatch.

    The Driver collects these tiny reports before creating an output store or
    publishing a run marker.  Keep only bounded, non-sensitive diagnostics in
    the report; source paths and credentials must never cross the boundary.
    """

    from pyspark import TaskContext

    actual = _runtime_identity()
    task_context = TaskContext.get()
    yield {
        "revision": actual["normalizer_revision"],
        "source_hash": actual["source_hash"],
        "host": socket.gethostname(),
        "pid": str(os.getpid()),
        "partition": task_context.partitionId() if task_context else -1,
        "expected_revision": expected_revision,
        "expected_source_hash": expected_source_hash,
    }


def _runtime_consistency_handshake(context: Any) -> dict[str, Any]:
    """Verify every sampled executor uses this Driver's source and revision."""

    expected = _runtime_identity()
    # One task per effective default-parallelism slot gives the scheduler a
    # chance to place the probe on all active executors while keeping the
    # result bounded.  Spark does not expose a reliable executor enumeration
    # API; a task that runs on a stale executor is sufficient to fail fast.
    partitions = max(1, int(context.defaultParallelism))
    reports = (
        context.parallelize(range(partitions), partitions)
        .mapPartitions(
            lambda part: _executor_runtime_identity(
                part,
                expected["normalizer_revision"],
                expected["source_hash"],
            )
        )
        .collect()
    )
    if not reports:
        raise RuntimeError(
            "Spark code consistency handshake produced no executor reports"
        )
    mismatches = [
        report
        for report in reports
        if report["revision"] != expected["normalizer_revision"]
        or report["source_hash"] != expected["source_hash"]
    ]
    if mismatches:
        details = [
            {
                "host": report.get("host", "unknown"),
                "pid": report.get("pid", "unknown"),
                "partition": report.get("partition", -1),
                "revision": report.get("revision", "unknown"),
                "source_hash": report.get("source_hash", "unknown"),
            }
            for report in mismatches[:8]
        ]
        raise RuntimeError(
            "Spark code consistency handshake failed: "
            f"driver_revision={expected['normalizer_revision']!r} "
            f"driver_source_hash={expected['source_hash']} "
            f"mismatches={details!r}"
        )
    hosts = sorted({str(report.get("host", "unknown")) for report in reports})
    LOGGER.info(
        "Spark code consistency handshake passed: executors=%d hosts=%d revision=%s source_hash=%s",
        len(reports),
        len(hosts),
        expected["normalizer_revision"],
        expected["source_hash"],
    )
    return {
        "normalizer_revision": expected["normalizer_revision"],
        "source_hash": expected["source_hash"],
        "executor_reports": len(reports),
        "executor_hosts": hosts,
    }


class _InvalidCaptureRoot(ValueError):
    pass


def _read_partition(
    shards: Iterable[str],
    *,
    input_uri: str,
    input_format: str,
    endpoint_url: str | None,
    region_name: str,
    input_credentials_path: str | None = None,
) -> Iterator[tuple[str, bytes, str]]:
    for shard in shards:
        yield from iter_shard_payloads(
            input_uri,
            shard,
            input_format=input_format,
            endpoint_url=endpoint_url,
            region_name=region_name,
            input_credentials_path=input_credentials_path,
        )


def _parse_partition(
    records: Iterable[tuple[str, bytes, str]],
    *,
    input_format: str,
) -> Iterator[tuple[str, Any]]:
    from ..sources.boyue import BoyueError, NonTrajectoryCapture

    for source_ref, payload, _listed_digest in records:
        try:
            # Distinguish an invalid input root from an internal parser TypeError.
            if not payload.lstrip().startswith(b"{"):
                loads(payload)
                raise _InvalidCaptureRoot("capture root must be an object")
            row = parse_payload(source_ref, payload, input_format=input_format)  # type: ignore[arg-type]
        except NonTrajectoryCapture:
            yield (
                "error",
                {
                    "source_ref": source_ref,
                    "sha256": hashlib.sha256(payload).hexdigest(),
                    "reason": "non_trajectory_endpoint",
                    "error_type": "NonTrajectoryCapture",
                },
            )
        except (
            BoyueError,
            TokenPlanError,
            SXFError,
            DeepInfraError,
            UnsupportedCaptureError,
            AnthropicCaptureError,
            ChatCompletionsAdapterError,
            ResponsesAdapterError,
            SnapshotUnicodeError,
            orjson.JSONDecodeError,
            ValidationError,
            _InvalidCaptureRoot,
        ) as error:
            # These are input/schema failures, not arbitrary programming errors.
            # Exception messages and payloads must not escape into diagnostics.
            yield (
                "error",
                {
                    "source_ref": source_ref,
                    "sha256": hashlib.sha256(payload).hexdigest(),
                    "reason": "parse_failure",
                    "error_type": type(error).__name__,
                },
            )
        else:
            yield "ok", row.as_tuple()


def _key_parsed(item: tuple[str, Any]) -> tuple[tuple[str, str], tuple[str, Any]]:
    kind, value = item
    if kind == "error":
        return ("parse_failure", value["source_ref"]), item
    return (value[2], value[3]), item


def _component_summary(item):
    from .component_plan import summarize_snapshot

    value = item[1]
    if len(value[3].encode()) > 64 * 1024:
        raise ValueError("aggregation identity exceeds byte budget")
    return (value[2], value[3]), summarize_snapshot(
        Snapshot.model_validate_json(value[-1])
    )


def _component_sort_key(item, summaries):
    from .component_plan import component_key

    scope, _ = _key_parsed(item)
    if item[0] == "error":
        return (*scope, "whole", "", item[1]["source_ref"]), item
    value = item[1]
    snapshot = Snapshot.model_validate_json(value[-1])
    component = component_key(snapshot, summaries[scope])
    return (*scope, component, value[7], value[0]), item


def _scope_byte_summary(item):
    from .byte_plan import record_order_key

    scope, summary = _component_summary(item)
    value = item[1]
    return scope, (
        summary,
        len(value[-1]),
        record_order_key(value[7], value[0], value[1]),
    )


def _merge_byte_summaries(left, right):
    from .component_plan import merge_summaries

    return (
        merge_summaries(left[0], right[0]),
        left[1] + right[1],
        min(left[2], right[2]),
    )


def _byte_metadata(item):
    from .byte_plan import ByteRecordMetadata

    scope, (summary, size, order_key) = _scope_byte_summary(item)
    return (scope, order_key), ByteRecordMetadata(scope, order_key, size, summary)


def _metadata_partition(key):
    from pyspark.rdd import portable_hash

    return portable_hash(key[0])


def _planned_component_sort_key(item, chunk_indexes, summaries):
    """Route cached bodies using sparse metadata boundaries, without re-encoding."""
    from .byte_plan import find_chunk, record_order_key
    from .component_plan import component_key

    scope, _ = _key_parsed(item)
    if item[0] == "error":
        return (*scope, "whole", (1, "", item[1]["source_ref"], "")), item
    value = item[1]
    order_key = record_order_key(value[7], value[0], value[1])
    if scope[0] == "user":
        chunk = find_chunk(chunk_indexes[scope], order_key)
        scope = aggregation_scope("", scope[1], "", chunk.chunk_id)
        # Keep the existing tuple contract. The snapshot chunk marker is added
        # only at the builder boundary, avoiding a full JSON re-serialization.
        value = (*value[:2], *scope, *value[4:])
        item = "ok", value
    snapshot = Snapshot.model_validate_json(value[-1])
    component = component_key(snapshot, summaries[scope])
    return (*scope, component, order_key), item


def _planned_partition(key, assignments):
    partition = assignments.get(key[:2])
    return _scope_partition(key) if partition is None else partition


def _admit_scope_plan(count, size, config):
    if count > config.max_scope_summaries:
        raise RuntimeError("scope summary count limit exceeded; no manifest published")
    if size > config.max_scope_summary_bytes:
        raise RuntimeError("scope summary byte limit exceeded; no manifest published")


def _sum_pair(left, right):
    return left[0] + right[0], left[1] + right[1]


def _chunk_plan_size(chunk):
    # Bound the broadcast plan before collecting boundaries onto the Driver.
    return len(dumps([chunk.scope, chunk.first_key, chunk.chunk_id])) + 512


def _scope_partition(key):
    from pyspark.rdd import portable_hash

    return portable_hash(key[:3])


def _build_partition(
    rows: Iterable[tuple[tuple[str, str], Iterable[tuple[str, Any]]]],
    *,
    scope_memory_bytes: int = 64 * 1024 * 1024,
    max_scope_snapshots: int = 1000,
    bounded_limits=None,
) -> Iterator[tuple[str, Any]]:
    from .component_plan import _has_routing_evidence

    for scope, values in rows:
        if scope[0] == "parse_failure":
            for _kind, value in values:
                # This is a skipped archive, counted by skip_reason_counts.
                # excluded_records is reserved for parsed quarantine records.
                yield "counts", {"input_files": 1, "skipped_inputs": 1}
                yield "diagnostic", value
            continue
        # Keep only a bounded serialized probe, not an unbounded Snapshot list.
        # Diagnostics are also spooled, since a disk builder consumes all inputs
        # before yielding its first trajectory.
        with tempfile.SpooledTemporaryFile(max_size=1024 * 1024) as diagnostics:
            count = 0
            iterator = iter(values)
            probe = []
            probe_bytes = 0
            spilled = False
            for item in iterator:
                probe.append(item)
                probe_bytes += len(item[1][-1])
                if (
                    len(probe) >= max_scope_snapshots
                    or probe_bytes >= scope_memory_bytes
                    # Small routing groups can still produce a large diagnostic
                    # graph. Always use the disk path's derived-graph budgets.
                    or _has_routing_evidence(Snapshot.model_validate_json(item[1][-1]))
                ):
                    spilled = True
                    break

            chunk_id = loads(scope[1])[1] if scope[0] == "user_chunk" else ""

            def snapshots(probe=probe, iterator=iterator, chunk_id=chunk_id):
                nonlocal count
                for _kind, value in chain(probe, iterator):
                    snapshot = Snapshot.model_validate_json(value[-1])
                    snapshot.sessionless_chunk_id = chunk_id
                    count += 1
                    record = _snapshot_record(snapshot)
                    if record is not None:
                        diagnostics.write(
                            dumps(project_quarantine_record(record)) + b"\n"
                        )
                    yield snapshot

            if spilled:
                from .bounded import build_scope_bounded

                LOGGER.info(
                    "Spark scope spilled to disk: buffered_records=%d buffered_bytes=%d",
                    len(probe),
                    probe_bytes,
                )
                trajectories = build_scope_bounded(snapshots(), limits=bounded_limits)
            else:
                trajectories = build_scope(snapshots())
            for built in trajectories:
                yield "trajectory", built
            diagnostics.seek(0)
            for line in diagnostics:
                yield "record", loads(line)
            yield "counts", {"input_files": count}
            yield (
                "build_metrics",
                {
                    "disk_scopes": int(spilled),
                    "memory_scopes": int(not spilled),
                    "largest_component_records": count,
                },
            )


def _build_sorted_partition(
    rows, *, scope_memory_bytes, max_scope_snapshots, bounded_limits=None
):
    # Spark's external sort spills. Unlike groupByKey, no Python list of all
    # values belonging to a hot key is needed at the reduce-side boundary.
    groups = (
        (scope, (item for _key, item in values))
        for scope, values in groupby(rows, key=lambda row: row[0][:3])
    )
    yield from _build_partition(
        groups,
        scope_memory_bytes=scope_memory_bytes,
        max_scope_snapshots=max_scope_snapshots,
        bounded_limits=bounded_limits,
    )


def _publish_partition(
    partition_id: int,
    artifacts: Iterable[tuple[str, Any]],
    *,
    output_uri: str,
    endpoint_url: str | None,
    region_name: str,
    max_objects: int = 10_000,
    client_factory: Callable[..., Any] = create_s3_client,
    run_id: str | None = None,
    output_credentials_path: str | None = None,
    retain_s3_auxiliary: bool = False,
    output_owner: str | None = None,
) -> Iterator[dict[str, Any]]:
    if retain_s3_auxiliary and (
        not isinstance(run_id, str)
        or len(run_id) != 32
        or any(char not in "0123456789abcdef" for char in run_id)
    ):
        raise ValueError(
            "retained S3 parts require a 32-character lowercase hex run_id"
        )
    store = create_output_store(
        output_uri,
        endpoint_url,
        region_name,
        client_factory,
        run_id=run_id,
        output_credentials_path=output_credentials_path,
        retain_s3_auxiliary=retain_s3_auxiliary,
        output_owner=output_owner,
    )
    output_objects = 0
    started = time.monotonic()
    build_metrics = {
        "disk_scopes": 0,
        "memory_scopes": 0,
        "largest_component_records": 0,
    }
    accepted = quarantined = duplicates = 0
    reason_counts: dict[str, int] = defaultdict(int)
    counts: dict[str, int] = defaultdict(int)
    # Sort only serialized lineage/diagnostics on local disk, never all full
    # trajectories in this partition. The DB is ephemeral task metadata.
    with tempfile.TemporaryDirectory(
        prefix=f"trajfoundry-publish-{partition_id}-"
    ) as work:
        database = sqlite3.connect(str(Path(work) / "metadata.sqlite"))
        database.execute("PRAGMA temp_store=FILE")
        database.execute("PRAGMA cache_size=-2048")
        database.execute(
            "CREATE TABLE lines (kind TEXT, key TEXT, line BLOB, PRIMARY KEY(kind,key))"
        )
        try:
            for kind, built in artifacts:
                if kind == "build_metrics":
                    build_metrics["disk_scopes"] += built["disk_scopes"]
                    build_metrics["memory_scopes"] += built["memory_scopes"]
                    build_metrics["largest_component_records"] = max(
                        build_metrics["largest_component_records"],
                        built["largest_component_records"],
                    )
                    continue
                if kind == "counts":
                    for name, value in built.items():
                        counts[name] += value
                    continue
                if kind in {"diagnostic", "record"}:
                    if kind == "diagnostic":
                        reason_counts[built["reason"]] += 1
                    if kind == "record":
                        counts["quarantined_records"] += 1
                        audit = built["normalization_audit"]
                        counts["excluded_records"] += int(audit["tag"] == "excluded")
                        for reason in audit["reason_codes"]:
                            reason_counts[reason] += 1
                    # S3 keeps summary counts only. Diagnostic details remain
                    # available for filesystem output, without S3 sidecars.
                    if store.local_path is None:
                        continue
                    database.execute(
                        "INSERT INTO lines VALUES (?,?,?)",
                        (
                            "diagnostic",
                            built["source_ref"],
                            dumps(built, sort_keys=True) + b"\n",
                        ),
                    )
                    continue
                if kind != "trajectory":
                    raise ValueError("unknown build event")
                if output_objects >= max_objects:
                    raise RuntimeError(
                        "partition output descriptor limit exceeded; no manifest published"
                    )
                artifact = serialize_trajectory(
                    built.node,
                    built.origins,
                    max_bytes=_MAX_TRAJECTORY_BYTES,
                )
                description = store.put_bytes(artifact.filename, artifact.payload)
                # Verify the actual stored bytes before an Executor can emit
                # a successful receipt. This distributes S3 read-back across
                # build tasks instead of serially downloading on the Driver.
                store.verify(description)
                database.execute(
                    "INSERT INTO lines VALUES (?,?,?)",
                    (
                        "objects",
                        artifact.trajectory_id,
                        dumps(
                            {
                                **description.manifest_entry(),
                                "trajectory_id": artifact.trajectory_id,
                            }
                        )
                        + b"\n",
                    ),
                )
                output_objects += 1
                database.execute(
                    "INSERT INTO lines VALUES (?,?,?)",
                    (
                        "lineage",
                        artifact.trajectory_id,
                        lineage_line(artifact),
                    ),
                )
                if is_strict_sample(built.node):
                    accepted += 1
                else:
                    quarantined += 1
                    if built.node.normalization_audit:
                        for reason in built.node.normalization_audit.reason_codes:
                            reason_counts[reason] += 1
                duplicates += max(0, len(built.origins) - 1)
            database.commit()
            parts = {}
            for kind in ("lineage", "diagnostic", "objects"):
                if kind == "diagnostic" and store.local_path is None:
                    continue
                identity = f"{run_id}_" if retain_s3_auxiliary else ""
                part_basename = f"._{kind}_part_{identity}{partition_id:06d}.ndjson"
                part_name = f"{DIAGNOSTICS_DIRECTORY}/{part_basename}"
                path = Path(work) / part_basename
                with path.open("wb") as handle:
                    for (line,) in database.execute(
                        "SELECT line FROM lines WHERE kind=? ORDER BY key", (kind,)
                    ):
                        handle.write(line)
                if path.stat().st_size:
                    parts[f"{kind}_part"] = store.put_file(
                        part_name, path
                    ).manifest_entry()
        finally:
            database.close()
    yield {
        "partition_id": partition_id,
        "object_count": output_objects,
        **parts,
        **counts,
        "accepted": accepted,
        "quarantined": quarantined,
        "duplicates": duplicates,
        "reason_counts": dict(reason_counts),
        "build_metrics": build_metrics,
        "worker_peak_rss_mib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        / 1024,
        "partition_seconds": round(time.monotonic() - started, 3),
    }


def _config_hash(config: SparkNormalizeConfig) -> str:
    payload = {
        "input_uri": config.input_uri,
        "output_uri": config.output_uri,
        "input_format": config.input_format,
        "prefix_shards": list(config.prefix_shards),
        "auto_prefix_shards": config.auto_prefix_shards,
        "max_prefix_shards": config.max_prefix_shards,
        "normalizer_revision": NORMALIZER_REVISION,
        "spark_conf": config.spark_conf(),
        "spark_execution_revision": "bounded-components-v6-single-record-spill",
        "retain_s3_auxiliary": config.retain_s3_auxiliary,
        "diagnostic_details": "filesystem_only",
        "max_output_objects": config.max_output_objects,
        "max_partition_objects": config.max_partition_objects,
        "scope_memory_bytes": config.scope_memory_bytes,
        "max_scope_snapshots": config.max_scope_snapshots,
        "max_scope_summaries": config.max_scope_summaries,
        "max_scope_summary_bytes": config.max_scope_summary_bytes,
        "max_input_object_bytes": config.max_input_object_bytes,
        "max_root_bytes": config.max_root_bytes,
        "max_trajectory_bytes": _MAX_TRAJECTORY_BYTES,
        "max_routing_bytes": config.max_routing_bytes,
        "max_routing_snapshots": config.max_routing_snapshots,
    }
    return hashlib.sha256(dumps(payload, sort_keys=True)).hexdigest()


def _download_part(
    client: Any, location: S3Location, relative_path: str, destination: Path
) -> None:
    response = client.get_object(
        Bucket=location.bucket, Key=location.key(relative_path)
    )
    body = response["Body"]
    try:
        with destination.open("wb") as handle:
            while chunk := body.read(8 * 1024 * 1024):
                handle.write(chunk)
    finally:
        close = getattr(body, "close", None)
        if callable(close):
            close()


def _ensure_empty_output_prefix(client: Any, location: S3Location) -> None:
    """Refuse replacement: a failed run must not invalidate an older manifest.

    This preflight is not a distributed lock. The caller must allocate a fresh
    prefix exclusively to this run, including against concurrent writers.
    """
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=location.bucket, Prefix=location.prefix):
        if page.get("Contents"):
            raise ValueError("output prefix is not empty; use a fresh exclusive prefix")


def _merge_counts(
    accumulator: ManifestAccumulator | DiskManifestAccumulator,
    results: list[dict[str, Any]],
) -> None:
    for name in (
        "input_files",
        "skipped_inputs",
        "quarantined_records",
        "excluded_records",
    ):
        setattr(
            accumulator.stats,
            name,
            getattr(accumulator.stats, name)
            + sum(int(item.get(name, 0)) for item in results),
        )
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


def _resolve_prefix_shards(
    context: Any, config: SparkNormalizeConfig
) -> tuple[str, ...]:
    """Resolve explicit or executor-discovered input shards."""

    if not config.auto_prefix_shards:
        return tuple(config.prefix_shards)
    # Credentials are deliberately loaded in the executor-side discovery
    # closure, just like ordinary shard reads. Only a bounded list of
    # immediate user prefixes crosses back to the Driver; object keys and
    # payloads never do.
    discovered = (
        context.parallelize([None], 1)
        .mapPartitions(
            lambda _part: discover_prefix_shards(
                config.input_uri,
                input_format=config.input_format,
                endpoint_url=config.endpoint_url,
                region_name=config.region_name,
                input_credentials_path=config.input_credentials_path,
                max_prefix_shards=config.max_prefix_shards,
            )
        )
        .collect()
    )
    shards_value = tuple(discovered)
    validate_prefix_shards(shards_value)
    LOGGER.info(
        "Spark automatic input prefix discovery: prefixes=%d",
        len(shards_value),
    )
    return shards_value


def _run_spark_job(
    config: SparkNormalizeConfig,
    *,
    accumulator: DiskManifestAccumulator,
    driver_work: Path,
    cleanup: list,
    spark: Any | None = None,
    source_reader: Callable[..., Any] = _read_partition,
    client_factory: Callable[..., Any] = create_s3_client,
) -> dict[str, Any]:
    """Run the distributed normalization and publish a flat v4 output set.

    The returned dictionary is intentionally small and JSON-serializable so a
    DolphinScheduler SHELL/Python wrapper can print it without collecting
    snapshots or trajectories on the driver. Output inventories are streamed
    into SQLite; only bounded scope summaries and partition receipts reside
    in Driver memory. Explicit admission limits guard oversized units.
    Reader/client injection supports hermetic local Spark integration tests.
    """

    started = time.monotonic()
    try:
        from pyspark import SparkConf
        from pyspark.sql import SparkSession
    except ImportError as error:  # pragma: no cover - deployment packaging issue
        raise RuntimeError(
            "Spark mode requires pyspark in the scheduler image"
        ) from error

    if spark is None:
        spark_conf = SparkConf().setAppName("trajfoundry-tokenplan-normalizer")
        for key, value in config.spark_conf().items():
            spark_conf.set(key, value)
        spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    context = spark.sparkContext
    # Run before the output store creates a run marker.  A Driver mounted with
    # one checkout and Executors mounted with another must fail before any
    # S3/filesystem writes, rather than hours into materialization.
    runtime_consistency = _runtime_consistency_handshake(context)
    from .progress import SparkProgressReporter

    reporter = SparkProgressReporter(context)
    reporter.start()
    cleanup.append(reporter)
    store = create_output_store(
        config.output_uri,
        config.endpoint_url,
        config.region_name,
        client_factory,
        output_credentials_path=config.output_credentials_path,
        retain_s3_auxiliary=config.retain_s3_auxiliary,
        allow_existing_output=config.allow_existing_output,
        output_owner=config.output_owner,
        resume_owned_output=config.resume_owned_output,
    )
    run_id = uuid.uuid4().hex
    store.initialize_run(run_id)
    parallelism = config.default_parallelism or context.defaultParallelism
    shuffle_partitions = config.shuffle_partitions or int(
        context.getConf().get(
            "spark.sql.shuffle.partitions", str(context.defaultParallelism)
        )
    )

    shards_value = _resolve_prefix_shards(context, config)
    shards = context.parallelize(
        list(shards_value),
        numSlices=max(1, min(len(shards_value), parallelism)),
    )
    if source_reader is _read_partition:
        # Shuffle only object references first: one huge S3 user prefix must
        # not force all its GET/decode/parse work into a single reader task.
        listed = shards.mapPartitions(
            lambda part: list_shard_objects(
                part,
                input_uri=config.input_uri,
                input_format=config.input_format,
                endpoint_url=config.endpoint_url,
                region_name=config.region_name,
                input_credentials_path=config.input_credentials_path,
            )
        )
        if config.input_inventory_sha256:
            from pyspark import StorageLevel

            from .source import inventory_fingerprint, inventory_item, merge_inventory

            listed.persist(StorageLevel.DISK_ONLY)
            cleanup.append(listed)
            inventory = listed.map(inventory_item).fold((0, 0, 0), merge_inventory)
            if inventory_fingerprint(inventory) != config.input_inventory_sha256:
                raise RuntimeError(
                    "input inventory changed before read; no manifest published"
                )
        raw = listed.repartition(parallelism).mapPartitions(
            lambda part: read_listed_objects(
                part,
                input_uri=config.input_uri,
                input_format=config.input_format,
                endpoint_url=config.endpoint_url,
                region_name=config.region_name,
                input_credentials_path=config.input_credentials_path,
                max_object_bytes=config.max_input_object_bytes,
            )
        )
    else:
        raw = shards.mapPartitions(
            lambda part: source_reader(
                part,
                input_uri=config.input_uri,
                input_format=config.input_format,
                endpoint_url=config.endpoint_url,
                region_name=config.region_name,
                input_credentials_path=config.input_credentials_path,
            )
        )
    parsed = raw.mapPartitions(
        lambda part: _parse_partition(
            part,
            input_format=config.input_format,
        )
    )
    # Counts/failures travel through the shuffle as data. Accumulators in a
    # transformation are not exact if Spark recomputes a task or stage.
    from pyspark import StorageLevel

    from .bounded import BoundedBuildLimits
    from .byte_plan import (
        ByteChunk,
        allocate_chunk_count,
        assign_scope_partitions,
        index_chunks,
        make_chunk_id,
        plan_byte_chunks,
        target_chunk_bytes,
    )

    # Reuse parse work without pinning capture histories in executor memory.
    parsed.persist(StorageLevel.DISK_ONLY)
    cleanup.append(parsed)
    summary_rdd = (
        parsed.filter(lambda item: item[0] == "ok")
        .map(_scope_byte_summary)
        .reduceByKey(_merge_byte_summaries, shuffle_partitions)
        .persist(StorageLevel.DISK_ONLY)
    )
    cleanup.append(summary_rdd)
    reporter.set_phase("read_parse_plan")
    summary_count, summary_bytes = summary_rdd.map(
        lambda item: (1, len(dumps([item[0], item[1][2]])) + 512)
    ).fold((0, 0), _sum_pair)
    _admit_scope_plan(summary_count, summary_bytes, config)
    # Admission is checked before collection, including long user identities.
    summary_rows = summary_rdd.collect()
    largest_scope_records = max((item[0].count for _, item in summary_rows), default=0)
    sessionless_input_bytes = sum(
        item[1] for scope, item in summary_rows if scope[0] == "user"
    )
    target_bytes = {}
    chunks = []
    final_summaries = {}
    weights = {}
    for scope, (summary, size, first_key) in summary_rows:
        if scope[0] != "user":
            final_summaries[scope] = summary
            weights[scope] = size
        elif (
            allocate_chunk_count(
                size, summary.count, sessionless_input_bytes, shuffle_partitions
            )
            == 1
        ):
            # Small users need no metadata sort: their minimum request key was
            # reduced with the constant-size byte summary above.
            chunks.append(
                ByteChunk(
                    scope, first_key, make_chunk_id(scope, first_key), size, summary
                )
            )
        else:
            target_bytes[scope] = target_chunk_bytes(
                size, summary.count, sessionless_input_bytes, shuffle_partitions
            )
    byte_plan_started = time.monotonic()
    if target_bytes:
        reporter.set_phase("byte_chunk_plan")
        targets = context.broadcast(target_bytes)
        cleanup.append(targets)
        chunk_rdd = (
            parsed.filter(
                lambda item: (
                    item[0] == "ok" and (item[1][2], item[1][3]) in targets.value
                )
            )
            .map(_byte_metadata)
            .repartitionAndSortWithinPartitions(
                numPartitions=shuffle_partitions, partitionFunc=_metadata_partition
            )
            .mapPartitions(
                lambda part: plan_byte_chunks(
                    (value for _key, value in part), targets.value
                )
            )
            .persist(StorageLevel.DISK_ONLY)
        )
        cleanup.append(chunk_rdd)
        chunk_count, chunk_bytes = chunk_rdd.map(
            lambda chunk: (1, _chunk_plan_size(chunk))
        ).fold((0, 0), _sum_pair)
        # Include base summaries and already planned single-chunk users in the
        # same admission budget. No per-request metadata reaches the Driver.
        _admit_scope_plan(
            len(final_summaries) + len(chunks) + chunk_count,
            summary_bytes
            + sum(_chunk_plan_size(chunk) for chunk in chunks)
            + chunk_bytes,
            config,
        )
        chunks.extend(chunk_rdd.collect())
    by_user = defaultdict(list)
    for chunk in chunks:
        by_user[chunk.scope].append(chunk)
        scope = aggregation_scope("", chunk.scope[1], "", chunk.chunk_id)
        final_summaries[scope] = chunk.summary
        weights[scope] = chunk.total_bytes
    _admit_scope_plan(
        len(final_summaries),
        summary_bytes + sum(_chunk_plan_size(chunk) for chunk in chunks),
        config,
    )
    largest_chunk_records = max((chunk.summary.count for chunk in chunks), default=0)
    largest_chunk_bytes = max((chunk.total_bytes for chunk in chunks), default=0)
    sessionless_chunks = len(chunks)
    byte_plan_seconds = time.monotonic() - byte_plan_started
    planning_seconds = time.monotonic() - started
    splittable_scopes = sum(
        not item.unsafe and bool(item.min_length) for item in final_summaries.values()
    )
    summaries = context.broadcast(final_summaries)
    chunk_indexes = context.broadcast(
        {scope: index_chunks(items) for scope, items in by_user.items()}
    )
    assignments = context.broadcast(
        assign_scope_partitions(weights, shuffle_partitions)
    )
    cleanup.extend([summaries, chunk_indexes, assignments])
    LOGGER.info(
        "Spark byte/component plan: input_scopes=%d build_scopes=%d splittable=%d "
        "sessionless_bytes=%d sessionless_chunks=%d largest_chunk_records=%d "
        "largest_chunk_bytes=%d partitions=%d byte_plan_seconds=%.1f",
        len(summary_rows),
        len(final_summaries),
        splittable_scopes,
        sessionless_input_bytes,
        sessionless_chunks,
        largest_chunk_records,
        largest_chunk_bytes,
        shuffle_partitions,
        byte_plan_seconds,
    )
    del summary_rows, chunks, by_user, final_summaries, weights, target_bytes
    rows = repartition_and_sort(
        parsed.map(
            lambda item: _planned_component_sort_key(
                item, chunk_indexes.value, summaries.value
            )
        ),
        num_partitions=shuffle_partitions,
        partition_func=lambda key: _planned_partition(key, assignments.value),
    )
    built = rows.mapPartitions(
        lambda part: _build_sorted_partition(
            part,
            scope_memory_bytes=config.scope_memory_bytes,
            max_scope_snapshots=config.max_scope_snapshots,
            bounded_limits=BoundedBuildLimits(
                max_routing_bytes=config.max_routing_bytes,
                max_routing_snapshots=config.max_routing_snapshots,
                max_root_bytes=config.max_root_bytes,
            ),
        )
    )
    published = built.mapPartitionsWithIndex(
        lambda index, part: _publish_partition(
            index,
            part,
            output_uri=config.output_uri,
            endpoint_url=config.endpoint_url,
            region_name=config.region_name,
            max_objects=config.max_partition_objects,
            client_factory=client_factory,
            run_id=run_id,
            output_credentials_path=config.output_credentials_path,
            retain_s3_auxiliary=config.retain_s3_auxiliary,
            output_owner=config.output_owner,
        )
    ).persist(StorageLevel.DISK_ONLY)
    cleanup.append(published)
    # toLocalIterator alone schedules one result partition at a time. Force a
    # distributed action first, caching only small partition receipts, so all
    # build/upload tasks can actually use the requested executor cores.
    reporter.set_phase("build_publish")
    published_partitions = published.count()
    LOGGER.info(
        "Spark distributed build/publish complete: partitions=%d", published_partitions
    )
    results = []
    output_objects = 0
    reporter.set_phase("merge_inventory")
    for item in published.toLocalIterator(prefetchPartitions=False):
        output_objects += item["object_count"]
        if (
            config.max_output_objects is not None
            and output_objects > config.max_output_objects
        ):
            raise RuntimeError(
                "driver output descriptor limit exceeded; no manifest published"
            )
        part = item.get("objects_part")
        if part is not None:
            descriptor_path = driver_work / f"objects-{item['partition_id']:06d}.ndjson"
            copied = store.copy_to(part["path"], descriptor_path)
            if copied.sha256 != part["sha256"] or copied.bytes != part["bytes"]:
                raise ValueError(
                    "output inventory hash or size mismatch; no manifest published"
                )
            read_objects = 0
            with descriptor_path.open("rb") as handle:
                for line in handle:
                    entry = loads(line)
                    description = SparkObjectDescription(
                        path=entry["path"],
                        sha256=entry["sha256"],
                        bytes=int(entry["bytes"]),
                    )
                    # S3 objects were read-back verified in the publishing
                    # task before the immutable inventory receipt was written.
                    # Keep the local recheck for shared-filesystem tampering.
                    if store.local_path is not None:
                        store.verify(description)
                    accumulator.add_object(description, entry["trajectory_id"])
                    read_objects += 1
            if read_objects != item["object_count"]:
                raise ValueError(
                    "output inventory count mismatch; no manifest published"
                )
        elif item["object_count"]:
            raise ValueError("missing output inventory; no manifest published")
        _merge_counts(accumulator, [item])
        # One small result per partition, never a list of its output descriptors.
        results.append(item)
    error_count = accumulator.stats.reason_counts.get("parse_failure", 0)
    for reason in ("parse_failure", "non_trajectory_endpoint"):
        count = accumulator.stats.reason_counts.get(reason, 0)
        if count:
            accumulator.stats.skip_reason_counts[reason] = count

    reporter.set_phase("merge_sidecars")
    with tempfile.TemporaryDirectory(prefix="trajfoundry-lineage-") as temp_dir:
        part_descriptions = [
            item["objects_part"] for item in results if "objects_part" in item
        ]
        diagnostic_description = None
        for kind, destination in (
            ("lineage", "lineage.jsonl"),
            ("diagnostic", f"{DIAGNOSTICS_DIRECTORY}/input_diagnostics.ndjson"),
        ):
            if kind == "diagnostic" and store.local_path is None:
                continue
            entries = sorted(
                (item[f"{kind}_part"] for item in results if f"{kind}_part" in item),
                key=lambda entry: entry["path"],
            )
            if kind == "diagnostic" and not entries:
                continue
            part_paths = []
            for offset, entry in enumerate(entries):
                path = Path(temp_dir) / f"{kind}-{offset:06d}.ndjson"
                copied = store.copy_to(entry["path"], path)
                if copied.sha256 != entry["sha256"] or copied.bytes != entry["bytes"]:
                    raise ValueError(
                        "output part hash or size mismatch; no manifest published"
                    )
                part_paths.append(path)
            merged_path = Path(temp_dir) / Path(destination).name
            merge_lineage_parts(part_paths, merged_path)
            description = store.put_file(destination, merged_path)
            store.verify(description)
            if kind == "lineage":
                accumulator.add_object(description)
            else:
                # v4 only permits trajectory/lineage files. This audit sidecar
                # is returned explicitly, not misrepresented as a trajectory.
                diagnostic_description = description.manifest_entry()
            part_descriptions.extend(entries)

    # Every referenced part was hash/size checked above. Retention is an
    # explicit S3 mode for accounts without DeleteObject, not a swallowed
    # cleanup failure: immutable run-scoped auxiliaries are not manifest files.
    retained_auxiliary_objects = 0
    if config.retain_s3_auxiliary:
        retained_auxiliary_objects = len(part_descriptions)
        LOGGER.info(
            "Spark auxiliary retention: mode=retain scope=current_run objects=%d",
            retained_auxiliary_objects,
        )
    else:
        # Preserve the default fail-closed cleanup contract.
        for entry in part_descriptions:
            store.delete(entry["path"])
    reporter.set_phase("publish_manifest")
    if config.input_inventory_sha256:
        from .source import inventory_fingerprint, inventory_item, merge_inventory

        # Resolve prefixes again, so newly created date-root/model/minute
        # directories cannot escape the completion check.
        final_shards = _resolve_prefix_shards(context, config)
        final_inventory = (
            context.parallelize(
                list(final_shards), max(1, min(len(final_shards), parallelism))
            )
            .mapPartitions(
                lambda part: list_shard_objects(
                    part,
                    input_uri=config.input_uri,
                    input_format=config.input_format,
                    endpoint_url=config.endpoint_url,
                    region_name=config.region_name,
                    input_credentials_path=config.input_credentials_path,
                )
            )
            .map(inventory_item)
            .fold((0, 0, 0), merge_inventory)
        )
        if inventory_fingerprint(final_inventory) != config.input_inventory_sha256:
            raise RuntimeError(
                "input inventory changed during processing; no manifest published"
            )
    manifest_path = driver_work / "manifest.json"
    accumulator.write_manifest(manifest_path)
    manifest_description = store.put_file(
        "manifest.json",
        manifest_path,
        content_type="application/json",
    )
    accumulator.add_object(manifest_description)
    store.finish_run()
    # Manifest is not recursively included in its own file list.
    return {
        "output_uri": config.output_uri,
        "output_backend": "filesystem" if store.local_path is not None else "s3",
        "input_files": accumulator.stats.input_files,
        "skipped_inputs": accumulator.stats.skipped_inputs,
        "parse_failures": error_count,
        "trajectories": accumulator.stats.accepted
        + accumulator.stats.quarantined_trajectories,
        "accepted": accumulator.stats.accepted,
        "quarantined_trajectories": accumulator.stats.quarantined_trajectories,
        "quarantined_records": accumulator.stats.quarantined_records,
        "output_objects": accumulator.object_count,
        "verified_trajectory_objects": output_objects,
        "retained_auxiliary_objects": retained_auxiliary_objects,
        "auxiliary_retention_mode": "retain"
        if config.retain_s3_auxiliary
        else "delete",
        "retention_scope": "current_run",
        "manifest_sha256": manifest_description.sha256,
        "diagnostics": diagnostic_description,
        "runtime_consistency": runtime_consistency,
        "shuffle_partitions": shuffle_partitions,
        "performance": {
            "total_seconds": round(time.monotonic() - started, 3),
            "read_parse_plan_seconds": round(planning_seconds, 3),
            "byte_plan_seconds": round(byte_plan_seconds, 3),
            "sessionless_partitioning": "normalized-bytes-v1",
            "sessionless_input_bytes": sessionless_input_bytes,
            "sessionless_chunks": sessionless_chunks,
            "largest_chunk_records": largest_chunk_records,
            "largest_chunk_bytes": largest_chunk_bytes,
            "largest_scope_records": largest_scope_records,
            "splittable_scopes": splittable_scopes,
            "disk_scopes": sum(
                item["build_metrics"]["disk_scopes"] for item in results
            ),
            "memory_scopes": sum(
                item["build_metrics"]["memory_scopes"] for item in results
            ),
            "largest_component_records": max(
                (
                    item["build_metrics"]["largest_component_records"]
                    for item in results
                ),
                default=0,
            ),
            "worker_peak_rss_mib": max(
                (item["worker_peak_rss_mib"] for item in results), default=0
            ),
        },
    }


def run_spark_job(
    config: SparkNormalizeConfig,
    *,
    spark: Any | None = None,
    source_reader: Callable[..., Any] = _read_partition,
    client_factory: Callable[..., Any] = create_s3_client,
) -> dict[str, Any]:
    """Publish v4 output with task/Driver inventories streamed through local disk."""
    started_at_ms = unix_ms()
    with tempfile.TemporaryDirectory(prefix="trajfoundry-driver-") as work:
        path = Path(work)
        with DiskManifestAccumulator(
            path / "manifest.sqlite",
            input_root=config.input_uri,
            input_format=config.input_format,
            config_hash=_config_hash(config),
            started_at_ms=started_at_ms,
        ) as accumulator:
            cleanup = []
            try:
                return _run_spark_job(
                    config,
                    accumulator=accumulator,
                    driver_work=path,
                    cleanup=cleanup,
                    spark=spark,
                    source_reader=source_reader,
                    client_factory=client_factory,
                )
            finally:
                for resource in reversed(cleanup):
                    try:
                        if hasattr(resource, "close"):
                            resource.close()
                        elif hasattr(resource, "destroy"):
                            resource.destroy()
                        else:
                            resource.unpersist()
                    except Exception:  # noqa: BLE001 - do not hide the original failure
                        LOGGER.warning("Spark intermediate cleanup failed")


__all__ = ["run_spark_job"]
