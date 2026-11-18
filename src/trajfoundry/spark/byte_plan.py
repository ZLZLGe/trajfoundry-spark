"""Streaming byte-balanced planning for Spark missing-session user scopes.

Only lightweight request metadata is sorted. A request is indivisible and byte
chunks deliberately form aggregation boundaries: prefix matching and mounting
remain local to each chunk, rather than being inferred during this planning.
"""

from __future__ import annotations

import json
from bisect import bisect_right
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from heapq import heappop, heappush
from itertools import groupby

from .component_plan import ScopeComponentSummary, merge_summaries

ScopeKey = tuple[str, str]
RecordOrderKey = tuple[int, str, str, str]


@dataclass(frozen=True, slots=True)
class ByteRecordMetadata:
    """One parsed request's planning metadata; no serialized body is retained."""

    scope: ScopeKey
    order_key: RecordOrderKey
    size_bytes: int
    summary: ScopeComponentSummary


@dataclass(frozen=True, slots=True)
class ByteChunk:
    scope: ScopeKey
    first_key: RecordOrderKey
    chunk_id: str
    total_bytes: int
    summary: ScopeComponentSummary


@dataclass(frozen=True, slots=True)
class ByteChunkIndex:
    """A compact broadcast index with cached boundaries for O(log chunks) lookup."""

    chunks: tuple[ByteChunk, ...]
    first_keys: tuple[RecordOrderKey, ...]


def record_order_key(
    captured_at: str, source_ref: str, source_sha256: str
) -> RecordOrderKey:
    """Order known instants in UTC, then unknown times by stable source identity.

    Empty, malformed and timezone-naive timestamps are all unknown; they never
    acquire a worker's local timezone. Explicit Z replacement supports the
    deployed Python 3.10 runtime as well as the development Python runtime.
    """

    try:
        value = (
            captured_at[:-1] + "+00:00" if captured_at.endswith("Z") else captured_at
        )
        instant = datetime.fromisoformat(value)
        if instant.tzinfo is not None and instant.utcoffset() is not None:
            return (
                0,
                instant.astimezone(timezone.utc).isoformat(timespec="microseconds"),  # noqa: UP017 - deployed Python 3.10
                source_ref,
                source_sha256,
            )
    except (AttributeError, TypeError, ValueError, OverflowError):
        pass
    return (1, "", source_ref, source_sha256)


def make_chunk_id(scope: ScopeKey, first_key: RecordOrderKey) -> str:
    """Stable identity independent of Spark partition ids and input enumeration."""

    encoded = json.dumps(
        [scope, first_key], ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    return sha256(b"trajfoundry-byte-chunk-v1\0" + encoded).hexdigest()


def allocate_chunk_count(
    scope_bytes: int, scope_count: int, total_bytes: int, target_chunks: int
) -> int:
    """Allocate a user's share of the global chunk target, capped by its count.

    Every nonempty user gets at least one chunk. Consequently ``target_chunks``
    is a proportional target, not a hard global chunk-count limit.
    """

    if target_chunks <= 0:
        raise ValueError("target_chunks must be positive")
    if scope_bytes < 0 or scope_count < 0 or total_bytes < scope_bytes:
        raise ValueError("invalid scope byte totals")
    if scope_count == 0:
        if scope_bytes:
            raise ValueError("empty scope cannot contain bytes")
        return 0
    if not total_bytes:
        return 1
    return min(
        scope_count,
        max(1, (scope_bytes * target_chunks + total_bytes - 1) // total_bytes),
    )


def target_chunk_bytes(
    scope_bytes: int, scope_count: int, total_bytes: int, target_chunks: int
) -> int:
    count = allocate_chunk_count(scope_bytes, scope_count, total_bytes, target_chunks)
    return max(1, (scope_bytes + count - 1) // count) if count else 0


def _checked_metadata(
    records: Iterable[ByteRecordMetadata],
) -> Iterator[ByteRecordMetadata]:
    previous = None
    for record in records:
        position = (record.scope, record.order_key)
        if previous is not None and position < previous:
            raise ValueError(
                "byte planning metadata must be sorted by scope and request"
            )
        if record.size_bytes < 0 or record.summary.count <= 0:
            raise ValueError("invalid request byte planning metadata")
        previous = position
        yield record


def plan_byte_chunks(
    sorted_metadata: Iterable[ByteRecordMetadata],
    target_bytes_by_scope: Mapping[ScopeKey, int],
) -> Iterator[ByteChunk]:
    """Stream sorted, complete user scopes using O(1) metadata per active scope.

    A request belongs to the target-sized interval containing its *starting*
    cumulative byte offset. It may extend past an interval boundary; it is not
    cut or prematurely moved into the next chunk. Thus ordinary requests cause
    at most one request of overshoot rather than producing many undersized
    chunks. An individually oversized request is isolated, so actual chunk
    counts can differ from the allocation when requests are indivisible.

    Duplicate ordering keys stay together to keep lookup boundaries unambiguous.
    The caller must put every request of a user in one sorted metadata partition;
    only yielded chunk summaries, not individual request metadata, are collected.
    """

    for scope, records in groupby(
        _checked_metadata(sorted_metadata), key=lambda row: row.scope
    ):
        target = target_bytes_by_scope[scope]
        if target <= 0:
            raise ValueError("chunk byte target must be positive")
        cumulative = 0
        pending = None
        pending_bucket = None
        for order_key, same_key in groupby(records, key=lambda row: row.order_key):
            size = 0
            summary = ScopeComponentSummary()
            for record in same_key:
                size += record.size_bytes
                summary = merge_summaries(summary, record.summary)
            bucket = cumulative // target
            if pending is not None and (bucket != pending_bucket or size > target):
                yield pending
                pending = None
            if size > target:
                yield ByteChunk(
                    scope, order_key, make_chunk_id(scope, order_key), size, summary
                )
            elif pending is None:
                pending = ByteChunk(
                    scope, order_key, make_chunk_id(scope, order_key), size, summary
                )
                pending_bucket = bucket
            else:
                pending = ByteChunk(
                    scope,
                    pending.first_key,
                    pending.chunk_id,
                    pending.total_bytes + size,
                    merge_summaries(pending.summary, summary),
                )
            cumulative += size
        if pending is not None:
            yield pending


def index_chunks(chunks: Iterable[ByteChunk]) -> ByteChunkIndex:
    ordered = tuple(sorted(chunks, key=lambda chunk: chunk.first_key))
    if not ordered:
        raise ValueError("chunk index must not be empty")
    if any(chunk.scope != ordered[0].scope for chunk in ordered):
        raise ValueError("chunk index must contain one user scope")
    first_keys = tuple(chunk.first_key for chunk in ordered)
    if len(set(first_keys)) != len(first_keys):
        raise ValueError("chunk boundaries must be unique")
    return ByteChunkIndex(ordered, first_keys)


def find_chunk(
    chunks: ByteChunkIndex | Sequence[ByteChunk], order_key: RecordOrderKey
) -> ByteChunk:
    """Find a request's chunk; pass a cached index on per-request hot paths."""

    index = chunks if isinstance(chunks, ByteChunkIndex) else index_chunks(chunks)
    position = bisect_right(index.first_keys, order_key) - 1
    if position < 0:
        raise ValueError("request precedes the first planned chunk")
    return index.chunks[position]


def assign_scope_partitions(
    weights: Mapping[ScopeKey, int], partition_count: int
) -> dict[ScopeKey, int]:
    """Deterministic largest-first scheduling, including real-session scopes.

    Integer byte weights avoid runtime-dependent Python hash collisions placing
    multiple large chunks in one physical Spark partition. Equal loads favor
    the lowest partition id, and equal weights favor the stable scope identity.
    """

    if partition_count <= 0:
        raise ValueError("partition_count must be positive")
    if any(weight < 0 for weight in weights.values()):
        raise ValueError("partition weights must not be negative")
    heap = [(0, partition) for partition in range(partition_count)]
    result = {}
    for scope, weight in sorted(weights.items(), key=lambda pair: (-pair[1], pair[0])):
        load, partition = heappop(heap)
        result[scope] = partition
        heappush(heap, (load + weight, partition))
    return result


__all__ = [
    "ByteChunk",
    "ByteChunkIndex",
    "ByteRecordMetadata",
    "RecordOrderKey",
    "ScopeKey",
    "allocate_chunk_count",
    "assign_scope_partitions",
    "find_chunk",
    "index_chunks",
    "make_chunk_id",
    "plan_byte_chunks",
    "record_order_key",
    "target_chunk_bytes",
]
