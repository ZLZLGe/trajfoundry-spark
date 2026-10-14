from __future__ import annotations

import json
import pickle
from collections import defaultdict
from dataclasses import replace
from functools import partial

import pytest

from trajfoundry.identity import aggregation_scope
from trajfoundry.models import Snapshot
from trajfoundry.spark import job
from trajfoundry.spark.adapters import parse_payload
from trajfoundry.spark.byte_plan import (
    ByteChunk,
    allocate_chunk_count,
    assign_scope_partitions,
    index_chunks,
    make_chunk_id,
    plan_byte_chunks,
    record_order_key,
    target_chunk_bytes,
)
from trajfoundry.spark.component_plan import merge_summaries, summarize_snapshot
from trajfoundry.spark.config import SparkNormalizeConfig
from trajfoundry.validation import validate_output

from .test_spark_adapters import _tokenplan_envelope
from .test_spark_job import local_spark, synthetic_reader  # noqa: F401
from .test_spark_local_output import _forbidden_s3_client


def _record(index, *, user="alice", session="", padding=0):
    envelope = _tokenplan_envelope(session_id=session)
    envelope["metadata"]["source_user_id"] = user
    envelope["metadata"]["request_id"] = f"request-{index}"
    envelope["metadata"]["completed_at_ms"] = 1_789_430_400_000 + index * 1000
    body = envelope["client_request"]["capture"]["body"]
    body["messages"][0]["content"] = f"question-{index}:" + "测试正文" * padding
    return f"{user}/req_{index:03d}.json", json.dumps(envelope).encode()


def _item(index=0, *, captured_at=None, **kwargs):
    source, payload = _record(index, **kwargs)
    row = parse_payload(source, payload, input_format="tokenplan")
    if captured_at is not None:
        snapshot = Snapshot.model_validate_json(row.snapshot_json)
        snapshot.captured_at = captured_at
        row = replace(
            row,
            captured_at=captured_at,
            snapshot_json=snapshot.model_dump_json().encode(),
        )
    return "ok", row.as_tuple()


def _chunk_scope(chunk):
    return aggregation_scope("", chunk.scope[1], "", chunk.chunk_id)


def test_scope_byte_summary_uses_serialized_body_bytes_and_normalized_order():
    item = _item(padding=1000, captured_at="2026-09-15T08:00:00+08:00")
    scope, (summary, size, order) = job._scope_byte_summary(item)
    body = item[1][-1]
    assert scope == ("user", "alice")
    assert size == len(body)
    assert size > len(body.decode())
    assert summary == summarize_snapshot(Snapshot.model_validate_json(body))
    assert order == record_order_key("2026-09-15T00:00:00Z", item[1][0], item[1][1])


@pytest.mark.parametrize("timestamp", ["", "invalid", "2026-09-15T08:00:00"])
def test_unknown_capture_time_orders_by_source_identity(timestamp):
    item = _item(captured_at=timestamp)
    _, (_, _, order) = job._scope_byte_summary(item)
    assert order == (1, "", item[1][0], item[1][1])


def test_lightweight_metadata_does_not_contain_body_and_summaries_reduce_exactly():
    first = _item(0, padding=1000)
    second = _item(1, padding=1)
    sort_key, metadata = job._byte_metadata(first)
    assert sort_key == (metadata.scope, metadata.order_key)
    assert metadata.size_bytes == len(first[1][-1])
    encoded = pickle.dumps(metadata)
    assert len(encoded) < 1024
    assert "测试正文".encode() not in encoded
    assert first[1][-1] not in encoded
    left = job._scope_byte_summary(first)[1]
    right = job._scope_byte_summary(second)[1]
    merged = job._merge_byte_summaries(left, right)
    assert merged == job._merge_byte_summaries(right, left)
    assert merged[0].count == 2
    assert merged[1] == len(first[1][-1]) + len(second[1][-1])
    assert merged[2] == min(left[2], right[2])


def test_planned_routing_uses_byte_boundary_and_preserves_original_body_object():
    items = [
        _item(0, padding=1),
        _item(1, padding=1000),
        _item(2, padding=2, captured_at=""),
    ]
    metadata = sorted(job._byte_metadata(item) for item in items)
    chunks = list(
        plan_byte_chunks((value for _, value in metadata), {("user", "alice"): 4000})
    )
    assert len(chunks) == 3
    indexes = {("user", "alice"): index_chunks(chunks)}
    summaries = {_chunk_scope(chunk): chunk.summary for chunk in chunks}
    seen = set()
    for item in reversed(items):
        key, planned = job._planned_component_sort_key(item, indexes, summaries)
        assert key[:2] == planned[1][2:4]
        assert key[0] == "user_chunk"
        assert json.loads(key[1])[0] == "alice"
        assert planned[1][-1] is item[1][-1]
        assert Snapshot.model_validate_json(planned[1][-1]).sessionless_chunk_id == ""
        assert key[3] == record_order_key(item[1][7], item[1][0], item[1][1])
        seen.add(json.loads(key[1])[1])
    assert seen == {chunk.chunk_id for chunk in chunks}


def test_real_session_and_parse_failure_do_not_require_chunk_index():
    item = _item(session="real-session")
    scope, summary = job._component_summary(item)
    key, planned = job._planned_component_sort_key(item, {}, {scope: summary})
    assert key[:2] == ("session", "real-session")
    assert planned is item
    failed = "error", {"source_ref": "broken/req.json", "reason": "parse_failure"}
    key, planned = job._planned_component_sort_key(failed, {}, {})
    assert key[:2] == ("parse_failure", "broken/req.json")
    assert planned is failed


@pytest.mark.parametrize("threshold", [1, 1000], ids=["disk", "memory"])
def test_builder_injects_chunk_marker_without_mutating_cached_json(
    monkeypatch, threshold
):
    from trajfoundry.spark import bounded

    item = _item()
    chunk_id = "c" * 64
    scope = aggregation_scope("", "alice", "", chunk_id)
    value = (*item[1][:2], *scope, *item[1][4:])
    captured = []

    def builder(snapshots, **_kwargs):
        captured.extend(snapshots)
        return iter(())

    monkeypatch.setattr(job, "build_scope", builder)
    monkeypatch.setattr(bounded, "build_scope_bounded", builder)
    artifacts = list(
        job._build_partition(
            [(scope, iter([("ok", value)]))], max_scope_snapshots=threshold
        )
    )
    assert len(captured) == 1
    assert captured[0].sessionless_chunk_id == chunk_id
    assert captured[0].user_id == "alice"
    assert Snapshot.model_validate_json(item[1][-1]).sessionless_chunk_id == ""
    assert ("counts", {"input_files": 1}) in artifacts


def test_planned_physical_partition_is_used_including_partition_zero(monkeypatch):
    weights = {
        ("user_chunk", "heavy-a"): 100,
        ("user_chunk", "heavy-b"): 100,
        ("session", "real"): 10,
    }
    mapping = assign_scope_partitions(weights, 2)

    def forbidden_hash(_key):
        pytest.fail("planned scope fell back to hash partitioning")

    monkeypatch.setattr(job, "_scope_partition", forbidden_hash)
    for scope, physical_partition in mapping.items():
        key = (*scope, "whole", (1, "", "source", "hash"))
        assert job._planned_partition(key, mapping) == physical_partition
    assert set(mapping.values()) == {0, 1}


def _expected_chunks(records, partitions):
    metadata_by_scope = defaultdict(list)
    for path, payload in records:
        row = parse_payload(path, payload, input_format="tokenplan")
        if row.scope_kind == "user":
            _, metadata = job._byte_metadata(("ok", row.as_tuple()))
            metadata_by_scope[metadata.scope].append(metadata)
    total = sum(
        item.size_bytes for items in metadata_by_scope.values() for item in items
    )
    result = []
    for scope, items in metadata_by_scope.items():
        items.sort(key=lambda item: item.order_key)
        size = sum(item.size_bytes for item in items)
        if allocate_chunk_count(size, len(items), total, partitions) == 1:
            summary = items[0].summary
            for item in items[1:]:
                summary = merge_summaries(summary, item.summary)
            result.append(
                ByteChunk(
                    scope,
                    items[0].order_key,
                    make_chunk_id(scope, items[0].order_key),
                    size,
                    summary,
                )
            )
        else:
            target = target_chunk_bytes(size, len(items), total, partitions)
            result.extend(plan_byte_chunks(items, {scope: target}))
    return result, total


def test_local_spark_unequal_requests_and_reversed_input_preserve_all_origins(
    local_spark,  # noqa: F811
    tmp_path,
):
    records = [
        _record(index, padding=padding)
        for index, padding in enumerate([1, 1, 5000, 1, 1, 500, 1, 1])
    ]
    records += [
        _record(10, user="bob", padding=1),
        _record(11, user="carol", session="real-session", padding=1),
    ]
    partitions = 4
    expected_chunks, total = _expected_chunks(records, partitions)
    published = []
    for name, ordered, shards in (
        ("forward", records, ("alice/", "bob/", "carol/")),
        ("reversed", list(reversed(records)), ("carol/", "bob/", "alice/")),
    ):
        output = tmp_path / name
        result = job.run_spark_job(
            SparkNormalizeConfig(
                "s3://fake/input/",
                str(output),
                prefix_shards=shards,
                shuffle_partitions=partitions,
            ),
            spark=local_spark,
            source_reader=partial(synthetic_reader, records=ordered),
            client_factory=_forbidden_s3_client,
        )
        assert result["input_files"] == result["trajectories"] == len(records)
        assert result["parse_failures"] == 0
        performance = result["performance"]
        assert performance["sessionless_chunks"] == len(expected_chunks)
        assert performance["sessionless_input_bytes"] == total
        assert performance["largest_scope_records"] == 8
        assert performance["largest_chunk_bytes"] == max(
            chunk.total_bytes for chunk in expected_chunks
        )
        assert performance["largest_chunk_records"] == max(
            chunk.summary.count for chunk in expected_chunks
        )
        assert performance["byte_plan_seconds"] >= 0
        report = validate_output(output)
        assert report.valid, report.errors
        lineage = [
            json.loads(line)
            for line in (output / "lineage.jsonl").read_bytes().splitlines()
        ]
        assert sum(item["origin_count"] for item in lineage) == len(records)
        assert {
            origin["source_ref"] for item in lineage for origin in item["origins"]
        } == {path for path, _ in records}
        published.append(json.loads((output / "manifest.json").read_bytes())["files"])
    assert published[0] == published[1]
