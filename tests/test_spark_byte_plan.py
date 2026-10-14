from __future__ import annotations

import pickle
import random
from collections import defaultdict
from functools import reduce

import pytest

from trajfoundry.spark.byte_plan import (
    ByteRecordMetadata,
    allocate_chunk_count,
    assign_scope_partitions,
    find_chunk,
    index_chunks,
    make_chunk_id,
    plan_byte_chunks,
    record_order_key,
    target_chunk_bytes,
)
from trajfoundry.spark.component_plan import ScopeComponentSummary, merge_summaries


def _metadata(sizes, scope=("user", "one")):
    return [
        ByteRecordMetadata(
            scope,
            record_order_key("2026-09-15T00:00:00Z", f"request-{index:05}", str(index)),
            size,
            ScopeComponentSummary(1, index + 1, index == 2),
        )
        for index, size in enumerate(sizes)
    ]


def test_proportional_allocation_uses_integer_bytes_and_caps_by_count():
    assert allocate_chunk_count(80, 10_000, 100, 512) == 410
    assert allocate_chunk_count(20, 10_000, 100, 512) == 103
    assert allocate_chunk_count(80, 2, 100, 512) == 2
    assert allocate_chunk_count(1, 50, 10_000, 512) == 1
    assert allocate_chunk_count(0, 0, 0, 512) == 0
    assert allocate_chunk_count(0, 1, 0, 512) == 1
    assert target_chunk_bytes(80, 10_000, 100, 4) == 20
    assert target_chunk_bytes(5, 10_000, 10, 3) == 3
    assert target_chunk_bytes(0, 0, 0, 512) == 0
    assert allocate_chunk_count(10**100, 10_000, 10**100, 512) == 512


@pytest.mark.parametrize(
    "args", [(1, 1, 1, 0), (-1, 1, 1, 2), (2, 1, 1, 2), (1, 0, 1, 2)]
)
def test_invalid_allocation_fails(args):
    with pytest.raises(ValueError):
        allocate_chunk_count(*args)


def test_timestamp_ordering_is_utc_and_independent_of_local_timezone():
    assert record_order_key("2026-09-15T08:00:00+08:00", "a", "x") == record_order_key(
        "2026-09-15T00:00:00Z", "a", "x"
    )
    before = record_order_key("2026-09-15T07:00:00+08:00", "b", "x")
    after = record_order_key("2026-09-15T00:00:00+00:00", "a", "x")
    assert before < after
    assert record_order_key("2026-09-15T00:00:00.1Z", "a", "x") > after
    assert after < record_order_key("", "a", "x")


@pytest.mark.parametrize(
    "timestamp", ["", "not-a-date", "2026-09-15T08:00:00", None, 42]
)
def test_unknown_timestamps_have_stable_source_order(timestamp):
    assert record_order_key(timestamp, "a", "x") == (1, "", "a", "x")
    assert record_order_key(timestamp, "a", "x") < record_order_key(timestamp, "b", "x")
    assert record_order_key(timestamp, "a", "x") < record_order_key(timestamp, "a", "y")


def test_balancing_preserves_all_requests_and_merges_summaries():
    records = _metadata([3, 3, 3, 3, 3, 3, 3])
    chunks = list(plan_byte_chunks(iter(records), {records[0].scope: 7}))
    assert [chunk.total_bytes for chunk in chunks] == [9, 6, 6]
    assert [chunk.summary.count for chunk in chunks] == [3, 2, 2]
    assert chunks[0].summary == ScopeComponentSummary(3, 1, True)
    assert reduce(merge_summaries, (chunk.summary for chunk in chunks)) == reduce(
        merge_summaries, (record.summary for record in records)
    )
    index = index_chunks(reversed(chunks))
    mapped = defaultdict(int)
    for record in records:
        mapped[find_chunk(index, record.order_key).chunk_id] += record.size_bytes
    assert dict(mapped) == {chunk.chunk_id: chunk.total_bytes for chunk in chunks}


def test_indivisible_large_record_is_its_own_chunk_and_last_chunk_is_retained():
    records = _metadata([1, 20, 2, 3])
    chunks = list(plan_byte_chunks(records, {records[0].scope: 5}))
    assert [chunk.total_bytes for chunk in chunks] == [1, 20, 5]
    assert [chunk.summary.count for chunk in chunks] == [1, 1, 2]
    assert sum(chunk.total_bytes for chunk in chunks) == 26


def test_next_request_is_not_prematurely_flushed_at_budget_boundary():
    records = _metadata([4, 4, 2])
    chunks = list(plan_byte_chunks(records, {records[0].scope: 5}))
    assert [chunk.total_bytes for chunk in chunks] == [8, 2]


def test_multiple_sorted_scopes_and_zero_byte_requests():
    first = _metadata([0, 0], ("user", ""))
    second = _metadata([1, 2], ("user", "second"))
    chunks = list(
        plan_byte_chunks(first + second, {first[0].scope: 1, second[0].scope: 2})
    )
    assert [
        (chunk.scope, chunk.total_bytes, chunk.summary.count) for chunk in chunks
    ] == [
        (("user", ""), 0, 2),
        (("user", "second"), 3, 2),
    ]
    assert list(plan_byte_chunks([], {})) == []


def test_duplicate_order_keys_never_create_ambiguous_boundaries():
    records = _metadata([4, 4])
    records.insert(1, records[0])
    chunks = list(plan_byte_chunks(records, {records[0].scope: 5}))
    assert [chunk.total_bytes for chunk in chunks] == [8, 4]
    assert find_chunk(index_chunks(chunks), records[0].order_key) == chunks[0]


def test_plan_and_ids_are_deterministic_across_input_order_and_pickle():
    records = _metadata([1, 7, 2, 4, 12, 1, 8])
    expected = list(plan_byte_chunks(records, {records[0].scope: 8}))
    shuffled = records.copy()
    random.Random(14).shuffle(shuffled)
    actual = list(
        plan_byte_chunks(
            sorted(shuffled, key=lambda row: (row.scope, row.order_key)),
            {records[0].scope: 8},
        )
    )
    assert actual == expected
    assert pickle.loads(pickle.dumps(actual)) == actual
    assert pickle.loads(pickle.dumps(index_chunks(actual))) == index_chunks(actual)
    assert all(
        len(chunk.chunk_id) == 64 and int(chunk.chunk_id, 16) >= 0 for chunk in actual
    )
    assert make_chunk_id(("user", "other"), records[0].order_key) != actual[0].chunk_id
    assert len(pickle.dumps(records[0])) < 512


def test_unsorted_metadata_and_invalid_sizes_are_rejected():
    records = _metadata([1, 2])
    with pytest.raises(ValueError, match="sorted"):
        list(plan_byte_chunks(reversed(records), {records[0].scope: 1}))
    with pytest.raises(ValueError, match="positive"):
        list(plan_byte_chunks(records, {records[0].scope: 0}))
    with pytest.raises(ValueError, match="invalid request"):
        list(plan_byte_chunks(_metadata([-1]), {records[0].scope: 1}))


def test_chunk_lookup_validates_before_first_and_inconsistent_index():
    records = _metadata([2, 2])
    chunks = list(plan_byte_chunks(records, {records[0].scope: 2}))
    with pytest.raises(ValueError, match="precedes"):
        find_chunk(index_chunks(chunks), (0, "", "", ""))
    with pytest.raises(ValueError, match="unique"):
        index_chunks([chunks[0], chunks[0]])
    with pytest.raises(ValueError, match="empty"):
        index_chunks([])
    other = list(
        plan_byte_chunks(_metadata([1], ("user", "other")), {("user", "other"): 2})
    )
    with pytest.raises(ValueError, match="one user"):
        index_chunks(chunks + other)
    assert find_chunk(chunks, records[-1].order_key) == chunks[-1]


def test_largest_first_assignment_includes_sessions_and_is_deterministic():
    weights = {
        ("user", "chunk-a"): 100,
        ("user", "chunk-b"): 100,
        ("session", "real"): 20,
        ("user", "small"): 10,
    }
    assignment = assign_scope_partitions(weights, 2)
    assert assignment == assign_scope_partitions(
        dict(reversed(list(weights.items()))), 2
    )
    assert assignment[("user", "chunk-a")] != assignment[("user", "chunk-b")]
    loads = [0, 0]
    for scope, partition in assignment.items():
        loads[partition] += weights[scope]
    assert sorted(loads) == [110, 120]
    assert assign_scope_partitions({}, 3) == {}
    with pytest.raises(ValueError, match="positive"):
        assign_scope_partitions(weights, 0)
    with pytest.raises(ValueError, match="negative"):
        assign_scope_partitions({("user", "bad"): -1}, 2)


def test_streaming_planner_does_not_materialize_all_request_metadata():
    consumed = 0

    def source():
        nonlocal consumed
        for record in _metadata([1] * 1000):
            consumed += 1
            yield record

    chunks = plan_byte_chunks(source(), {("user", "one"): 10})
    assert next(chunks).total_bytes == 10
    assert consumed < 20
    assert sum(chunk.total_bytes for chunk in chunks) == 990
