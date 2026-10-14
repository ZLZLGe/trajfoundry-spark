from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from functools import partial

import pytest

from trajfoundry.canonical import semantic_payload, trajectory_id
from trajfoundry.identity import aggregation_scope, sessionless_window
from trajfoundry.spark.adapters import parse_payload
from trajfoundry.spark.bounded import build_scope_bounded
from trajfoundry.spark.build import build_scope
from trajfoundry.spark.config import SparkNormalizeConfig
from trajfoundry.spark.job import _component_summary, run_spark_job
from trajfoundry.spark.output import serialize_trajectory
from trajfoundry.streaming import streaming_prefix_leaves
from trajfoundry.validation import validate_output

from .test_spark_adapters import _tokenplan_envelope
from .test_spark_build import _duplicate_capture
from .test_spark_job import local_spark, synthetic_reader  # noqa: F401
from .test_spark_local_output import _forbidden_s3_client
from .test_subagents import assistant, snapshot, spawn, user


@pytest.mark.parametrize(
    "instant,expected",
    [
        ("2026-10-12T15:59:59.999999Z", "2026-10-12T16:00:00+08:00"),
        ("2026-10-12T16:00:00Z", "2026-10-13T00:00:00+08:00"),
        ("2026-10-12T23:59:59.999999Z", "2026-10-13T00:00:00+08:00"),
        ("2026-10-13T00:00:00Z", "2026-10-13T08:00:00+08:00"),
        ("2026-10-13T07:59:59.999999Z", "2026-10-13T08:00:00+08:00"),
        ("2026-10-13T08:00:00Z", "2026-10-13T16:00:00+08:00"),
        ("2026-10-13T15:59:59.999999Z", "2026-10-13T16:00:00+08:00"),
        ("2026-10-13T16:00:00Z", "2026-10-14T00:00:00+08:00"),
        ("2026-10-13T08:00:00+08:00", "2026-10-13T08:00:00+08:00"),
        ("2026-10-12T20:00:00-04:00", "2026-10-13T08:00:00+08:00"),
    ],
)
def test_fixed_beijing_windows_include_date_and_exact_boundaries(instant, expected):
    assert sessionless_window(instant) == expected


@pytest.mark.parametrize(
    "timestamp", ["", "invalid", "2026-10-13", "2026-10-13T08:00:00"]
)
@pytest.mark.parametrize("session", ["", "no_session_id"])
def test_unknown_timestamp_retains_exact_user_fallback(timestamp, session):
    assert aggregation_scope(session, "alice", timestamp) == ("user", "alice")
    assert aggregation_scope(session, "", timestamp) == ("user", "")
    assert aggregation_scope(session, "no_user_id", timestamp) == ("user", "no_user_id")


def test_scope_encoding_is_collision_safe_and_real_session_is_time_independent():
    users = ["", "no_user_id", "alice", 'alice,"2026-10-13"', "用户"]
    scopes = [aggregation_scope("", value, "2026-10-13T00:00:00Z") for value in users]
    assert len(set(scopes)) == len(users)
    assert scopes == [("user", value) for value in users]
    chunks = [
        aggregation_scope("", value, "2026-10-13T00:00:00Z", "a" * 64)
        for value in users
    ]
    assert len(set(chunks)) == len(users)
    assert [json.loads(value)[0] for _, value in chunks] == users
    assert aggregation_scope("real", "alice", "invalid") == ("session", "real")
    assert aggregation_scope("real", "bob", "2026-10-13T08:00:00Z") == (
        "session",
        "real",
    )


def _capture(path, timestamp, *, session=""):
    return _duplicate_capture(
        path, captured_at=timestamp, thread="thread", session=session
    )


def _groups(captures):
    groups = defaultdict(list)
    for item in captures:
        groups[
            aggregation_scope(
                item.session_id,
                item.user_id,
                item.captured_at,
                item.sessionless_chunk_id,
            )
        ].append(item)
    return groups.values()


@pytest.mark.parametrize("session", ["", "no_session_id", "real"])
def test_unassigned_scopes_merge_across_windows_without_changing_real_sessions(
    tmp_path, session
):
    first = _capture("first.json", "2026-10-12T23:59:59Z", session=session)
    same = first.model_copy(
        update={"source_path": "same.json", "captured_at": "2026-10-12T22:00:00Z"}
    )
    later = first.model_copy(
        update={"source_path": "later.json", "captured_at": "2026-10-13T00:00:00Z"}
    )
    extending = later.model_copy(
        update={
            "source_path": "extending.json",
            "history": [*later.history, *later.response, user("next")],
            "response": [assistant("done")],
        }
    )
    captures = [first, same, later, extending]
    prefix = streaming_prefix_leaves(captures)
    assert len(prefix.leaves) == 1
    built = build_scope(captures)
    assert len(built) == 1
    assert sorted(len(item.origins) for item in built) == [4]
    assert build_scope(reversed(captures)) == built
    disk = [
        item
        for group in _groups(captures)
        for item in build_scope_bounded(group, workspace_parent=tmp_path)
    ]
    assert sorted(disk, key=lambda item: trajectory_id(item.node)) == sorted(
        built, key=lambda item: trajectory_id(item.node)
    )


def test_identical_transcripts_in_explicit_chunks_have_distinct_files_and_lineage():
    times = [
        "2026-10-12T23:59:59Z",
        "2026-10-13T00:00:00Z",
        "2026-10-13T08:00:00Z",
        "2026-10-13T16:00:00Z",
        "invalid",
    ]
    captures = [_capture(f"{index}.json", value) for index, value in enumerate(times)]
    for index, capture in enumerate(captures):
        capture.sessionless_chunk_id = f"{index:064x}"
    outputs = [
        serialize_trajectory(item.node, item.origins) for item in build_scope(captures)
    ]
    assert len(outputs) == len(times)
    assert len({item.filename for item in outputs}) == len(times)
    assert len({item.lineage["trajectory_id"] for item in outputs}) == len(times)
    assert all(item.filename.startswith("no_session_id_") for item in outputs)
    for item in build_scope(captures):
        identity = semantic_payload(item.node)["aggregation_identity"]
        assert (
            identity["sessionless_chunk_id"] == item.node.metadata.sessionless_chunk_id
        )
        assert "sessionless_window" not in identity


def _mount_pair(child_time):
    parent = snapshot(
        "parent",
        thread="main",
        turn="turn-1",
        history=[user("go")],
        response=[assistant(calls=[spawn("call", "child")])],
    )
    child = snapshot(
        "child",
        thread="child",
        turn="child-turn",
        response=[assistant("result")],
        parent_thread="main",
        parent_turn="turn-1",
        forked_from="main",
        marker=json.dumps({"spawn_call_id": "call", "task_name": "child"}),
    )
    for item, timestamp in ((parent, "2026-10-12T23:59:59Z"), (child, child_time)):
        item.session_id = ""
        item.user_id = "alice"
        item.captured_at = timestamp
    return [parent, child]


@pytest.mark.parametrize("chunks", ["unassigned", "same", "different"])
def test_subagent_mounting_ignores_windows_but_stays_within_chunk(tmp_path, chunks):
    captures = _mount_pair("2026-10-13T00:00:00Z")
    if chunks != "unassigned":
        captures[0].sessionless_chunk_id = "a" * 64
        captures[1].sessionless_chunk_id = ("a" if chunks == "same" else "b") * 64
    built = build_scope(captures)
    if chunks != "different":
        assert len(built) == 1
        assert set(built[0].node.sub_agent_trajectory or {}) == {"call"}
        assert tuple(build_scope_bounded(captures, workspace_parent=tmp_path)) == built
    else:
        assert len(built) == 2
        assert all(not item.node.sub_agent_trajectory for item in built)
        disk = [
            item
            for group in _groups(captures)
            for item in build_scope_bounded(group, workspace_parent=tmp_path)
        ]
        assert sorted(disk, key=lambda item: trajectory_id(item.node)) == list(built)
        with pytest.raises(ValueError, match="requires one aggregation scope"):
            tuple(build_scope_bounded(captures, workspace_parent=tmp_path))
        assert list(tmp_path.iterdir()) == []


def _record(index, timestamp):
    envelope = _tokenplan_envelope(session_id="no_session_id" if index % 2 else "")
    millis = int(datetime.fromisoformat(timestamp).timestamp() * 1000)
    envelope["metadata"].update(
        source_user_id="alice", received_at_ms=millis, completed_at_ms=millis
    )
    return f"alice/req_{index}.json", json.dumps(envelope).encode()


def test_parsed_scope_remains_user_before_byte_planning_across_windows():
    records = [
        _record(index, value)
        for index, value in enumerate(
            [
                "2026-10-12T23:59:59+00:00",
                "2026-10-13T00:00:00+00:00",
                "2026-10-13T08:00:00+00:00",
            ]
        )
    ]
    rows = [
        parse_payload(path, payload, input_format="tokenplan")
        for path, payload in records
    ]
    keys = [_component_summary(("ok", row.as_tuple()))[0] for row in rows]
    assert set(keys) == {("user", "alice")}


@pytest.mark.parametrize("partitions,expected_roots", [(1, 1), (3, 3)])
def test_synthetic_spark_publish_keeps_byte_chunks_distinct_in_memory_and_disk(
    partitions,
    expected_roots,
    local_spark,  # noqa: F811
    tmp_path,
):
    records = [
        _record(index, value)
        for index, value in enumerate(
            [
                "2026-10-12T23:59:59+00:00",
                "2026-10-13T00:00:00+00:00",
                "2026-10-13T08:00:00+00:00",
                "2026-10-13T16:00:00+00:00",
                "2026-10-13T16:00:01+00:00",
            ]
        )
    ]
    files = []
    for name, threshold in (("memory", 1000), ("disk", 1)):
        output = tmp_path / name
        result = run_spark_job(
            SparkNormalizeConfig(
                "s3://fake/input/",
                str(output),
                prefix_shards=("alice/",),
                shuffle_partitions=partitions,
                max_scope_snapshots=threshold,
            ),
            spark=local_spark,
            source_reader=partial(synthetic_reader, records=records),
            client_factory=_forbidden_s3_client,
        )
        assert result["trajectories"] == expected_roots
        assert result["performance"]["largest_scope_records"] == 5
        assert result["performance"]["sessionless_chunks"] == expected_roots
        assert result["performance"]["largest_chunk_records"] == (
            5 if partitions == 1 else 2
        )
        report = validate_output(output)
        assert report.valid, report.errors
        manifest = json.loads((output / "manifest.json").read_bytes())
        assert manifest["sessionless_partitioning"] == "normalized-bytes-v1"
        assert "sessionless_window_hours" not in manifest
        files.append(manifest["files"])
        lineage = [
            json.loads(line)
            for line in (output / "lineage.jsonl").read_bytes().splitlines()
        ]
        assert len({item["trajectory_id"] for item in lineage}) == expected_roots
        assert sum(item["origin_count"] for item in lineage) == 5
        assert len(list(output.glob("no_session_id_*.jsonl"))) == expected_roots
    assert files[0] == files[1]
