from __future__ import annotations

import json
from collections import defaultdict
from hashlib import sha256

import pytest

from trajfoundry.canonical import semantic_payload, trajectory_id
from trajfoundry.identity import aggregation_scope, sessionless_window
from trajfoundry.models import Snapshot
from trajfoundry.output_contract import OutputContractError
from trajfoundry.spark.bounded import build_scope_bounded
from trajfoundry.spark.build import build_scope
from trajfoundry.streaming import streaming_prefix_leaves

from .test_spark_build import _duplicate_capture
from .test_subagents import assistant, snapshot, spawn, user


def _chunk(name):
    return sha256(name.encode()).hexdigest() if name else ""


def _capture(path, timestamp, *, chunk="part-0", session=""):
    result = _duplicate_capture(
        path, captured_at=timestamp, thread="first", session=session
    )
    result.sessionless_chunk_id = _chunk(chunk)
    return result


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


@pytest.mark.parametrize("timestamp", ["", "invalid", "2026-10-13T00:00:00Z"])
@pytest.mark.parametrize("session", ["", "no_session_id"])
def test_chunk_scope_does_not_depend_on_time(timestamp, session):
    assert aggregation_scope(session, "alice", timestamp, "part-2") == (
        "user_chunk",
        '["alice","part-2"]',
    )
    assert aggregation_scope(session, "alice", timestamp) == ("user", "alice")


def test_chunk_scope_encoding_is_collision_safe_and_real_session_ignores_chunk():
    values = [("", "x"), ("no_user_id", "x"), ("a,b", "c"), ("a", "b,c")]
    scopes = [aggregation_scope("", owner, "", part) for owner, part in values]
    assert len(set(scopes)) == len(values)
    assert [tuple(json.loads(value)) for _, value in scopes] == values
    assert aggregation_scope("real", "alice", "invalid", "part-9") == (
        "session",
        "real",
    )


def test_snapshot_roundtrip_preserves_chunk_without_changing_request_content():
    original = _capture("one.json", "", chunk="part-7")
    restored = Snapshot.model_validate_json(original.model_dump_json())
    assert restored == original
    assert restored.sessionless_chunk_id == _chunk("part-7")
    assert restored.history == [user("question")]
    assert restored.response == [assistant("answer")]


@pytest.mark.parametrize("chunk", ["part-0", ""])
def test_single_chunk_can_merge_prefix_across_old_eight_hour_boundary(tmp_path, chunk):
    first = _capture("first.json", "2026-10-12T23:59:59Z", chunk=chunk)
    second = first.model_copy(
        update={
            "source_path": "second.json",
            "captured_at": "2026-10-13T00:00:00Z",
            "history": [*first.history, *first.response, user("next")],
            "response": [assistant("done")],
        }
    )
    assert len(streaming_prefix_leaves([first, second]).leaves) == 1
    built = build_scope([first, second])
    assert len(built) == 1
    assert len(built[0].origins) == 2
    assert built[0].node.metadata.sessionless_chunk_id == _chunk(chunk)
    assert (
        tuple(build_scope_bounded([first, second], workspace_parent=tmp_path)) == built
    )
    assert build_scope([second, first]) == built


@pytest.mark.parametrize("timestamp", ["", "invalid", "2026-10-13T00:00:00Z"])
def test_identical_transcripts_in_different_chunks_have_distinct_ids(
    tmp_path, timestamp
):
    captures = [
        _capture("a.json", timestamp, chunk="part-0"),
        _capture("b.json", timestamp, chunk="part-1"),
    ]
    built = build_scope(captures)
    assert len(built) == 2
    assert len(streaming_prefix_leaves(captures).leaves) == 2
    assert len({trajectory_id(item.node) for item in built}) == 2
    assert {item.node.metadata.sessionless_chunk_id for item in built} == {
        _chunk("part-0"),
        _chunk("part-1"),
    }
    for item in built:
        identity = semantic_payload(item.node)["aggregation_identity"]
        assert (
            identity["sessionless_chunk_id"] == item.node.metadata.sessionless_chunk_id
        )
        assert "sessionless_window" not in identity
    disk = [
        item
        for group in _groups(captures)
        for item in build_scope_bounded(group, workspace_parent=tmp_path)
    ]
    assert sorted(disk, key=lambda item: trajectory_id(item.node)) == list(built)
    with pytest.raises(ValueError, match="requires one aggregation scope"):
        tuple(build_scope_bounded(captures, workspace_parent=tmp_path))
    assert list(tmp_path.iterdir()) == []


def _mount_pair(*, same_chunk):
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
    for item, timestamp, chunk in (
        (parent, "2026-10-12T23:59:59Z", "part-0"),
        (child, "2026-10-13T00:00:00Z", "part-0" if same_chunk else "part-1"),
    ):
        item.session_id = ""
        item.user_id = "alice"
        item.captured_at = timestamp
        item.sessionless_chunk_id = _chunk(chunk)
    return [parent, child]


@pytest.mark.parametrize("same_chunk", [True, False])
def test_subagent_mounting_uses_chunk_not_time_boundary(tmp_path, same_chunk):
    captures = _mount_pair(same_chunk=same_chunk)
    built = build_scope(captures)
    if same_chunk:
        assert len(built) == 1
        assert set(built[0].node.sub_agent_trajectory or {}) == {"call"}
        child = built[0].node.sub_agent_trajectory["call"]
        assert child.metadata.sessionless_chunk_id == _chunk("part-0")
    else:
        assert len(built) == 2
        assert all(not item.node.sub_agent_trajectory for item in built)
    disk = [
        item
        for group in _groups(captures)
        for item in build_scope_bounded(group, workspace_parent=tmp_path)
    ]
    assert sorted(disk, key=lambda item: trajectory_id(item.node)) == sorted(
        built, key=lambda item: trajectory_id(item.node)
    )


def test_real_session_merges_and_hashes_without_chunk_identity(tmp_path):
    first = _capture("a.json", "2026-10-12T23:59:59Z", session="real")
    second = _capture("b.json", "2026-10-13T00:00:00Z", chunk="part-1", session="real")
    built = build_scope([first, second])
    assert len(built) == 1
    assert built[0].node.metadata.sessionless_chunk_id == ""
    identity = semantic_payload(built[0].node)["aggregation_identity"]
    assert "sessionless_chunk_id" not in identity
    original_id = trajectory_id(built[0].node)
    changed = built[0].node.model_copy(deep=True)
    changed.metadata.sessionless_chunk_id = _chunk("not-a-real-session-boundary")
    with pytest.raises(OutputContractError, match="must be omitted for a real session"):
        trajectory_id(changed)
    first.sessionless_chunk_id = second.sessionless_chunk_id = ""
    assert trajectory_id(build_scope([first, second])[0].node) == original_id
    assert (
        tuple(build_scope_bounded([first, second], workspace_parent=tmp_path)) == built
    )


def test_explicit_legacy_window_policy_keeps_published_hash():
    old = _capture("legacy.json", "2026-10-13T00:00:00Z", chunk="")
    node = build_scope([old])[0].node
    assert sessionless_window(node.metadata.created_at) == "2026-10-13T08:00:00+08:00"
    assert trajectory_id(node, sessionless_window_hours=8) == (
        "d6bc7383a709f2e67f416bbe3a82c59c37c1d157a4376152c0dfd7658a313acb"
    )
    assert trajectory_id(node) == (
        "0caa2c7666d9d959dfed7852499dafc7fd25a6ff900567b71374a24616c00312"
    )
    assert "sessionless_window" not in semantic_payload(node)["aggregation_identity"]
