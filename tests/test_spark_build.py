from __future__ import annotations

import json

import pytest

from trajfoundry.identity import aggregation_scope
from trajfoundry.models import (
    AuditIssue,
    AuditTag,
    FunctionCall,
    Message,
    Snapshot,
    ToolCall,
    ToolDefinition,
    TrajectoryNode,
)
from trajfoundry.output_contract import parse_trajectory_record
from trajfoundry.pipeline import PipelineStats, _build_trajectories
from trajfoundry.quality import StaleDerivedFieldsError, validate_derived_fields
from trajfoundry.spark.adapters import parse_payload
from trajfoundry.spark.build import build_scope
from trajfoundry.spark.output import serialize_trajectory
from trajfoundry.state import StateStore

from .test_spark_adapters import _tokenplan_envelope
from .test_subagents import assistant, snapshot, spawn, user


def _snapshot(source_ref: str = "req.json") -> Snapshot:
    row = parse_payload(
        source_ref,
        json.dumps(_tokenplan_envelope()).encode(),
        input_format="tokenplan",
    )
    return Snapshot.model_validate_json(row.snapshot_json)


def test_build_scope_materializes_one_trajectory_without_sqlite() -> None:
    built = build_scope([_snapshot()])

    assert len(built) == 1
    result = built[0]
    assert result.node.metadata.session_id == "session-1"
    assert result.node.metadata.sub_session_id == 0
    assert [message.role for message in result.node.messages] == [
        "user",
        "assistant",
    ]
    assert len(result.origins) == 1
    assert result.origins[0]["source_ref"] == "req.json"
    assert len(result.origins[0]["sha256"]) == 64
    assert result.origins[0]["captured_at"] == "1970-01-01T00:00:01.234Z"


def test_build_scope_serializes_end_user_metadata_separately() -> None:
    snapshot = _snapshot().model_copy(update={"end_user_id": "end-user"})

    artifact = serialize_trajectory(build_scope([snapshot])[0].node)
    payload = json.loads(artifact.payload)

    assert payload["metadata"]["user_id"] == "no_user_id"
    assert payload["metadata"]["end_user_id"] == "end-user"


def test_build_scope_derived_fields_survive_sorted_json_round_trip() -> None:
    snapshot = Snapshot(
        source_path="mismatch.json",
        source_sha256="a" * 64,
        session_id="session-mismatch",
        thread_id="thread-mismatch",
        provider="openai",
        operation="responses",
        outcome="success",
        history=[Message(role="user", content="lookup")],
        response=[
            Message(
                role="assistant",
                content="",
                reasoning_content="checking",
                tool_calls=[
                    ToolCall(
                        id="call-1",
                        function=FunctionCall(
                            name="lookup",
                            arguments={"zeta": 1, "alpha": 2},
                        ),
                    )
                ],
            ),
            Message(
                role="tool",
                content="matched",
                tool_call_id="call-1",
                name="lookup",
            ),
            Message(role="assistant", content="done", reasoning_content=""),
        ],
        tools=[
            ToolDefinition(
                name="lookup",
                parameters={
                    "type": "object",
                    "properties": {
                        "alpha": {"type": "string"},
                        "zeta": {"type": "string"},
                    },
                },
            )
        ],
        captured_at="2026-10-15T00:00:00Z",
        wire_complete=True,
    )

    built = build_scope([snapshot])
    assert len(built) == 1
    node = built[0].node
    assert node.tool_call_check.mismatches[0].type_mismatch is not None
    assert [
        mismatch.arg for mismatch in node.tool_call_check.mismatches[0].type_mismatch
    ] == ["alpha", "zeta"]

    artifact = serialize_trajectory(node, built[0].origins)
    restored = parse_trajectory_record(json.loads(artifact.payload))
    validate_derived_fields(restored)
    assert restored.metadata.sub_session_id == 0
    assert restored.tool_call_check.mismatches[0].type_mismatch is not None
    assert [
        mismatch.arg
        for mismatch in restored.tool_call_check.mismatches[0].type_mismatch
    ] == ["alpha", "zeta"]


def test_build_scope_is_empty_for_no_snapshots() -> None:
    assert build_scope([]) == ()


def _duplicate_capture(
    source_ref: str, *, captured_at: str, thread: str, session: str = "s"
) -> Snapshot:
    return Snapshot(
        source_path=source_ref,
        source_sha256=("a" if thread == "first" else "b") * 64,
        session_id=session,
        thread_id=thread,
        provider="openai",
        operation="responses",
        outcome="success",
        user_id="u",
        captured_at=captured_at,
        history=[user("question")],
        response=[assistant("answer")],
        wire_complete=True,
    )


def _serial_artifacts(captures, tmp_path):
    """Use the independent SQLite-backed builder as the lineage reference."""
    with StateStore(tmp_path / "baseline.sqlite", ephemeral=True) as state:
        for capture in captures:
            state.put_snapshot(capture)
        stats = PipelineStats()
        _build_trajectories(state, stats, build_workers=1)
        assert stats.skipped_inputs == 0
        return [
            serialize_trajectory(node, origins)
            for _identifier, node, origins in state.iter_trajectories()
        ]


@pytest.mark.parametrize("quarantined", [False, True], ids=["pass", "quarantined"])
def test_origin_audit_matches_serial_materialized_trajectory(tmp_path, quarantined):
    capture = _duplicate_capture(
        "user/req.json", captured_at="2026-10-08T00:00:01Z", thread="first"
    )
    if quarantined:
        capture.issues = [AuditIssue(code="synthetic_error", stage="provider")]

    built = build_scope([capture])[0]
    audit = built.node.normalization_audit
    assert audit is not None
    assert audit.tag == (AuditTag.QUARANTINED if quarantined else AuditTag.PASS)
    assert built.origins == (
        {
            "source_ref": capture.source_path,
            "sha256": capture.source_sha256,
            "captured_at": capture.captured_at,
            "disposition": audit.tag.value,
            "reason_codes": ["synthetic_error"] if quarantined else [],
        },
    )
    assert built.origins[0]["reason_codes"] is not audit.reason_codes
    assert [serialize_trajectory(built.node, built.origins)] == _serial_artifacts(
        [capture], tmp_path
    )


@pytest.mark.parametrize("dirty_is_earlier", [False, True])
@pytest.mark.parametrize("reverse_input", [False, True])
def test_deduplicated_origins_retain_their_own_audit_and_match_serial(
    tmp_path, dirty_is_earlier, reverse_input
):
    dirty = _duplicate_capture(
        "a/dirty.json",
        captured_at=(
            "2026-10-08T00:00:01Z" if dirty_is_earlier else "2026-10-08T00:00:03Z"
        ),
        thread="first",
    )
    dirty.issues = [AuditIssue(code="synthetic_error", stage="provider")]
    clean = _duplicate_capture(
        "z/clean.json", captured_at="2026-10-08T00:00:02Z", thread="second"
    )
    captures = [clean, dirty] if reverse_input else [dirty, clean]
    outputs = build_scope(captures)
    assert len(outputs) == 1
    built = outputs[0]
    assert built.node.normalization_audit.tag == AuditTag.PASS
    assert built.node.metadata.source_file == "clean.json"
    origins = {origin["source_ref"]: origin for origin in built.origins}
    assert origins[dirty.source_path]["disposition"] == "quarantined"
    assert origins[dirty.source_path]["reason_codes"] == ["synthetic_error"]
    assert origins[clean.source_path]["disposition"] == "pass"
    assert origins[clean.source_path]["reason_codes"] == []
    assert [serialize_trajectory(built.node, built.origins)] == _serial_artifacts(
        captures, tmp_path
    )


@pytest.mark.parametrize("session", ["s", ""])
def test_final_identity_deduplicates_threads_before_branch_numbering(session) -> None:
    captures = [
        _duplicate_capture(
            "a-later.json",
            captured_at="2026-10-08T00:00:03Z",
            thread="first",
            session=session,
        ),
        _duplicate_capture(
            "z-earlier.json",
            captured_at="2026-10-08T00:00:01Z",
            thread="second",
            session=session,
        ),
        _duplicate_capture(
            "distinct.json",
            captured_at="2026-10-08T00:00:02Z",
            thread="third",
            session=session,
        ).model_copy(update={"response": [assistant("different answer")]}),
    ]

    def artifacts(items):
        return sorted(
            (
                serialize_trajectory(item.node, item.origins)
                for item in build_scope(items)
            ),
            key=lambda item: item.trajectory_id,
        )

    outputs = artifacts(captures)
    assert len(outputs) == 2
    assert outputs == artifacts(reversed(captures))
    assert len({item.trajectory_id for item in outputs}) == 2
    duplicate = next(item for item in outputs if len(item.origins) == 2)
    assert duplicate.representative == "z-earlier.json"
    assert [item["source_ref"] for item in duplicate.origins] == [
        "z-earlier.json",
        "a-later.json",
    ]
    nodes = [parse_trajectory_record(json.loads(item.payload)) for item in outputs]
    for node in nodes:
        validate_derived_fields(node)
    assert sorted(node.metadata.sub_session_id for node in nodes) == (
        [0, 1] if session else [0, 0]
    )
    if session:
        assert duplicate.filename == "s_sub_0.jsonl"
        assert {item.filename for item in outputs} == {"s_sub_0.jsonl", "s_sub_1.jsonl"}
    else:
        assert len({item.filename for item in outputs}) == 2


def test_final_identity_representative_prefers_pass_over_earlier_quarantine() -> None:
    dirty = _duplicate_capture(
        "a/dirty.json", captured_at="2026-10-08T00:00:01Z", thread="first"
    ).model_copy(
        update={"issues": [AuditIssue(code="synthetic_error", stage="provider")]}
    )
    clean = _duplicate_capture(
        "z/clean.json", captured_at="2026-10-08T00:00:02Z", thread="second"
    )
    assert build_scope([dirty])[0].node.normalization_audit.tag == AuditTag.QUARANTINED
    assert build_scope([clean])[0].node.normalization_audit.tag == AuditTag.PASS
    outputs = build_scope([dirty, clean])
    assert len(outputs) == 1
    assert outputs == build_scope([clean, dirty])
    assert outputs[0].node.metadata.source_file == "clean.json"
    assert outputs[0].node.normalization_audit.tag == AuditTag.PASS
    assert {item["source_ref"] for item in outputs[0].origins} == {
        "a/dirty.json",
        "z/clean.json",
    }
    validate_derived_fields(outputs[0].node)


def test_final_identity_representative_tiebreak_uses_full_source_path() -> None:
    captures = [
        _duplicate_capture(name, captured_at="2026-10-08T00:00:01Z", thread=thread)
        for name, thread in (("z/a.json", "first"), ("a/z.json", "second"))
    ]
    outputs = build_scope(captures)
    assert len(outputs) == 1
    assert outputs == build_scope(reversed(captures))
    assert outputs[0].node.metadata.source_file == "z.json"
    assert [item["source_ref"] for item in outputs[0].origins] == [
        "a/z.json",
        "z/a.json",
    ]


def test_sessionless_prefixes_merge_by_user_and_outputs_remain_distinct() -> None:
    rows = []
    for name, user_id, session_id, extended in (
        ("alice-short", "alice", "", False),
        ("alice-long", "alice", "no_session_id", True),
        ("bob-short", "bob", "no_session_id", False),
        ("real-session", "alice", "real-session", False),
    ):
        envelope = _tokenplan_envelope(session_id=session_id)
        envelope["metadata"]["source_user_id"] = user_id
        if extended:
            envelope["client_request"]["capture"]["body"]["messages"].extend(
                [
                    {"role": "assistant", "content": "done"},
                    {"role": "user", "content": "next"},
                ]
            )
        rows.append(
            parse_payload(
                f"{name}.json", json.dumps(envelope).encode(), input_format="tokenplan"
            )
        )
    grouped = {}
    for row in rows:
        grouped.setdefault((row.scope_kind, row.scope_id), []).append(
            Snapshot.model_validate_json(row.snapshot_json)
        )
    assert set(grouped) == {
        aggregation_scope("", "alice", rows[0].captured_at),
        aggregation_scope("", "bob", rows[0].captured_at),
        ("session", "real-session"),
    }
    artifacts = []
    for key, captures in grouped.items():
        built = build_scope(captures)
        repeated = build_scope(reversed(captures))
        assert len(built) == len(repeated) == 1
        artifact = serialize_trajectory(built[0].node, built[0].origins)
        assert artifact == serialize_trajectory(repeated[0].node, repeated[0].origins)
        validate_derived_fields(parse_trajectory_record(json.loads(artifact.payload)))
        artifacts.append(artifact)
        if key[0] == "user":
            assert built[0].node.metadata.session_id == "no_session_id"
            assert built[0].node.metadata.sub_session_id == 0
            assert (
                artifact.filename
                == f"no_session_id_{artifact.trajectory_id}_sub_0.jsonl"
            )
        if key == aggregation_scope("", "alice", rows[0].captured_at):
            assert {origin["source_ref"] for origin in built[0].origins} == {
                "alice-short.json",
                "alice-long.json",
            }
    assert len({artifact.filename for artifact in artifacts}) == 3


@pytest.mark.parametrize("mounted_child", [False, True], ids=["root", "subagent"])
@pytest.mark.parametrize(
    "call_id",
    [
        "password=fake",
        "Authorization: Bearer fake",
        "password=[REDACTED]",
        "x" * 1000 + " password=fake; " + "x" * 200,
    ],
    ids=["password", "authorization", "redacted", "truncated"],
)
def test_spark_derived_audit_survives_json_and_output_round_trips(
    call_id, mounted_child
) -> None:
    affected = snapshot(
        "affected",
        thread="child" if mounted_child else "main",
        turn="child-turn" if mounted_child else "turn-1",
        history=[user("lookup a value")],
        response=[
            assistant(
                calls=[
                    ToolCall(
                        id=call_id, function=FunctionCall(name="lookup", arguments={})
                    )
                ]
            )
        ],
        parent_thread="main" if mounted_child else "",
        parent_turn="turn-1" if mounted_child else "",
        forked_from="main" if mounted_child else "",
        marker='{"spawn_call_id":"spawn-1","task_name":"child"}'
        if mounted_child
        else "",
    ).model_copy(update={"tools": [ToolDefinition(name="lookup")]})
    captures = [affected]
    if mounted_child:
        spawn_message = assistant(calls=[spawn("spawn-1", "child")])
        event = snapshot(
            "parent-event",
            thread="main",
            turn="turn-1",
            history=[user("delegate")],
            response=[spawn_message],
        )
        leaf = snapshot(
            "parent-leaf",
            thread="main",
            turn="turn-2",
            history=[
                user("delegate"),
                spawn_message,
                Message(
                    role="tool",
                    tool_call_id="spawn-1",
                    name="spawn_agent",
                    content="started",
                ),
            ],
            response=[assistant("done")],
        )
        captures.extend([event, leaf])
    # The shuffle uses JSON-mode strict validation, preserving enum instances.
    captures = [
        Snapshot.model_validate_json(item.model_dump_json()) for item in captures
    ]
    built = build_scope(captures)
    assert len(built) == 1
    node = built[0].node
    validate_derived_fields(node)
    restored = TrajectoryNode.model_validate_json(node.model_dump_json())
    assert restored == node
    validate_derived_fields(restored)
    affected_node = (
        restored.sub_agent_trajectory["spawn-1"] if mounted_child else restored
    )
    assert affected_node.messages[-1].tool_calls[0].id == call_id
    assert "missing_tool_result" in affected_node.normalization_audit.reason_codes
    assert restored.normalization_audit.tag == AuditTag.QUARANTINED
    if mounted_child:
        assert "subagent_quality_failure" in restored.normalization_audit.reason_codes
    artifact = serialize_trajectory(restored, built[0].origins)
    published = parse_trajectory_record(json.loads(artifact.payload))
    validate_derived_fields(published)
    assert serialize_trajectory(published, built[0].origins) == artifact
    stale = restored.model_copy(update={"total_rounds": restored.total_rounds + 1})
    with pytest.raises(StaleDerivedFieldsError):
        serialize_trajectory(stale, built[0].origins)
