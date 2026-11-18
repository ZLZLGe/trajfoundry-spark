"""Arbitrary-precision data survives every disk-backed Spark model boundary."""

from __future__ import annotations

import json
from hashlib import sha256

import pytest

from trajfoundry.models import (
    AgentMessageEvidence,
    CompactionRecord,
    FunctionCall,
    Message,
    ServerToolCall,
    Snapshot,
    ToolCall,
    ToolDefinition,
)
from trajfoundry.spark.bounded import (
    BoundedBuildLimits,
    _Budget,
    _DiskScope,
    build_scope_bounded,
)
from trajfoundry.spark.build import build_scope
from trajfoundry.spark.component_plan import component_key, summarize_snapshot
from trajfoundry.spark.manifest import DiskManifestAccumulator
from trajfoundry.spark.output import serialize_trajectory
from trajfoundry.spark.storage import LocalOutputStore

LARGE_INTEGERS = [2**64, -(2**64), 10**100 + 123, -(10**1000 + 123)]


def _snapshot(value: int, *, routing: bool = False) -> Snapshot:
    nested = {"large": value, "nested": [value, {"opposite": -value}]}
    result = Snapshot(
        source_path="user/no_session_id/req.json",
        source_sha256="a" * 64,
        source_name="tokenplan",
        session_id="no_session_id",
        thread_id="main",
        turn_id="turn",
        user_id="user",
        provider="openai",
        operation="responses",
        outcome="success",
        captured_at="2026-09-15T00:00:00Z",
        history=[Message(role="user", content="question")],
        response=[
            Message(
                role="assistant",
                content="answer",
                reasoning_content="",
                reasoning_details=[nested],
                reasoning=nested,
                tool_calls=[
                    ToolCall(
                        id="lookup-call",
                        function=FunctionCall(name="lookup", arguments=nested),
                    )
                ],
            )
        ],
        tools=[ToolDefinition(name="lookup", parameters={"default": nested})],
        server_tool_calls=[
            ServerToolCall(
                name="web_search",
                id="server-call",
                arguments=nested,
                origin="response",
                result=nested,
            )
        ],
        compaction_items=[
            CompactionRecord(
                origin="history",
                item_index=0,
                item={"type": "compaction", "encrypted_content": "opaque", **nested},
            )
        ],
        wire_complete=True,
    )
    if routing:
        result.agent_messages = [
            AgentMessageEvidence(
                origin="history",
                item_index=0,
                item={
                    "type": "agent_message",
                    "id": "relay",
                    "author": "/root/child",
                    "recipient": "/root",
                    "content": [
                        {"type": "encrypted_content", "encrypted_content": "x"}
                    ],
                    **nested,
                },
                preceding_completed_spawn_call_ids=[],
            )
        ]
    return result


@pytest.mark.parametrize(
    "value", LARGE_INTEGERS, ids=["uint64", "negative", "huge", "beyond-double"]
)
def test_pydantic_disk_snapshot_roundtrip_is_integer_exact(tmp_path, value):
    original = _snapshot(value, routing=True)
    scope = _DiskScope(tmp_path / "scope.sqlite", BoundedBuildLimits())
    try:
        scope.ingest([original])
        actual = scope.snapshot(original.source_path, _Budget(10**7, "test"))
        assert actual == original
        arguments = actual.response[0].tool_calls[0].function.arguments
        assert type(arguments["large"]) is int
        assert arguments["large"] == value
        metadata = scope.snapshot(
            original.source_path, _Budget(10**7, "test"), metadata=True
        )
        assert metadata.history == metadata.response == []
        assert metadata.tools == original.tools
        assert metadata.compaction_items == original.compaction_items
        assert metadata.agent_messages == original.agent_messages
    finally:
        scope.close()


@pytest.mark.parametrize("routing", [False, True], ids=["prefix", "routing"])
@pytest.mark.parametrize(
    "value", LARGE_INTEGERS, ids=["uint64", "negative", "huge", "beyond-double"]
)
def test_memory_disk_and_written_artifacts_preserve_large_integer(
    tmp_path, value, routing
):
    capture = _snapshot(value, routing=routing)
    memory = build_scope([capture])
    disk = tuple(build_scope_bounded([capture], workspace_parent=tmp_path))
    assert len(memory) == len(disk) == 1
    expected = serialize_trajectory(memory[0].node, memory[0].origins)
    actual = serialize_trajectory(disk[0].node, disk[0].origins)
    assert actual == expected
    decoded = json.loads(actual.payload)
    assistant = next(
        message for message in decoded["messages"] if message["role"] == "assistant"
    )
    arguments = assistant["tool_calls"][0]["function"]["arguments"]
    assert type(arguments["large"]) is int
    assert arguments["large"] == value
    assert arguments["nested"][1]["opposite"] == -value
    assert decoded["tools"][0]["parameters"]["default"]["large"] == value
    assert decoded["compaction_items"][0]["item"]["large"] == value
    assert decoded["server_tool_calls"][0]["arguments"]["large"] == value
    if routing:
        assert decoded["agent_messages"][0]["item"]["large"] == value

    store = LocalOutputStore(tmp_path / "output")
    store.initialize_run("bigint-test")
    receipt = store.put_bytes(actual.filename, actual.payload)
    store.verify(receipt)
    written = (tmp_path / "output" / actual.filename).read_bytes()
    assert written == expected.payload
    assert receipt.sha256 == sha256(written).hexdigest()
    store.finish_run()


@pytest.mark.parametrize(
    "value", LARGE_INTEGERS, ids=["uint64", "negative", "huge", "beyond-double"]
)
def test_component_planning_keeps_bigint_capture_and_model_roundtrip_together(value):
    capture = _snapshot(value)
    restored = Snapshot.model_validate_json(capture.model_dump_json())
    summary = summarize_snapshot(capture)
    assert summary == summarize_snapshot(restored)
    assert component_key(capture, summary) == component_key(restored, summary)


def test_disk_manifest_uses_arbitrary_precision_counts(tmp_path):
    with DiskManifestAccumulator(
        tmp_path / "manifest.sqlite",
        input_root="s3://test/input/",
        input_format="tokenplan",
        config_hash="test",
    ) as manifest:
        manifest.stats.reason_counts = {"test": 10**100 + 123}
        manifest.write_manifest(tmp_path / "manifest.json", created_at="test")
    document = json.loads((tmp_path / "manifest.json").read_bytes())
    assert document["counts"]["reason_counts"]["test"] == 10**100 + 123
