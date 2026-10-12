from __future__ import annotations

import hashlib
import json

import orjson
import pytest

from trajfoundry.models import Snapshot
from trajfoundry.spark.adapters import parse_payload


def _envelope(protocol: str, value: int) -> dict:
    arguments = {"nested": {"values": [value, 7]}}
    parameters = {
        "type": "object",
        "properties": {"nested": {"type": "object", "default": arguments["nested"]}},
    }
    if protocol == "anthropic_messages":
        path = "/v1/messages"
        response_format = "ANTHROPIC_MESSAGES_NORMALIZED"
        request = {
            "model": "claude-test",
            "messages": [{"role": "user", "content": "look up"}],
            "tools": [{"name": "lookup", "input_schema": parameters}],
        }
        response = {
            "type": "message",
            "role": "assistant",
            "model": "claude-test",
            "content": [
                {
                    "type": "tool_use",
                    "id": "call-1",
                    "name": "lookup",
                    "input": arguments,
                }
            ],
            "stop_reason": "tool_use",
        }
    elif protocol == "openai_chat":
        path = "/v1/chat/completions"
        response_format = "OPENAI_CHAT_COMPLETIONS_NORMALIZED"
        request = {
            "model": "gpt-test",
            "messages": [{"role": "user", "content": "look up"}],
            "tools": [
                {
                    "type": "function",
                    "function": {"name": "lookup", "parameters": parameters},
                }
            ],
        }
        response = {
            "id": "response-1",
            "object": "chat.completion",
            "model": "gpt-test",
            "choices": [
                {
                    "index": 0,
                    "finish_reason": "tool_calls",
                    "message": {
                        "role": "assistant",
                        "content": "",
                        "tool_calls": [
                            {
                                "id": "call-1",
                                "type": "function",
                                "function": {
                                    "name": "lookup",
                                    "arguments": json.dumps(arguments),
                                },
                            }
                        ],
                    },
                }
            ],
        }
    else:
        path = "/v1/responses"
        response_format = "OPENAI_RESPONSES_NORMALIZED"
        request = {
            "model": "gpt-test",
            "input": [{"role": "user", "content": "look up"}],
            "tools": [{"type": "function", "name": "lookup", "parameters": parameters}],
        }
        response = {
            "id": "response-1",
            "object": "response",
            "model": "gpt-test",
            "status": "completed",
            "output": [
                {
                    "type": "function_call",
                    "id": "item-1",
                    "call_id": "call-1",
                    "name": "lookup",
                    "arguments": json.dumps(arguments),
                    "status": "completed",
                }
            ],
        }
    return {
        "client_request": {
            "path": path,
            "protocol": protocol,
            "stream": False,
            "capture": {"body": request, "status": "COMPLETE"},
            "media": [],
        },
        "client_response": {
            "capture": {
                "body": response,
                "format": response_format,
                "status": "COMPLETE",
            },
            "media": [],
        },
        "metadata": {
            "request_id": "request-1",
            "session_id": "no_session_id",
            "source_user_id": "user-1",
            "received_at_ms": 1_000,
            "completed_at_ms": 1_234,
            "http_status_code": 200,
            "data_quality": {
                "client_request_complete": True,
                "client_response_complete": True,
                "all_attachments_available": True,
                "truncated": False,
            },
        },
    }


@pytest.mark.parametrize(
    "protocol", ["anthropic_messages", "openai_chat", "openai_responses"]
)
@pytest.mark.parametrize(
    "value",
    [
        2**64 - 1,
        2**64,
        2**80 + 123,
        -(2**63),
        -(2**63) - 1,
        -(2**80) + 123,
        10**400 + 7,
    ],
)
def test_parse_payload_preserves_exact_tool_arguments_and_schema_integers(
    protocol, value
):
    payload = json.dumps(_envelope(protocol, value)).encode()
    row = parse_payload(
        "user-1/no_session_id/req.json", payload, input_format="tokenplan"
    )

    assert row.sha256 == hashlib.sha256(payload).hexdigest()
    assert (row.scope_kind, row.scope_id) == ("user", "user-1")
    assert parse_payload(row.source_ref, payload, input_format="tokenplan") == row
    # Check the wire bytes independently, then the same Snapshot validation
    # used after Spark shuffle. Neither route may turn exact integers into floats.
    wire = json.loads(row.snapshot_json)
    assert (
        wire["response"][0]["tool_calls"][0]["function"]["arguments"]["nested"][
            "values"
        ][0]
        == value
    )
    assert (
        type(
            wire["response"][0]["tool_calls"][0]["function"]["arguments"]["nested"][
                "values"
            ][0]
        )
        is int
    )
    snapshot = Snapshot.model_validate_json(row.snapshot_json)
    actual = (
        snapshot.response[0].tool_calls[0].function.arguments["nested"]["values"][0]
    )
    assert type(actual) is int
    assert actual == value
    default = snapshot.tools[0].parameters["properties"]["nested"]["default"]["values"][
        0
    ]
    assert type(default) is int
    assert default == value
    dumped = snapshot.model_dump(mode="json")
    assert (
        dumped["response"][0]["tool_calls"][0]["function"]["arguments"]["nested"][
            "values"
        ][0]
        == value
    )


@pytest.mark.parametrize(
    "protocol", ["anthropic_messages", "openai_chat", "openai_responses"]
)
def test_parse_payload_keeps_ordinary_snapshot_bytes_compatible(protocol):
    payload = json.dumps(_envelope(protocol, 123)).encode()
    row = parse_payload("req.json", payload, input_format="tokenplan")
    snapshot = Snapshot.model_validate_json(row.snapshot_json)
    assert row.snapshot_json == orjson.dumps(
        snapshot.model_dump(mode="json"), option=orjson.OPT_SORT_KEYS
    )
