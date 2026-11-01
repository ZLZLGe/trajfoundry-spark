from __future__ import annotations

import hashlib
import json

import pytest

from trajfoundry.sources.tokenplan import TokenPlanError
from trajfoundry.spark.adapters import parse_payload


def _tokenplan_envelope(*, session_id: str = "session-1") -> dict:
    return {
        "client_request": {
            "path": "/v1/messages",
            "protocol": "anthropic_messages",
            "stream": True,
            "capture": {
                "body": {
                    "model": "claude-test",
                    "stream": True,
                    "messages": [{"role": "user", "content": "hello"}],
                },
                "status": "COMPLETE",
            },
            "media": [],
        },
        "client_response": {
            "capture": {
                "body": {
                    "type": "message",
                    "role": "assistant",
                    "model": "claude-test",
                    "content": [{"type": "text", "text": "done"}],
                },
                "format": "ANTHROPIC_MESSAGES_NORMALIZED",
                "status": "COMPLETE",
            },
            "media": [],
        },
        "metadata": {
            "request_id": "request-1",
            "session_id": session_id,
            "task_id": "task-1",
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


def test_parse_payload_returns_one_bounded_shuffle_record() -> None:
    payload = json.dumps(_tokenplan_envelope()).encode()

    row = parse_payload(
        "00/req_001.json",
        payload,
        input_format="tokenplan",
    )

    assert row.source_ref == "00/req_001.json"
    assert row.sha256 == hashlib.sha256(payload).hexdigest()
    assert row.scope_kind == "session"
    assert row.scope_id == "session-1"
    assert row.session_id == "session-1"
    assert row.endpoint == "/v1/messages"
    assert row.captured_at == "1970-01-01T00:00:01.234Z"
    assert row.snapshot_json.startswith(b"{")
    assert json.loads(row.snapshot_json)["session_id"] == "session-1"


def test_parse_payload_is_deterministic_for_same_source_bytes() -> None:
    payload = json.dumps(
        _tokenplan_envelope(session_id="session-deterministic")
    ).encode()

    first = parse_payload("req.json", payload, input_format="tokenplan")
    second = parse_payload("req.json", payload, input_format="tokenplan")

    assert first == second


def test_parse_payload_preserves_user_scope_when_session_is_missing() -> None:
    envelope = _tokenplan_envelope(session_id="")
    envelope["client_request"]["capture"]["body"]["metadata"] = {"user_id": "user-7"}

    row = parse_payload(
        "req.json",
        json.dumps(envelope).encode(),
        input_format="tokenplan",
    )

    assert row.scope_kind == "user"
    assert row.scope_id == "user-7"
    assert row.session_id == ""
    assert row.user_id == "user-7"


def test_parse_payload_preserves_end_user_without_changing_source_user_scope() -> None:
    envelope = _tokenplan_envelope(session_id="")
    envelope["metadata"]["source_user_id"] = "source-user"
    envelope["metadata"]["end_user_id"] = "end-user"

    row = parse_payload(
        "req.json",
        json.dumps(envelope).encode(),
        input_format="tokenplan",
    )

    snapshot = json.loads(row.snapshot_json)
    assert (row.scope_kind, row.scope_id) == ("user", "source-user")
    assert row.user_id == "source-user"
    assert snapshot["user_id"] == "source-user"
    assert snapshot["end_user_id"] == "end-user"


@pytest.mark.parametrize("session_id", ["", "no_session_id"])
@pytest.mark.parametrize("user_id", ["alice", "bob", "", "no_user_id"])
def test_session_sentinel_uses_exact_user_shuffle_scope(session_id, user_id) -> None:
    envelope = _tokenplan_envelope(session_id=session_id)
    envelope["metadata"]["source_user_id"] = user_id
    row = parse_payload(
        "not-an-identity/no_session_id/req.json",
        json.dumps(envelope).encode(),
        input_format="tokenplan",
    )
    assert row.scope_kind == "user"
    assert row.scope_id == user_id
    assert row.session_id == ""
    assert json.loads(row.snapshot_json)["session_id"] == ""
    assert row.user_id == user_id


def test_real_request_session_overrides_envelope_sentinel_before_shuffle() -> None:
    envelope = _tokenplan_envelope(session_id="no_session_id")
    envelope["metadata"]["source_user_id"] = "alice"
    envelope["client_request"]["capture"]["body"]["metadata"] = {
        "session_id": "body-real"
    }
    row = parse_payload(
        "no_session_id/req.json",
        json.dumps(envelope).encode(),
        input_format="tokenplan",
    )
    assert (row.scope_kind, row.scope_id) == ("session", "body-real")
    assert row.session_id == "body-real"


def test_parse_payload_exposes_stable_tokenplan_error() -> None:
    with pytest.raises(TokenPlanError) as error:
        parse_payload("broken.json", b"{}", input_format="tokenplan")

    assert error.value.code == "tokenplan_empty_envelope"
