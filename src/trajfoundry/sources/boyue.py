"""Project Boyue relay archives without inventing transport or session data."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from .deepinfra import (
    _body_identity,
    _clean_request_identity,
    _header_identity,
    _identity_headers,
    _identity_text,
    _redacted_identity_fields,
)


class BoyueError(ValueError):
    """Malformed archive; messages contain schema information only."""


class NonTrajectoryCapture(ValueError):
    """A recognized endpoint that cannot contain a conversational trajectory."""


def exclude_nontrajectory(endpoint: str) -> None:
    if endpoint in {
        "/v1/embeddings",
        "/embeddings",
        "/tokenize",
        "/v1/tokenize",
        "/health",
        "/healthz",
        "/ready",
        "/readyz",
        "/metrics",
        "/models",
        "/v1/models",
        "/v1/messages/count_tokens",
        "/messages/count_tokens",
    } or endpoint.startswith(("/admin/", "/health/")):
        raise NonTrajectoryCapture("non_trajectory_endpoint")


def _object(value: Any, field: str) -> dict:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            raise BoyueError(f"{field} is not a JSON object") from None
    if not isinstance(value, dict):
        raise BoyueError(f"{field} must be an object")
    return value


@dataclass(frozen=True)
class BoyueCapture:
    capture: dict[str, Any]
    response_is_normalized_final: bool
    issues: tuple[str, ...]
    incomplete: bool


def adapt_boyue_envelope(value: object) -> BoyueCapture:
    root = _object(value, "archive")
    request = _object(root.get("request_body"), "request_body")
    metadata = _object(root.get("metadata", {}), "metadata")
    other = _object(metadata.get("other") or {}, "metadata.other")
    endpoint = other.get("request_path", "")
    if not isinstance(endpoint, str):
        raise BoyueError("request_path must be a string")
    endpoint = endpoint.split("?", 1)[0].rstrip("/")
    exclude_nontrajectory(endpoint)
    response = _object(root.get("response_body"), "response_body")
    issues = []
    # Old archives omit request_path. Infer only unambiguous response protocols.
    if not endpoint:
        if "choices" in response:
            endpoint = "/v1/chat/completions"
        elif response.get("object") == "response" or str(
            response.get("type", "")
        ).startswith("response."):
            endpoint = "/v1/responses"
        elif response.get("type") == "message" or (
            response.get("role") == "assistant"
            and isinstance(response.get("content"), list)
        ):
            endpoint = "/v1/messages"
        else:
            raise BoyueError("missing path and ambiguous response protocol")
        issues.append("boyue_endpoint_inferred")
    if endpoint not in {
        "/v1/messages",
        "/messages",
        "/v1/responses",
        "/responses",
        "/v1/chat/completions",
        "/chat/completions",
    }:
        raise BoyueError("unsupported request_path")

    incomplete = False
    if endpoint.endswith("/messages") and isinstance(
        response.get("response_text"), str
    ):
        # The archive retained only text. Preserve that text but never invent
        # omitted tool calls, blocks, stop_reason, or a successful outcome.
        response = {
            "model": response.get("model", ""),
            "role": "assistant",
            "content": [{"type": "text", "text": response["response_text"]}],
        }
        issues.append("boyue_response_lossy")
        incomplete = True
    if endpoint.endswith("/responses"):
        event_type = response.get("type")
        if isinstance(event_type, str) and event_type.startswith("response."):
            response = _object(response.get("response"), "response_body.response")
            if event_type != "response.completed":
                incomplete = True
        if response.get("status") != "completed" or not isinstance(
            response.get("output"), list
        ):
            incomplete = True
        if incomplete:
            issues.append("boyue_response_incomplete")
    stream_status = other.get("stream_status")
    if isinstance(stream_status, dict) and stream_status.get("status") not in {
        None,
        "ok",
        "success",
    }:
        incomplete = True
        issues.append("boyue_stream_incomplete")

    headers, header_user = _identity_headers(root.get("user_headers") or {})
    masked = _redacted_identity_fields(request) | _redacted_identity_fields(
        root.get("user_headers")
    )
    request = _clean_request_identity(request)
    timestamp = root.get("created_at")
    if type(timestamp) not in {int, float}:
        raise BoyueError("created_at must be Unix seconds")
    try:
        captured_at = datetime.fromtimestamp(timestamp, UTC).isoformat()
    except (ValueError, OverflowError, OSError):
        raise BoyueError("created_at is outside the supported range") from None
    capture = {
        "path": endpoint,
        "request_body": request,
        "response_body": response,
        "request_headers": headers,
        "captured_at": captured_at,
        "request_id": str(root.get("relay_log_id", "")),
        "user_id": _identity_text(root.get("user_id")) or header_user,
        "is_stream": request.get("stream") is True,
    }
    # No synthetic HTTP 200. Provider parsing explicitly supports archives
    # without HTTP metadata, and the missing evidence is retained as an audit.
    issues.append("boyue_http_status_unavailable")
    for field in ("session_id", "thread_id"):
        identity = _body_identity(request, field) or _header_identity(headers, field)
        if identity:
            capture[field] = identity
    if headers.get("parent_thread_id"):
        capture["parent_thread_id"] = headers["parent_thread_id"]
    if masked:
        capture["_masked_identity_fields"] = sorted(masked)
    return BoyueCapture(capture, True, tuple(issues), incomplete)
