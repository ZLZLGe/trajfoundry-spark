"""Format adapters used by the no-snapshot Spark ingest stage."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from typing import Any, Literal

from ..identity import normalize_session_id
from ..io import decode_capture
from ..json_codec import dumps as json_dumps
from ..pipeline import parse_capture
from ..sources.deepinfra import adapt_deepinfra_envelope
from ..sources.sxf import adapt_sxf_envelope
from ..sources.tokenplan import TokenPlanError, adapt_tokenplan_envelope
from .records import ParsedSnapshotRow

InputFormat = Literal["freerouter", "tokenplan", "sxf", "deepinfra"]


def _scope_fields(snapshot: Any) -> tuple[str, str]:
    session_id = normalize_session_id(snapshot.session_id)
    user_id = snapshot.user_id or ""
    if session_id:
        return "session", session_id
    # Keep missing users distinct from a literal user named "no_user_id".
    # Public placeholders belong in final output, not in shuffle identities.
    return "user", user_id


def parse_payload(
    source_ref: str,
    payload: bytes,
    *,
    input_format: InputFormat,
) -> ParsedSnapshotRow:
    """Parse one source object using the existing TrajFoundry semantics."""

    digest = hashlib.sha256(payload).hexdigest()
    capture = decode_capture(payload, input_format=input_format)
    endpoint_value = capture.get("path")
    endpoint = (
        endpoint_value.split("?", 1)[0].rstrip("/")
        if isinstance(endpoint_value, str)
        else ""
    )
    source_name: InputFormat = "freerouter"
    captured_at = ""
    response_is_normalized_final = False
    multimodal_file_mapping: list[Any] = []

    if input_format == "tokenplan":
        adapted = adapt_tokenplan_envelope(capture)
        capture = adapted.capture
        source_name = adapted.source_name
        endpoint = adapted.endpoint
        captured_at = adapted.captured_at
        response_is_normalized_final = adapted.response_is_normalized_final
        multimodal_file_mapping = adapted.multimodal_file_mapping
    elif input_format == "sxf":
        capture = adapt_sxf_envelope(capture)
        source_name = "sxf"
    elif input_format == "deepinfra":
        outer_request = capture.get("request")
        if isinstance(outer_request, Mapping):
            request_path = outer_request.get("path")
            if isinstance(request_path, str):
                endpoint = request_path.split("?", 1)[0].rstrip("/")
        capture = adapt_deepinfra_envelope(capture)
        source_name = "deepinfra"

    captured_value = capture.get("captured_at")
    if isinstance(captured_value, str):
        captured_at = captured_value
    snapshot = parse_capture(
        capture,
        source_path=source_ref,
        source_sha256=digest,
        source_name=source_name,
        response_is_normalized_final=response_is_normalized_final,
        multimodal_file_mapping=multimodal_file_mapping,
    )
    scope_kind, scope_id = _scope_fields(snapshot)
    return ParsedSnapshotRow(
        source_ref=source_ref,
        sha256=digest,
        scope_kind=scope_kind,
        scope_id=scope_id,
        session_id=normalize_session_id(snapshot.session_id),
        user_id=snapshot.user_id or "",
        thread_id=snapshot.thread_id or "",
        captured_at=captured_at,
        endpoint=endpoint,
        snapshot_json=json_dumps(snapshot.model_dump(mode="json"), sort_keys=True),
    )


__all__ = ["ParsedSnapshotRow", "TokenPlanError", "parse_payload"]
