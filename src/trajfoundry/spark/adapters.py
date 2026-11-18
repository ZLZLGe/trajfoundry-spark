"""Format adapters used by the no-snapshot Spark ingest stage."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from typing import Any, Literal

from ..identity import aggregation_scope, normalize_session_id
from ..io import decode_capture
from ..json_codec import dumps as json_dumps
from ..models import AuditIssue, Severity, Snapshot
from ..pipeline import parse_capture
from ..sources.boyue import adapt_boyue_envelope, exclude_nontrajectory
from ..sources.deepinfra import adapt_deepinfra_envelope
from ..sources.sxf import adapt_sxf_envelope
from ..sources.tokenplan import TokenPlanError, adapt_tokenplan_envelope
from .records import ParsedSnapshotRow

InputFormat = Literal["freerouter", "tokenplan", "sxf", "deepinfra", "boyue"]


class SnapshotUnicodeError(ValueError):
    """A capture produced Unicode surrogate code points that JSON cannot encode."""


def _serialize_snapshot(snapshot: Snapshot) -> bytes:
    value = snapshot.model_dump(mode="json")
    try:
        return json_dumps(value, sort_keys=True)
    except TypeError as error:
        # orjson.JSONEncodeError aliases TypeError. Match only its known
        # surrogate error at this boundary; programming faults must propagate.
        if str(error) != "str is not valid UTF-8: surrogates not allowed":
            raise
    except UnicodeEncodeError as error:
        # The arbitrary-precision integer fallback uses Python's UTF-8 encoder.
        if error.encoding != "utf-8" or error.reason != "surrogates not allowed":
            raise
    raise SnapshotUnicodeError("invalid Unicode in snapshot") from None


def _scope_fields(snapshot: Any) -> tuple[str, str]:
    # Parsing establishes only the base session/user identity. The Spark job
    # assigns byte chunks after collecting lightweight size/order summaries.
    # Capture time orders requests; it no longer establishes hour boundaries.
    # Raw user ids keep missing users distinct from a literal "no_user_id".
    return aggregation_scope(
        snapshot.session_id, snapshot.user_id, snapshot.captured_at
    )


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
    boyue = None

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
        exclude_nontrajectory(endpoint)
        capture = adapt_deepinfra_envelope(capture)
        source_name = "deepinfra"
    elif input_format == "boyue":
        boyue = adapt_boyue_envelope(capture)
        capture = boyue.capture
        endpoint = capture["path"]
        source_name = "boyue"
        response_is_normalized_final = boyue.response_is_normalized_final

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
    if boyue is not None:
        for code in boyue.issues:
            snapshot.issues.append(
                AuditIssue(
                    code=code,
                    stage="parsing",
                    severity=Severity.WARNING,
                    path="/response_body",
                    detail="source archive evidence: " + code,
                )
            )
        if boyue.incomplete:
            snapshot.wire_complete = False
            snapshot.outcome = "truncated"
            snapshot.issues.append(
                AuditIssue(
                    code="boyue_incomplete_capture",
                    stage="parsing",
                    severity=Severity.ERROR,
                    path="/response_body",
                    detail="archive does not preserve a complete provider response",
                )
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
        snapshot_json=_serialize_snapshot(snapshot),
    )


__all__ = ["ParsedSnapshotRow", "SnapshotUnicodeError", "TokenPlanError", "parse_payload"]
