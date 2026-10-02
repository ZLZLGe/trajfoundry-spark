from __future__ import annotations

import json

from trajfoundry.models import Snapshot
from trajfoundry.spark.adapters import parse_payload
from trajfoundry.spark.build import build_scope

from .test_spark_adapters import _tokenplan_envelope


def _snapshot(source_ref: str = "req.json") -> Snapshot:
    row = parse_payload(
        source_ref,
        json.dumps(_tokenplan_envelope()).encode(),
        input_format="tokenplan",
    )
    return Snapshot.model_validate(json.loads(row.snapshot_json))


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


def test_build_scope_is_empty_for_no_snapshots() -> None:
    assert build_scope([]) == ()
