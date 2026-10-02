from __future__ import annotations

import json

import pytest

from trajfoundry.models import Message, Metadata, TrajectoryNode
from trajfoundry.quality import enrich_trajectory
from trajfoundry.spark.output import (
    ManifestAccumulator,
    SparkObjectDescription,
    lineage_line,
    serialize_trajectory,
)


def _trajectory() -> TrajectoryNode:
    return enrich_trajectory(
        TrajectoryNode(
            messages=[
                Message(role="user", content="hello"),
                Message(role="assistant", content="done", reasoning_content=""),
            ],
            tools=[],
            source="req_001.json",
            metadata=Metadata(
                source_file="req_001.json",
                source_name="tokenplan",
                specific_source="token-plan",
                session_id="session-1",
                sub_session_id=0,
            ),
        )
    )


def test_trajectory_artifact_is_deterministic_and_flat() -> None:
    origins = ({"source_ref": "00/req_001.json", "sha256": "a" * 64},)

    first = serialize_trajectory(_trajectory(), origins)
    second = serialize_trajectory(_trajectory(), origins)

    assert first == second
    assert first.filename == "session-1_sub_0.jsonl"
    assert first.payload.endswith(b"\n")
    assert json.loads(first.payload)["metadata"]["session_id"] == "session-1"
    assert first.description.path == first.filename
    assert first.description.bytes == len(first.payload)
    assert first.description.sha256
    assert json.loads(lineage_line(first))["origin_count"] == 1


def test_manifest_accumulator_publishes_v4_metadata_and_counts() -> None:
    accumulator = ManifestAccumulator(
        input_root="s3://bucket/masked-raw/v002/dt=2026-09-16/",
        input_format="tokenplan",
        config_hash="b" * 64,
    )

    artifact = accumulator.add_trajectory(
        _trajectory(),
        ({"source_ref": "req_001.json", "sha256": "a" * 64},),
    )
    manifest = json.loads(accumulator.manifest_bytes(created_at="2026-10-02T00:00:00Z"))

    assert artifact.filename in {entry["path"] for entry in manifest["files"]}
    assert manifest["schema_version"] == "trajfoundry-v4"
    assert manifest["input_format"] == "tokenplan"
    assert manifest["created_at"] == "2026-10-02T00:00:00Z"
    assert manifest["counts"]["accepted"] == 1
    assert manifest["files"][0]["sha256"] == artifact.description.sha256


@pytest.mark.parametrize(
    "description",
    [
        ("nested/name.jsonl", "a" * 64, 1),
        ("flat.jsonl", "A" * 64, 1),
        ("flat.jsonl", "a" * 63, 1),
        ("flat.jsonl", "a" * 64, -1),
    ],
)
def test_manifest_object_descriptions_reject_unsafe_values(
    description: tuple[str, str, int],
) -> None:
    with pytest.raises(ValueError):
        SparkObjectDescription(*description)
