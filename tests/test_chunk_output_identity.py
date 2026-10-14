from __future__ import annotations

import json
from hashlib import sha256

import pytest

from trajfoundry.canonical import semantic_payload, trajectory_id
from trajfoundry.export import OutputSet
from trajfoundry.json_codec import dumps
from trajfoundry.output_contract import (
    OutputContractError,
    parse_trajectory_record,
    project_trajectory,
)
from trajfoundry.s3 import S3Location
from trajfoundry.s3_output import S3OutputSet
from trajfoundry.spark.output import serialize_trajectory
from trajfoundry.validation import validate_output, validate_output_backend

from .test_identity_policy import MemoryBackend, _output, _timed_node
from .test_s3_output import FakeS3Client


def _chunked_node(*, mounted=False, chunk="a" * 64):
    node = _timed_node(mounted=mounted)
    pending = [node]
    while pending:
        current = pending.pop()
        current.metadata.session_id = "no_session_id"
        current.metadata.sessionless_chunk_id = chunk
        pending.extend((current.sub_agent_trajectory or {}).values())
    return node


@pytest.mark.parametrize("mounted", [False, True])
def test_chunk_identity_roundtrips_and_separates_equal_transcripts(mounted):
    first = _chunked_node(mounted=mounted)
    second = _chunked_node(mounted=mounted, chunk="b" * 64)
    projected = project_trajectory(first)
    assert projected["metadata"]["sessionless_chunk_id"] == "a" * 64
    assert parse_trajectory_record(projected) == first
    assert semantic_payload(first)["aggregation_identity"]["sessionless_chunk_id"] == "a" * 64
    one = serialize_trajectory(first)
    two = serialize_trajectory(second)
    assert one.filename != two.filename
    assert one.trajectory_id != two.trajectory_id
    assert one.lineage["trajectory_id"] == trajectory_id(first)
    assert one.filename.startswith("no_session_id_")


@pytest.mark.parametrize("real", [False, True])
def test_unplanned_projection_does_not_add_empty_marker(real):
    node = _timed_node(real_root=real)
    assert "sessionless_chunk_id" not in project_trajectory(node)["metadata"]


@pytest.mark.parametrize("value", ["", "a", "A" * 64, "../" + "a" * 61, 12, None, True])
def test_contract_rejects_invalid_chunk_marker(value):
    record = project_trajectory(_chunked_node())
    record["metadata"]["sessionless_chunk_id"] = value
    with pytest.raises(OutputContractError):
        parse_trajectory_record(record)


def test_real_session_cannot_claim_a_sessionless_chunk():
    node = _chunked_node()
    node.metadata.session_id = "real-session"
    with pytest.raises(OutputContractError, match="real session"):
        project_trajectory(node)


def test_mounted_child_cannot_escape_chunk_boundary():
    node = _chunked_node(mounted=True)
    next(iter(node.sub_agent_trajectory.values())).metadata.sessionless_chunk_id = "b" * 64
    with pytest.raises(OutputContractError, match="root trajectory chunk"):
        project_trajectory(node)


@pytest.mark.parametrize("mounted", [False, True])
def test_byte_manifest_validates_chunked_trajectory_and_lineage(mounted):
    manifest, backend = _output(_chunked_node(mounted=mounted))
    manifest["sessionless_partitioning"] = "normalized-bytes-v1"
    result = validate_output_backend(dumps(manifest), backend)
    assert result.valid, result.errors


@pytest.mark.parametrize("declared_policy", [0, 8])
def test_legacy_manifest_cannot_silently_accept_new_chunk_identity(declared_policy):
    manifest, backend = _output(_chunked_node(), declared_policy=declared_policy)
    result = validate_output_backend(dumps(manifest), backend)
    assert not result.valid
    assert any("chunk identity" in error for error in result.errors)


def test_byte_manifest_requires_marker_for_missing_session():
    manifest, backend = _output(_timed_node())
    manifest["sessionless_partitioning"] = "normalized-bytes-v1"
    result = validate_output_backend(dumps(manifest), backend)
    assert not result.valid
    assert any("chunk identity" in error for error in result.errors)


def test_byte_manifest_preserves_real_session_without_marker():
    manifest, backend = _output(_timed_node(real_root=True))
    manifest["sessionless_partitioning"] = "normalized-bytes-v1"
    result = validate_output_backend(dumps(manifest), backend)
    assert result.valid, result.errors


@pytest.mark.parametrize("policy", ["request-count-v1", "", 1, True])
def test_unknown_byte_manifest_policy_is_rejected(policy):
    manifest, backend = _output(_timed_node())
    manifest["sessionless_partitioning"] = policy
    assert not validate_output_backend(dumps(manifest), backend).valid


def test_byte_manifest_cannot_also_declare_time_windows():
    manifest, backend = _output(_chunked_node())
    manifest.update(sessionless_partitioning="normalized-bytes-v1", sessionless_window_hours=8)
    result = validate_output_backend(dumps(manifest), backend)
    assert not result.valid
    assert any("manifest contract" in error for error in result.errors)


def test_chunk_marker_tampering_invalidates_filename_and_lineage():
    manifest, backend = _output(_chunked_node())
    manifest["sessionless_partitioning"] = "normalized-bytes-v1"
    filename = next(name for name in backend.objects if name != "lineage.jsonl")
    record = json.loads(backend.objects[filename])
    record["metadata"]["sessionless_chunk_id"] = "b" * 64
    payload = dumps(record, sort_keys=True) + b"\n"
    backend.objects[filename] = payload
    for entry in manifest["files"]:
        if entry["path"] == filename:
            entry.update(bytes=len(payload), sha256=sha256(payload).hexdigest())
    manifest["total_bytes"] = len(payload)
    result = validate_output_backend(dumps(manifest), backend)
    assert not result.valid
    assert any("filename does not match" in error for error in result.errors)
    assert "lineage trajectory IDs do not match trajectory content" in result.errors


@pytest.mark.parametrize("chunked", [False, True])
def test_generic_export_declares_only_policy_it_actually_receives(tmp_path, chunked):
    node = _chunked_node() if chunked else _timed_node()
    output = OutputSet(tmp_path)
    output.stats.input_files = 1
    output.write_trajectory(node, [{"source_ref": "sample.json", "sha256": "a" * 64}])
    output.close(input_root="/synthetic", config_hash="synthetic")
    manifest = json.loads((tmp_path / "manifest.json").read_bytes())
    assert "sessionless_window_hours" not in manifest
    assert manifest.get("sessionless_partitioning") == (
        "normalized-bytes-v1" if chunked else None
    )
    result = validate_output(tmp_path)
    assert result.valid, result.errors


@pytest.mark.parametrize("chunked_first", [False, True])
def test_generic_export_rejects_mixed_missing_session_policies(tmp_path, chunked_first):
    nodes = [_timed_node(), _chunked_node()]
    if chunked_first:
        nodes.reverse()
    output = OutputSet(tmp_path)
    try:
        output.write_trajectory(nodes[0], [])
        with pytest.raises(ValueError, match="mix chunked and unchunked"):
            output.write_trajectory(nodes[1], [])
    finally:
        output.abort()


@pytest.mark.parametrize("chunked", [False, True])
def test_direct_s3_export_declares_only_received_policy_and_roundtrips(chunked):
    client = FakeS3Client()
    location = S3Location("synthetic", "normalized/")
    output = S3OutputSet(client, location)
    output.stats.input_files = 1
    node = _chunked_node() if chunked else _timed_node()
    output.write_trajectory(node, [{"source_ref": "sample.json", "sha256": "a" * 64}])
    manifest_bytes = output.close(input_root="/synthetic", config_hash="synthetic")
    manifest = json.loads(manifest_bytes)
    assert "sessionless_window_hours" not in manifest
    assert manifest.get("sessionless_partitioning") == (
        "normalized-bytes-v1" if chunked else None
    )
    backend = MemoryBackend({
        entry["path"]: client.objects[(location.bucket, location.key(entry["path"]))]
        for entry in manifest["files"]
    })
    result = validate_output_backend(manifest_bytes, backend)
    assert result.valid, result.errors


@pytest.mark.parametrize("chunked_first", [False, True])
def test_direct_s3_export_rejects_mixed_policies_before_second_upload(chunked_first):
    client = FakeS3Client()
    output = S3OutputSet(client, S3Location("synthetic", "normalized/"))
    nodes = [_timed_node(), _chunked_node()]
    if chunked_first:
        nodes.reverse()
    try:
        output.write_trajectory(nodes[0], [])
        objects = dict(client.objects)
        with pytest.raises(ValueError, match="mix chunked and unchunked"):
            output.write_trajectory(nodes[1], [])
        assert client.objects == objects
    finally:
        output.abort()
