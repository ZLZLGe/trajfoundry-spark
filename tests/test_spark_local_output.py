from __future__ import annotations

import hashlib
import json
from functools import partial

import pytest

from trajfoundry.spark import job
from trajfoundry.spark.config import SparkNormalizeConfig
from trajfoundry.spark.storage import LocalOutputStore
from trajfoundry.validation import validate_output

from . import test_spark_job
from .test_spark_adapters import _tokenplan_envelope

local_spark = test_spark_job.local_spark
synthetic_reader = test_spark_job.synthetic_reader


def _forbidden_s3_client(*_args, **_kwargs):
    """Fail in either Driver or Python worker if local publishing creates S3."""

    raise AssertionError("local output must never instantiate an S3 client")


def _output_uri(path, uri_kind):
    return path.as_uri() if uri_kind == "file-uri" else str(path)


def _synthetic_records():
    records = []
    for name, user_id, session_id, extended in (
        ("alice/req_short.json", "alice", "", False),
        ("alice/req_long.json", "alice", "no_session_id", True),
        ("bob/req_short.json", "bob", "no_session_id", False),
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
        records.append((name, json.dumps(envelope).encode()))
    records.append(("bob/req_invalid.json", b"not-json password=synthetic-secret"))
    return records


@pytest.mark.parametrize("uri_kind", ["path", "file-uri"])
def test_config_accepts_explicit_absolute_local_output_without_creating_it(
    tmp_path, uri_kind
):
    output = tmp_path / "fresh output"
    config = SparkNormalizeConfig(
        "s3://fake/in/",
        _output_uri(output, uri_kind),
        prefix_shards=("alice/",),
    )
    assert config.output_uri
    assert not output.exists()


@pytest.mark.parametrize("output_uri", ["relative/output", "./output", "file:relative"])
def test_config_rejects_relative_local_output(output_uri):
    with pytest.raises(ValueError):
        SparkNormalizeConfig("s3://fake/in/", output_uri, prefix_shards=("alice/",))


@pytest.mark.parametrize(
    "output_uri",
    [
        "/",
        "file:///",
        "file://remote-host/share/output",
        "file:///share/output?ignored=true",
        "file:///share/output#ignored",
        "file:///share/%2E%2E/output",
    ],
)
def test_config_rejects_ambiguous_or_unsafe_local_destinations(output_uri):
    with pytest.raises(ValueError):
        SparkNormalizeConfig("s3://fake/in/", output_uri, prefix_shards=("alice/",))


@pytest.mark.parametrize("uri_kind", ["path", "file-uri"])
def test_real_spark_local_output_end_to_end_is_s3_free(local_spark, tmp_path, uri_kind):
    # A space exercises actual file:// decoding rather than string slicing.
    output = tmp_path / "fresh output"
    records = _synthetic_records()
    config = SparkNormalizeConfig(
        "s3://fake/in/",
        _output_uri(output, uri_kind),
        prefix_shards=("alice/", "bob/"),
    )
    result = job.run_spark_job(
        config,
        spark=local_spark,
        source_reader=partial(synthetic_reader, records=records),
        client_factory=_forbidden_s3_client,
    )

    assert result["input_files"] == 4
    assert result["parse_failures"] == 1
    assert result["trajectories"] == 2
    assert result["shuffle_partitions"] == 2
    report = validate_output(output)
    assert report.valid, report.errors

    manifest_bytes = (output / "manifest.json").read_bytes()
    assert hashlib.sha256(manifest_bytes).hexdigest() == result["manifest_sha256"]
    manifest = json.loads(manifest_bytes)
    assert manifest["schema_version"] == "trajfoundry-v4"
    assert manifest["input_root"] == "s3://fake/in/"
    assert manifest["counts"]["input_files"] == 4
    assert manifest["counts"]["skipped_inputs"] == 1
    assert manifest["counts"]["skip_reason_counts"] == {"parse_failure": 1}
    assert len(manifest["files"]) == 3
    for entry in manifest["files"]:
        payload = (output / entry["path"]).read_bytes()
        assert len(payload) == entry["bytes"]
        assert hashlib.sha256(payload).hexdigest() == entry["sha256"]

    lineage = [
        json.loads(line)
        for line in (output / "lineage.jsonl").read_bytes().splitlines()
    ]
    assert sorted(item["origin_count"] for item in lineage) == [1, 2]
    assert len({item["trajectory_id"] for item in lineage}) == 2
    assert {origin["source_ref"] for item in lineage for origin in item["origins"]} == {
        name for name, _payload in records[:-1]
    }
    assert len(list(output.glob("no_session_id_*.jsonl"))) == 2

    diagnostics = result["diagnostics"]
    diagnostic_bytes = (output / diagnostics["path"]).read_bytes()
    assert len(diagnostic_bytes) == diagnostics["bytes"]
    assert hashlib.sha256(diagnostic_bytes).hexdigest() == diagnostics["sha256"]
    assert b"synthetic-secret" not in diagnostic_bytes
    diagnostic = json.loads(diagnostic_bytes)
    assert diagnostic["source_ref"] == "bob/req_invalid.json"
    assert diagnostic["reason"] == "parse_failure"
    assert diagnostic["sha256"] == hashlib.sha256(records[-1][1]).hexdigest()
    assert not list(output.glob("._*part*"))


@pytest.mark.parametrize("uri_kind", ["path", "file-uri"])
def test_real_spark_nonempty_local_output_is_preserved(local_spark, tmp_path, uri_kind):
    output = tmp_path / "existing-output"
    output.mkdir()
    (output / "manifest.json").write_bytes(b"existing successful manifest")
    nested = output / "retained"
    nested.mkdir()
    (nested / "old.jsonl").write_bytes(b"existing user content\n")
    before = {
        path.relative_to(output): path.read_bytes()
        for path in output.rglob("*")
        if path.is_file()
    }
    config = SparkNormalizeConfig(
        "s3://fake/in/",
        _output_uri(output, uri_kind),
        prefix_shards=("alice/",),
    )
    with pytest.raises(ValueError, match="not empty|fresh|exclusive"):
        job.run_spark_job(
            config,
            spark=local_spark,
            source_reader=partial(synthetic_reader, records=_synthetic_records()),
            client_factory=_forbidden_s3_client,
        )
    assert {
        path.relative_to(output): path.read_bytes()
        for path in output.rglob("*")
        if path.is_file()
    } == before


def test_real_spark_local_descriptor_limit_never_publishes_manifest(
    local_spark, tmp_path
):
    output = tmp_path / "limited-output"
    records = [
        (
            f"user/req_{index}.json",
            json.dumps(_tokenplan_envelope(session_id=f"session-{index}")).encode(),
        )
        for index in range(2)
    ]
    config = SparkNormalizeConfig(
        "s3://fake/in/",
        str(output),
        prefix_shards=("user/",),
        max_output_objects=1,
    )
    with pytest.raises(RuntimeError, match="driver output descriptor limit"):
        job.run_spark_job(
            config,
            spark=local_spark,
            source_reader=partial(synthetic_reader, records=records),
            client_factory=_forbidden_s3_client,
        )
    assert not (output / "manifest.json").exists()


def test_real_spark_empty_local_input_never_publishes_manifest(local_spark, tmp_path):
    output = tmp_path / "no-input-output"
    config = SparkNormalizeConfig(
        "s3://fake/in/", str(output), prefix_shards=("unmatched/",)
    )
    with pytest.raises(RuntimeError, match="no input objects"):
        job.run_spark_job(
            config,
            spark=local_spark,
            source_reader=partial(synthetic_reader, records=[]),
            client_factory=_forbidden_s3_client,
        )
    assert not (output / "manifest.json").exists()


def test_real_spark_local_part_cleanup_failure_never_publishes_manifest(
    local_spark, tmp_path, monkeypatch
):
    output = tmp_path / "cleanup-failure-output"
    records = [("alice/req_valid.json", json.dumps(_tokenplan_envelope()).encode())]
    config = SparkNormalizeConfig(
        "s3://fake/in/", str(output), prefix_shards=("alice/",)
    )

    def fail_cleanup(self, name):
        raise OSError("synthetic part cleanup failure")

    monkeypatch.setattr(LocalOutputStore, "delete", fail_cleanup)
    with pytest.raises(OSError, match="synthetic part cleanup failure"):
        job.run_spark_job(
            config,
            spark=local_spark,
            source_reader=partial(synthetic_reader, records=records),
            client_factory=_forbidden_s3_client,
        )

    # The trajectory and merged lineage exist, but cleanup must complete
    # before any successful manifest becomes observable.
    assert (output / "session-1_sub_0.jsonl").is_file()
    assert (output / "lineage.jsonl").is_file()
    assert not (output / "manifest.json").exists()
