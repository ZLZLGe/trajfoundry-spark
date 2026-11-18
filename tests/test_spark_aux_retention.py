"""Explicit S3 auxiliary retention preserves publication checks, not garbage."""

from __future__ import annotations

import hashlib
import io
import json
import re
from dataclasses import replace
from functools import partial

import pytest
from botocore.exceptions import ClientError

from trajfoundry.spark import job
from trajfoundry.spark.config import SparkNormalizeConfig
from trajfoundry.validation import validate_output

from . import test_spark_job
from .test_spark_adapters import _tokenplan_envelope
from .test_spark_build import _snapshot

local_spark = test_spark_job.local_spark


class NoDeleteObjectStore(test_spark_job.NoDiagnosticObjectStore):
    """Shared fake S3 allows read/write but never grants DeleteObject."""

    def __init__(self, root, *_args, corrupt_kind=None, **_kwargs):
        super().__init__(root)
        self.corrupt_kind = corrupt_kind

    def delete_object(self, *, Bucket, Key):
        (self.root / "delete-attempted").touch()
        raise ClientError(
            {
                "ResponseMetadata": {"HTTPStatusCode": 403},
                "Error": {"Code": "AccessDenied"},
            },
            "DeleteObject",
        )

    def get_object(self, *, Bucket, Key):
        response = super().get_object(Bucket=Bucket, Key=Key)
        marker = self.root / "read-evidence" / hashlib.sha256(Key.encode()).hexdigest()
        marker.parent.mkdir(exist_ok=True)
        marker.touch()
        selected = self.corrupt_kind and (
            (self.corrupt_kind == "trajectory" and "_sub_" in Key)
            or (
                self.corrupt_kind.endswith("_part")
                and f"/._{self.corrupt_kind}_" in Key
            )
            or Key.endswith("/" + self.corrupt_kind)
        )
        if selected:
            response["Body"].close()
            return {"Body": io.BytesIO(b"corrupted")}
        return response


def _config(**kwargs):
    return SparkNormalizeConfig(
        "s3://fake/in/",
        "s3://fake/out/",
        prefix_shards=("alice/", "bob/"),
        retain_s3_auxiliary=True,
        **kwargs,
    )


def _records():
    return [
        ("alice/req.json", json.dumps(_tokenplan_envelope()).encode()),
        ("bob/req_bad.json", b"not-json"),
    ]


@pytest.mark.parametrize("value", [None, 0, 1, "true", [], {}])
def test_retention_config_requires_boolean(value):
    with pytest.raises(ValueError, match="must be a boolean"):
        SparkNormalizeConfig(
            "s3://fake/in/",
            "s3://fake/out/",
            prefix_shards=("user/",),
            retain_s3_auxiliary=value,
        )


def test_retention_config_only_supports_s3_and_changes_config_hash(tmp_path):
    with pytest.raises(ValueError, match="only supported for S3"):
        replace(_config(), output_uri=str(tmp_path / "local"))
    assert job._config_hash(_config()) != job._config_hash(
        replace(_config(), retain_s3_auxiliary=False)
    )


@pytest.mark.parametrize("run_id", [None, "short", "A" * 32, "g" * 32, "a" * 33])
def test_retained_partition_names_require_exact_private_run_identity(tmp_path, run_id):
    with pytest.raises(ValueError, match="32-character lowercase hex run_id"):
        list(
            job._publish_partition(
                0,
                iter(()),
                output_uri="s3://fake/out/",
                endpoint_url=None,
                region_name="unused",
                retain_s3_auxiliary=True,
                run_id=run_id,
                client_factory=partial(NoDeleteObjectStore, str(tmp_path)),
            )
        )
    assert list(tmp_path.iterdir()) == []


def test_retained_partition_names_are_retry_stable_and_run_isolated(tmp_path):
    built = job.build_scope([_snapshot()])[0]

    def publish(run_id):
        return next(
            job._publish_partition(
                3,
                iter([("trajectory", built)]),
                output_uri="s3://fake/out/",
                endpoint_url=None,
                region_name="unused",
                retain_s3_auxiliary=True,
                run_id=run_id,
                client_factory=partial(NoDeleteObjectStore, str(tmp_path)),
            )
        )

    first, retry, next_run = publish("a" * 32), publish("a" * 32), publish("b" * 32)
    for kind in ("objects", "lineage"):
        entry = first[f"{kind}_part"]
        assert entry == retry[f"{kind}_part"]
        assert entry["path"] == (f"diagnostics/._{kind}_part_{'a' * 32}_000003.ndjson")
        assert next_run[f"{kind}_part"]["path"] != entry["path"]
        assert next_run[f"{kind}_part"]["sha256"] == entry["sha256"]
    assert not (tmp_path / "delete-attempted").exists()


def test_local_spark_retains_verified_aux_without_delete_and_manifest_is_final_only(
    local_spark, tmp_path
):
    output = tmp_path / "fake" / "out"
    output.mkdir(parents=True)
    old_probe = output / (".trajfoundry-access-probe-" + "c" * 32)
    old_part = (
        output / "diagnostics" / ("._objects_part_" + "d" * 32 + "_000004.ndjson")
    )
    old_part.parent.mkdir()
    old_probe.write_bytes(b"previous authorized probe")
    old_part.write_bytes(b"unrelated previous-run auxiliary bytes")
    result = job.run_spark_job(
        _config(),
        spark=local_spark,
        source_reader=partial(test_spark_job.synthetic_reader, records=_records()),
        client_factory=partial(NoDeleteObjectStore, str(tmp_path)),
    )
    assert old_probe.read_bytes() == b"previous authorized probe"
    assert old_part.read_bytes() == b"unrelated previous-run auxiliary bytes"
    parts = [
        part
        for part in (output / "diagnostics").glob("._*_part_*.ndjson")
        if part != old_part
    ]
    assert parts and result["retained_auxiliary_objects"] == len(parts)
    assert result["auxiliary_retention_mode"] == "retain"
    assert result["retention_scope"] == "current_run"
    assert not (tmp_path / "delete-attempted").exists()
    identities = set()
    for part in parts:
        match = re.fullmatch(
            r"\._(?:objects|lineage)_part_([0-9a-f]{32})_[0-9]{6}\.ndjson",
            part.name,
        )
        assert match
        identities.add(match.group(1))
        key = "out/diagnostics/" + part.name
        assert (
            tmp_path / "read-evidence" / hashlib.sha256(key.encode()).hexdigest()
        ).exists()
    assert len(identities) == 1
    old_key = "out/diagnostics/" + old_part.name
    assert not (
        tmp_path / "read-evidence" / hashlib.sha256(old_key.encode()).hexdigest()
    ).exists()
    manifest_raw = (output / "manifest.json").read_bytes()
    assert hashlib.sha256(manifest_raw).hexdigest() == result["manifest_sha256"]
    manifest = json.loads(manifest_raw)
    assert {entry["path"] for entry in manifest["files"]} == {
        "session-1_sub_0.jsonl",
        "lineage.jsonl",
    }
    for entry in manifest["files"]:
        payload = (output / entry["path"]).read_bytes()
        assert len(payload) == entry["bytes"]
        assert hashlib.sha256(payload).hexdigest() == entry["sha256"]
    assert result["diagnostics"] is None
    assert not (output / "diagnostics" / "input_diagnostics.ndjson").exists()
    assert manifest["counts"]["skip_reason_counts"] == {"parse_failure": 1}
    assert result["input_files"] == 2
    assert result["parse_failures"] == 1
    assert result["trajectories"] == result["verified_trajectory_objects"] == 1
    assert result["output_objects"] == 3
    # The public manifest, not auxiliary directory contents, defines the dataset.
    report = validate_output(output)
    assert report.valid, report.errors


@pytest.mark.parametrize(
    "corrupt_kind",
    [
        "trajectory",
        "objects_part",
        "lineage_part",
        "lineage.jsonl",
    ],
)
def test_retention_never_suppresses_byte_verification_failures(
    local_spark, tmp_path, corrupt_kind
):
    with pytest.raises(Exception, match="hash or size"):
        job.run_spark_job(
            _config(),
            spark=local_spark,
            source_reader=partial(test_spark_job.synthetic_reader, records=_records()),
            client_factory=partial(
                NoDeleteObjectStore, str(tmp_path), corrupt_kind=corrupt_kind
            ),
        )
    assert not (tmp_path / "fake" / "out" / "manifest.json").exists()
    assert not (tmp_path / "delete-attempted").exists()


def test_retention_rejects_unrecognized_preexisting_output(local_spark, tmp_path):
    output = tmp_path / "fake" / "out"
    output.mkdir(parents=True)
    old = output / "unknown.json"
    old.write_bytes(b"must remain untouched")
    with pytest.raises(ValueError, match="not empty"):
        job.run_spark_job(
            _config(),
            spark=local_spark,
            source_reader=partial(test_spark_job.synthetic_reader, records=_records()),
            client_factory=partial(NoDeleteObjectStore, str(tmp_path)),
        )
    assert old.read_bytes() == b"must remain untouched"
    assert list(output.iterdir()) == [old]
