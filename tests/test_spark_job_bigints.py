"""Keep arbitrary-precision capture values intact through the entire Spark job."""

from __future__ import annotations

import hashlib
import json
from functools import partial

import pytest

from trajfoundry.models import Snapshot
from trajfoundry.spark import job
from trajfoundry.spark.adapters import parse_payload
from trajfoundry.spark.config import SparkNormalizeConfig
from trajfoundry.spark.storage import LocalOutputStore
from trajfoundry.validation import validate_output

from .test_spark_adapters import _tokenplan_envelope
from .test_spark_build import _serial_artifacts, _snapshot
from .test_spark_job import local_spark, synthetic_reader  # noqa: F401
from .test_spark_local_output import _forbidden_s3_client

BIG_VALUES = [2**80 + 123, -(2**95) + 321, 10**400 + 7]


def _bigint_capture() -> tuple[str, bytes]:
    envelope = _tokenplan_envelope(session_id="bigint-session")
    request = envelope["client_request"]["capture"]["body"]
    request["messages"].extend(
        [
            {
                "role": "assistant",
                "content": [
                    {
                        "type": "tool_use",
                        "id": "bigint-call",
                        "name": "lookup",
                        "input": {"nested": {"values": BIG_VALUES}},
                    }
                ],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "bigint-call",
                        "content": "found",
                    }
                ],
            },
        ]
    )
    request["tools"] = [
        {
            "name": "lookup",
            "description": "Look up exact integer identifiers",
            "input_schema": {
                "type": "object",
                "properties": {
                    "nested": {"type": "object", "default": {"values": BIG_VALUES}}
                },
            },
        }
    ]
    return "user/req_bigint.json", json.dumps(envelope).encode()


@pytest.mark.parametrize("value", [2**64, -(2**63) - 1, *BIG_VALUES])
def test_snapshot_pydantic_json_round_trip_preserves_exact_ints(value):
    # Keep Pydantic's fast JSON path: its decoder already preserves large ints,
    # unlike direct orjson.loads. This also protects strict enum deserialization.
    snapshot = _snapshot()
    snapshot.response[0].reasoning_details = [{"nested": {"values": [value]}}]
    restored = Snapshot.model_validate_json(snapshot.model_dump_json())
    actual = restored.response[0].reasoning_details[0]["nested"]["values"][0]
    assert type(actual) is int
    assert actual == value


def test_nonobject_huge_integer_input_is_a_root_failure_not_overflow():
    payload = json.dumps([10**400 + 7]).encode()
    result = list(
        job._parse_partition([("user/req_array.json", payload, "")], input_format="tokenplan")
    )
    assert result == [
        (
            "error",
            {
                "source_ref": "user/req_array.json",
                "sha256": hashlib.sha256(payload).hexdigest(),
                "reason": "parse_failure",
                "error_type": "_InvalidCaptureRoot",
            },
        )
    ]


def test_internal_integer_serialization_type_error_is_not_silently_skipped(monkeypatch):
    def programming_fault(*_args, **_kwargs):
        raise TypeError("Integer exceeds 64-bit range")

    monkeypatch.setattr(job, "parse_payload", programming_fault)
    with pytest.raises(TypeError, match="Integer exceeds 64-bit range"):
        list(job._parse_partition([("user/req.json", b"{}", "")], input_format="tokenplan"))


def test_partition_diagnostic_serialization_preserves_integer_values(tmp_path):
    run_id = "a" * 32
    LocalOutputStore(tmp_path / "output").initialize_run(run_id)
    diagnostic = {"source_ref": "user/req.json", "reason": "test", "values": BIG_VALUES}
    result = next(
        job._publish_partition(
            0,
            iter([("diagnostic", diagnostic)]),
            output_uri=str(tmp_path / "output"),
            endpoint_url=None,
            region_name="unused",
            client_factory=_forbidden_s3_client,
            run_id=run_id,
        )
    )
    payload = (tmp_path / "output" / result["diagnostic_part"]["path"]).read_bytes()
    assert json.loads(payload) == diagnostic
    assert all(type(value) is int for value in json.loads(payload)["values"])


@pytest.mark.parametrize("threshold", [1000, 1], ids=["memory", "forced-disk"])
def test_real_spark_bigints_match_serial_trajectory_bytes(
    local_spark, tmp_path, threshold  # noqa: F811 - imported pytest fixture
):
    source_ref, payload = _bigint_capture()
    parsed = parse_payload(source_ref, payload, input_format="tokenplan")
    snapshot = Snapshot.model_validate_json(parsed.snapshot_json)
    expected = _serial_artifacts([snapshot], tmp_path)
    assert len(expected) == 1
    output = tmp_path / "output"
    invalid = ("user/req_invalid.json", json.dumps([10**400 + 7]).encode())
    result = job.run_spark_job(
        SparkNormalizeConfig(
            "s3://fake/input/",
            str(output),
            prefix_shards=("user/",),
            default_parallelism=2,
            shuffle_partitions=2,
            max_scope_snapshots=threshold,
        ),
        spark=local_spark,
        source_reader=partial(synthetic_reader, records=[(source_ref, payload), invalid]),
        client_factory=_forbidden_s3_client,
    )
    assert result["input_files"] == 2
    assert result["parse_failures"] == 1
    assert result["trajectories"] == 1
    assert result["verified_trajectory_objects"] == 1
    metrics_key = "disk_scopes" if threshold == 1 else "memory_scopes"
    assert result["performance"][metrics_key] == 1
    assert {
        path.name: path.read_bytes() for path in output.glob("bigint-session_sub_*.jsonl")
    } == {artifact.filename: artifact.payload for artifact in expected}

    projected = json.loads((output / expected[0].filename).read_bytes())
    arguments = projected["messages"][1]["tool_calls"][0]["function"]["arguments"]
    assert arguments["nested"]["values"] == BIG_VALUES
    assert all(type(value) is int for value in arguments["nested"]["values"])
    defaults = projected["tools"][0]["parameters"]["properties"]["nested"]["default"]
    assert defaults["values"] == BIG_VALUES
    assert all(type(value) is int for value in defaults["values"])
    assert [json.loads(line) for line in (output / "lineage.jsonl").read_bytes().splitlines()] == [
        artifact.lineage for artifact in expected
    ]
    diagnostic = json.loads((output / result["diagnostics"]["path"]).read_bytes())
    assert diagnostic["error_type"] == "_InvalidCaptureRoot"
    report = validate_output(output)
    assert report.valid, report.errors
