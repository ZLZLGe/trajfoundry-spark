"""Isolate invalid Unicode in one capture without hiding programming faults."""

from __future__ import annotations

import hashlib
import io
import json
from functools import partial

import pytest

from trajfoundry.models import Snapshot
from trajfoundry.quality import _value_tokens, _visible_token_text, estimate_tokens
from trajfoundry.spark import adapters, job, source
from trajfoundry.spark.config import SparkNormalizeConfig
from trajfoundry.validation import validate_output

from .test_deepinfra_source import _envelope
from .test_spark_job import local_spark, synthetic_reader  # noqa: F401
from .test_spark_lakehouse_sources import parquet_client
from .test_spark_local_output import _forbidden_s3_client


@pytest.mark.parametrize("surrogate", [r"\ud800", r"\udfff"])
@pytest.mark.parametrize("bigint", [False, True])
def test_quality_token_estimate_escapes_embedded_surrogate(surrogate, bigint) -> None:
    # The outer message is valid text. The embedded JSON is decoded during
    # media filtering and contains a lone surrogate only after JSON decoding.
    number = ',"number":1208925819614629174706176' if bigint else ""
    value = (
        'prefix {"type":"text","text":"中文'
        + surrogate
        + '","encrypted_content":"hidden"'
        + number
        + '} suffix'
    )
    number = '"number":1208925819614629174706176,' if bigint else ""
    expected = 'prefix {' + number + '"text":"中文' + surrogate + '","type":"text"} suffix'
    assert _visible_token_text(value) == expected
    assert _value_tokens(value) == estimate_tokens(expected)


def _capture(text, *, session_id="unicode-session", bigint=False):
    messages = [{"role": "user", "content": text}]
    if bigint:
        # The integer precedes the invalid text in the serialized snapshot,
        # exercising the stdlib fallback's UTF-8 encoder rather than orjson.
        messages[:0] = [
            {
                "role": "assistant",
                "tool_calls": [
                    {
                        "id": "lookup-call",
                        "type": "function",
                        "function": {
                            "name": "lookup",
                            "arguments": json.dumps({"id": 2**80 + 123}),
                        },
                    }
                ],
            },
            {"role": "tool", "tool_call_id": "lookup-call", "content": "found"},
        ]
    return json.dumps(
        _envelope(
            request_body={
                "model": "test-model",
                "metadata": {"session_id": session_id},
                "messages": messages,
            },
            response_body={
                "id": "answer",
                "object": "chat.completion",
                "model": "test-model",
                "choices": [
                    {
                        "index": 0,
                        "finish_reason": "stop",
                        "message": {"role": "assistant", "content": "done"},
                    }
                ],
            },
        )
    ).encode()


@pytest.mark.parametrize("surrogate", [chr(0xD800), chr(0xDC00)], ids=["high", "low"])
@pytest.mark.parametrize("bigint", [False, True], ids=["orjson", "bigint-fallback"])
def test_bad_unicode_is_one_diagnostic_between_valid_captures(surrogate, bigint):
    bad = _capture("synthetic-sensitive" + surrogate, bigint=bigint)
    rows = list(
        job._parse_partition(
            [
                ("before.json", _capture("before 中文🙂"), ""),
                ("bad.json", bad, ""),
                ("after.json", _capture("after"), ""),
            ],
            input_format="deepinfra",
        )
    )
    assert [kind for kind, _ in rows] == ["ok", "error", "ok"]
    assert rows[1][1] == {
        "source_ref": "bad.json",
        "sha256": hashlib.sha256(bad).hexdigest(),
        "reason": "parse_failure",
        "error_type": "SnapshotUnicodeError",
    }
    assert "synthetic-sensitive" not in json.dumps(rows[1][1])
    restored = Snapshot.model_validate_json(rows[0][1][-1])
    assert restored.history[0].content == "before 中文🙂"


@pytest.mark.parametrize(
    "fault",
    [
        TypeError("Type is not JSON serializable: object"),
        TypeError("Integer exceeds 64-bit range"),
        UnicodeEncodeError("ascii", "中", 0, 1, "ordinal not in range(128)"),
    ],
)
def test_other_snapshot_encoding_errors_still_propagate(monkeypatch, fault):
    def broken_encoder(*_args, **_kwargs):
        raise fault

    monkeypatch.setattr(adapters, "json_dumps", broken_encoder)
    with pytest.raises(type(fault)) as raised:
        list(
            job._parse_partition(
                [("valid.json", _capture("hello"), "")], input_format="deepinfra"
            )
        )
    assert raised.value is fault


def test_unicode_type_error_outside_encoding_boundary_still_propagates(monkeypatch):
    fault = TypeError("str is not valid UTF-8: surrogates not allowed")

    def broken_parser(*_args, **_kwargs):
        raise fault

    monkeypatch.setattr(job, "parse_payload", broken_parser)
    with pytest.raises(TypeError) as raised:
        list(
            job._parse_partition(
                [("valid.json", _capture("hello"), "")], input_format="deepinfra"
            )
        )
    assert raised.value is fault


def test_bad_unicode_is_isolated_per_parquet_row(monkeypatch):
    pa = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    output = io.BytesIO()
    pq.write_table(
        pa.table(
            {
                "raw_json": [
                    _capture("before").decode(),
                    _capture("synthetic-sensitive\ud800").decode(),
                    _capture("after").decode(),
                ]
            }
        ),
        output,
        row_group_size=2,
    )
    raw = output.getvalue()
    monkeypatch.setattr(source, "_input_client", lambda *args: parquet_client(raw))
    records = source.read_listed_objects(
        [("requests.parquet", len(raw), '"p"')],
        input_uri="s3://fake/in/",
        input_format="deepinfra",
        endpoint_url=None,
        region_name="us-east-1",
    )
    rows = list(job._parse_partition(records, input_format="deepinfra"))
    assert [kind for kind, _ in rows] == ["ok", "error", "ok"]
    assert rows[1][1]["source_ref"] == "requests.parquet#L00000001"
    assert rows[1][1]["error_type"] == "SnapshotUnicodeError"


def test_spark_preserves_escaped_surrogate_in_embedded_json(
    local_spark, tmp_path  # noqa: F811 - imported pytest fixture
):
    content = r'prefix {"type":"text","text":"bad\ud800","encrypted_content":"hidden"}'
    output = tmp_path / "embedded-json-output"
    result = job.run_spark_job(
        SparkNormalizeConfig(
            "s3://fake/in/",
            str(output),
            input_format="deepinfra",
            prefix_shards=("user/",),
            default_parallelism=2,
            shuffle_partitions=2,
        ),
        spark=local_spark,
        source_reader=partial(
            synthetic_reader,
            records=[("user/escaped.json", _capture(content, session_id="escaped"))],
        ),
        client_factory=_forbidden_s3_client,
    )
    assert result["input_files"] == 1
    assert result["parse_failures"] == 0
    assert result["trajectories"] == result["verified_trajectory_objects"] == 1
    trajectory = json.loads((output / "escaped_sub_0.jsonl").read_bytes())
    assert trajectory["messages"][0]["content"] == content
    assert trajectory["total_tokens"] > 0
    assert validate_output(output).valid


@pytest.mark.parametrize("bigint", [False, True], ids=["orjson", "bigint-fallback"])
def test_real_spark_publishes_good_captures_and_counts_bad_unicode(
    local_spark, tmp_path, bigint  # noqa: F811 - imported pytest fixture
):
    bad = _capture("synthetic-sensitive\ud800", session_id="bad", bigint=bigint)
    records = [
        ("user/before.json", _capture("before 中文🙂", session_id="before")),
        ("user/bad.json", bad),
        ("user/after.json", _capture("after", session_id="after")),
    ]
    output = tmp_path / "output"
    result = job.run_spark_job(
        SparkNormalizeConfig(
            "s3://fake/in/",
            str(output),
            input_format="deepinfra",
            prefix_shards=("user/",),
            default_parallelism=2,
            shuffle_partitions=2,
        ),
        spark=local_spark,
        source_reader=partial(synthetic_reader, records=records),
        client_factory=_forbidden_s3_client,
    )
    assert result["input_files"] == 3
    assert result["parse_failures"] == 1
    assert result["trajectories"] == result["verified_trajectory_objects"] == 2
    manifest = json.loads((output / "manifest.json").read_bytes())
    assert manifest["counts"]["input_files"] == 3
    assert manifest["counts"]["skip_reason_counts"] == {"parse_failure": 1}
    assert manifest["parse_failure_count"] == 1
    assert manifest["record_count"] == 2
    diagnostics = (output / result["diagnostics"]["path"]).read_bytes()
    assert [json.loads(line) for line in diagnostics.splitlines()] == [
        {
            "source_ref": "user/bad.json",
            "sha256": hashlib.sha256(bad).hexdigest(),
            "reason": "parse_failure",
            "error_type": "SnapshotUnicodeError",
        }
    ]
    assert b"synthetic-sensitive" not in diagnostics
    trajectory = json.loads((output / "before_sub_0.jsonl").read_bytes())
    assert trajectory["messages"][0]["content"] == "before 中文🙂"
    report = validate_output(output)
    assert report.valid, report.errors
