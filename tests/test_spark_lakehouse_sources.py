from __future__ import annotations

import hashlib
import io
import json
from unittest.mock import Mock

import pytest

from trajfoundry.models import Snapshot
from trajfoundry.sources.boyue import (
    BoyueError,
    adapt_boyue_envelope,
)
from trajfoundry.spark import job, source
from trajfoundry.spark.adapters import parse_payload


def boyue_record():
    return {
        "user_id": "source-user",
        "relay_log_id": "request-only",
        "created_at": 1735689600,
        "request_body": {
            "model": "test",
            "messages": [{"role": "user", "content": "hello"}],
        },
        "response_body": {
            "object": "chat.completion",
            "choices": [
                {
                    "finish_reason": "stop",
                    "message": {"role": "assistant", "content": "hello back"},
                }
            ],
        },
        "metadata": {"other": {"request_path": "/v1/chat/completions"}},
        "user_headers": {
            "Authorization": "synthetic-sensitive",
            "X-Claude-Code-Session-Id": "real-session",
        },
    }


def snapshot(record):
    return Snapshot.model_validate_json(
        parse_payload(
            "00-00/request/raw.json", json.dumps(record).encode(), input_format="boyue"
        ).snapshot_json
    )


def test_boyue_archive_retains_identity_and_audits_missing_status():
    record = boyue_record()
    adapted = adapt_boyue_envelope(record)
    assert "status_code" not in adapted.capture
    assert "synthetic-sensitive" not in json.dumps(adapted.capture)
    parsed = snapshot(record)
    assert parsed.session_id == "real-session"
    assert parsed.user_id == "source-user"
    assert parsed.request_id == "request-only"
    assert parsed.source_name == "boyue"
    assert parsed.wire_complete and parsed.outcome == "success"
    assert "boyue_http_status_unavailable" in {issue.code for issue in parsed.issues}
    record["request_body"]["metadata"] = {"session_id": "body-session"}
    assert snapshot(record).session_id == "body-session"
    record["user_headers"] = {}
    record["request_body"].pop("metadata")
    assert snapshot(record).session_id == ""


def test_boyue_normalized_anthropic_stream_is_read_as_final():
    record = boyue_record()
    record["metadata"]["other"]["request_path"] = "/v1/messages"
    record["request_body"]["stream"] = True
    record["response_body"] = {
        "type": "message",
        "role": "assistant",
        "model": "test",
        "stop_reason": "end_turn",
        "content": [{"type": "text", "text": "done"}],
    }
    parsed = snapshot(record)
    assert parsed.wire_complete and parsed.outcome == "success"
    record["response_body"] = {"response_text": "retained text", "model": "test"}
    parsed = snapshot(record)
    assert not parsed.wire_complete and parsed.outcome == "truncated"
    assert "boyue_response_lossy" in {issue.code for issue in parsed.issues}


@pytest.mark.parametrize(
    "event,status,complete",
    [
        ("response.created", "in_progress", False),
        ("response.completed", "completed", True),
    ],
)
def test_boyue_responses_requires_terminal_evidence(event, status, complete):
    record = boyue_record()
    record["metadata"]["other"]["request_path"] = "/v1/responses"
    record["request_body"] = {"model": "test", "input": "hello"}
    record["response_body"] = {
        "type": event,
        "response": {
            "object": "response",
            "status": status,
            "output": [
                {
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "done"}],
                }
            ],
        },
    }
    assert snapshot(record).wire_complete is complete


def test_missing_path_only_infers_unambiguous_protocol():
    record = boyue_record()
    record["metadata"] = {}
    assert snapshot(record).outcome == "success"
    record["response_body"] = {"result": "unrecognized"}
    with pytest.raises(BoyueError):
        snapshot(record)


def test_nontrajectory_is_counted_separately_from_parse_failures():
    record = boyue_record()
    record["metadata"]["other"]["request_path"] = "/v1/embeddings"
    parsed = list(
        job._parse_partition(
            [("r/raw.json", json.dumps(record).encode(), "unused")],
            input_format="boyue",
        )
    )
    assert parsed[0][1]["reason"] == "non_trajectory_endpoint"
    built = list(
        job._build_partition([(("parse_failure", "r/raw.json"), iter(parsed))])
    )
    counts = dict(built)["counts"]
    assert counts == {"input_files": 1, "skipped_inputs": 1}
    # This classification must not be counted as malformed JSON.
    assert dict(built)["diagnostic"]["reason"] != "parse_failure"


def test_boyue_trajectory_retains_total_tokens_and_manifest_counts(tmp_path):
    from trajfoundry.output_contract import parse_trajectory_record, project_trajectory
    from trajfoundry.spark.build import build_scope
    from trajfoundry.spark.manifest import DiskManifestAccumulator
    from trajfoundry.spark.output import SparkObjectDescription

    parsed = snapshot(boyue_record())
    built = list(build_scope([parsed]))
    assert len(built) == 1
    value = project_trajectory(built[0].node)
    assert value["total_tokens"] > 0
    assert value["metadata"]["specific_source"] == "boyue-rich"
    parse_trajectory_record(value)
    with DiskManifestAccumulator(
        tmp_path / "counts.sqlite",
        input_root="s3://b/i/",
        input_format="boyue",
        config_hash="a" * 64,
        started_at_ms=10,
    ) as acc:
        acc.stats.accepted = 1
        acc.stats.input_files = 1
        acc.add_object(SparkObjectDescription("s_sub_0.jsonl", "a" * 64, 123))
        acc.write_manifest(tmp_path / "manifest.json", completed_at_ms=20)
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert manifest["record_count"] == manifest["accepted_trajectory_count"] == 1
    assert (
        manifest["quarantined_trajectory_count"] == manifest["parse_failure_count"] == 0
    )
    assert manifest["started_at_ms"] == 10 and manifest["completed_at_ms"] == 20


def test_only_final_boyue_raw_and_deepinfra_data_are_selected():
    for path in ["00/request/raw.json", "00/request/no_session_id/raw.json"]:
        assert source._selected(path, "boyue")
    for path in [
        "00/request/raw.json.tmp",
        "00/request/request.json",
        "00/request/response.json",
        "manifest.json",
    ]:
        assert not source._selected(path, "boyue")
    assert not source._selected("manifest.json", "deepinfra")
    assert source._selected("model=x/no_session_id/part.parquet", "deepinfra")


def test_root_and_nested_deepinfra_shards_do_not_overlap(monkeypatch):
    client = Mock()
    client.get_paginator.return_value.paginate.return_value = [
        {
            "Contents": [{"Key": "in/a.json", "Size": 2, "ETag": '"a"'}],
            "CommonPrefixes": [{"Prefix": "in/no_session_id/"}],
        }
    ]
    monkeypatch.setattr(source, "_input_client", lambda *args: client)
    options = {
        "input_uri": "s3://bucket/in/",
        "input_format": "deepinfra",
        "endpoint_url": None,
        "region_name": "us-east-1",
    }
    assert list(source.discover_prefix_shards(**options)) == [".", "no_session_id/"]
    assert list(source.list_shard_objects(["."], **options)) == [("a.json", 2, '"a"')]
    assert (
        client.get_paginator.return_value.paginate.call_args.kwargs["Delimiter"] == "/"
    )


def parquet_client(raw):
    client = Mock()
    client.get_object.return_value = {
        "ETag": '"p"',
        "ContentLength": len(raw),
        "Body": io.BytesIO(raw),
    }
    return client


def test_parquet_preserves_row_identity_and_invalid_json_diagnostic(monkeypatch):
    pa = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    output = io.BytesIO()
    pq.write_table(
        pa.table(
            {
                "raw_json": ['{"a":1}', "broken-json", None],
                "req_id": ["one", "two", "three"],
            }
        ),
        output,
        row_group_size=2,
    )
    raw = output.getvalue()
    client = parquet_client(raw)
    monkeypatch.setattr(source, "_input_client", lambda *args: client)
    rows = list(
        source.read_listed_objects(
            [("p.parquet", len(raw), '"p"')],
            input_uri="s3://b/i/",
            input_format="deepinfra",
            endpoint_url=None,
            region_name="us-east-1",
        )
    )
    assert [row[0] for row in rows] == [f"p.parquet#L{i:08d}" for i in range(3)]
    assert [row[1] for row in rows] == [b'{"a":1}', b"broken-json", b"null"]
    assert all(row[2] == hashlib.sha256(row[1]).hexdigest() for row in rows)
    assert all(
        kind == "error"
        for kind, _ in job._parse_partition(rows, input_format="deepinfra")
    )
    assert client.get_object.call_args.kwargs["IfMatch"] == '"p"'


@pytest.mark.parametrize("bad", [b"not-parquet", b"PAR1brokenPAR1"])
def test_corrupt_parquet_is_a_partition_failure(monkeypatch, bad):
    pa = pytest.importorskip("pyarrow")
    monkeypatch.setattr(source, "_input_client", lambda *args: parquet_client(bad))
    with pytest.raises(pa.ArrowInvalid):
        list(
            source.read_listed_objects(
                [("p.parquet", len(bad), '"p"')],
                input_uri="s3://b/i/",
                input_format="deepinfra",
                endpoint_url=None,
                region_name="us-east-1",
            )
        )


def test_inventory_fingerprint_ignores_listing_order_but_detects_change():
    from functools import reduce

    items = [("a", 1, '"a"'), ("b", 2, '"b"')]

    def fingerprint(values):
        return source.inventory_fingerprint(
            reduce(
                source.merge_inventory, map(source.inventory_item, values), (0, 0, 0)
            )
        )

    assert fingerprint(items) == fingerprint(reversed(items))
    assert fingerprint(items) != fingerprint(items + items[:1])
    assert fingerprint(items) != fingerprint([("a", 1, '"new"'), items[1]])
