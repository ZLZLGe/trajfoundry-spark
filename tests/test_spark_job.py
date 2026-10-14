from __future__ import annotations

import hashlib
import json
import sys
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError

from trajfoundry.s3 import parse_s3_uri
from trajfoundry.spark import job
from trajfoundry.spark.config import SparkNormalizeConfig
from trajfoundry.validation import validate_output

from .test_spark_adapters import _tokenplan_envelope
from .test_spark_build import _snapshot


def test_small_routing_scope_uses_bounded_builder(monkeypatch):
    from trajfoundry.spark import bounded

    captures = [_snapshot(f"request-{index}.json") for index in range(3)]
    captures[1].parent_thread_id = "parent"
    consumed = []

    def disk_builder(snapshots, *, limits):
        consumed.extend(item.source_path for item in snapshots)
        return iter(())

    def forbidden_memory_builder(_snapshots):
        pytest.fail("routing evidence bypassed derived graph budgets")

    monkeypatch.setattr(bounded, "build_scope_bounded", disk_builder)
    monkeypatch.setattr(job, "build_scope", forbidden_memory_builder)
    rows = [(("user", "user"), iter(("ok", (s.model_dump_json(),)) for s in captures))]
    artifacts = list(job._build_partition(rows))
    assert consumed == [item.source_path for item in captures]
    assert ("counts", {"input_files": 3}) in artifacts
    assert dict(artifacts)["build_metrics"] == {
        "disk_scopes": 1,
        "memory_scopes": 0,
        "largest_component_records": 3,
    }


class LocalObjectStore:
    """File-backed synthetic S3 boundary shared by local Spark subprocesses."""

    def __init__(self, root: str, *_args, **_kwargs):
        self.root = Path(root)

    def head_object(self, *, Bucket, Key):
        path = self.root / Bucket / Key
        if not path.is_file():
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "HeadObject")
        content = path.read_bytes()
        return {
            "ContentLength": len(content),
            "Metadata": {"sha256": hashlib.sha256(content).hexdigest()},
        }

    def put_object(self, *, Bucket, Key, Body, **_kwargs):
        path = self.root / Bucket / Key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(Body if isinstance(Body, bytes) else Body.read())

    def get_object(self, *, Bucket, Key):
        return {"Body": (self.root / Bucket / Key).open("rb")}

    def delete_object(self, *, Bucket, Key):
        (self.root / Bucket / Key).unlink(missing_ok=True)

    def get_paginator(self, _name):
        return self

    def paginate(self, *, Bucket, Prefix):
        root = self.root / Bucket
        yield {
            "Contents": [
                {"Key": path.relative_to(root).as_posix()}
                for path in sorted(root.rglob("*"))
                if path.is_file()
                and path.relative_to(root).as_posix().startswith(Prefix)
            ]
        }


class CorruptObjectStore(LocalObjectStore):
    """Keep original HEAD metadata while corrupting a selected GET body."""

    def __init__(self, root, *_args, corrupt_kind, **_kwargs):
        super().__init__(root)
        self.corrupt_kind = corrupt_kind

    def get_object(self, *, Bucket, Key):
        import io

        response = super().get_object(Bucket=Bucket, Key=Key)
        selected = (
            self.corrupt_kind == "trajectory" and "_sub_" in Key
        ) or Key.endswith("/" + self.corrupt_kind)
        if selected:
            response["Body"].close()
            return {"Body": io.BytesIO(b"corrupted")}
        return response


def synthetic_reader(shards, *, records, **_kwargs):
    for shard in shards:
        for source_ref, payload in records:
            if source_ref.startswith(shard):
                yield source_ref, payload, hashlib.sha256(payload).hexdigest()


def test_parse_failures_are_auditable_and_programming_faults_propagate():
    rows = list(
        job._parse_partition(
            [("fake/req_bad.json", b"not-json password=synthetic-secret", "")],
            input_format="tokenplan",
        )
    )
    assert rows[0][0] == "error"
    assert rows[0][1]["source_ref"] == "fake/req_bad.json"
    assert rows[0][1]["reason"] == "parse_failure"
    assert "synthetic-secret" not in json.dumps(rows)
    with (
        patch.object(job, "parse_payload", side_effect=RuntimeError("internal fault")),
        pytest.raises(RuntimeError, match="internal fault"),
    ):
        list(
            job._parse_partition(
                [("fake/req.json", b"{}", "")], input_format="tokenplan"
            )
        )


def test_invalid_non_object_input_is_an_explicit_failure():
    rows = list(
        job._parse_partition([("req.json", b"[]", "")], input_format="tokenplan")
    )
    assert rows[0][0] == "error"


def test_publish_consumes_trajectories_incrementally_and_sorts_only_lineage(tmp_path):
    first = job.build_scope([_snapshot("req_first.json")])[0]
    second_snapshot = _snapshot("req_second.json")
    second_snapshot.session_id = "another-session"
    second = job.build_scope([second_snapshot])[0]

    def events():
        yield "trajectory", first
        # The consumer must publish the first trajectory before requesting more.
        assert (tmp_path / "fake" / "out" / "session-1_sub_0.jsonl").exists()
        yield "trajectory", second

    results = list(
        job._publish_partition(
            0,
            events(),
            output_uri="s3://fake/out/",
            endpoint_url=None,
            region_name="unused",
            client_factory=partial(LocalObjectStore, str(tmp_path)),
        )
    )
    assert results[0]["object_count"] == 2
    assert "objects" not in results[0]
    assert (
        results[0]["objects_part"]["path"] == "diagnostics/._objects_part_000000.ndjson"
    )
    assert (
        results[0]["lineage_part"]["path"] == "diagnostics/._lineage_part_000000.ndjson"
    )
    inventory = tmp_path / "fake" / "out" / results[0]["objects_part"]["path"]
    assert len(inventory.read_bytes().splitlines()) == 2
    lines = (
        (tmp_path / "fake" / "out" / results[0]["lineage_part"]["path"])
        .read_bytes()
        .splitlines()
    )
    identifiers = [json.loads(line)["trajectory_id"] for line in lines]
    assert identifiers == sorted(identifiers)


def test_partition_descriptor_limit_is_fail_closed(tmp_path):
    built = job.build_scope([_snapshot()])[0]
    with pytest.raises(RuntimeError, match="partition output descriptor limit"):
        list(
            job._publish_partition(
                0,
                iter([("trajectory", built), ("trajectory", built)]),
                output_uri="s3://fake/out/",
                endpoint_url=None,
                region_name="unused",
                max_objects=1,
                client_factory=partial(LocalObjectStore, str(tmp_path)),
            )
        )
    assert not (tmp_path / "fake" / "out" / "manifest.json").exists()


def test_publish_verifies_remote_trajectory_before_receipt(tmp_path):
    built = job.build_scope([_snapshot()])[0]
    with pytest.raises(ValueError, match="hash or size"):
        list(
            job._publish_partition(
                0,
                iter([("trajectory", built)]),
                output_uri="s3://fake/out/",
                endpoint_url=None,
                region_name="unused",
                client_factory=partial(
                    CorruptObjectStore, str(tmp_path), corrupt_kind="trajectory"
                ),
            )
        )
    assert not list((tmp_path / "fake" / "out").rglob("._objects_part*"))


def test_publish_forwards_only_output_credential_path(monkeypatch, tmp_path):
    seen = []
    original = job.create_output_store

    def store(*args, **kwargs):
        seen.append(kwargs.pop("output_credentials_path"))
        return original(*args, **kwargs)

    monkeypatch.setattr(job, "create_output_store", store)
    list(
        job._publish_partition(
            0,
            iter(()),
            output_uri="s3://fake/out/",
            endpoint_url=None,
            region_name="unused",
            client_factory=partial(LocalObjectStore, str(tmp_path)),
            output_credentials_path="/shared/private/output.json",
        )
    )
    assert seen == ["/shared/private/output.json"]


def test_config_hash_includes_normalizer_revision():
    config = SparkNormalizeConfig(
        "s3://fake/in/", "s3://fake/out/", prefix_shards=("user/",)
    )
    before = job._config_hash(config)
    with patch.object(job, "NORMALIZER_REVISION", "synthetic-next-revision"):
        assert job._config_hash(config) != before


def test_automatic_prefix_resolution_collects_only_bounded_names(monkeypatch):
    class DiscoveryRDD:
        def mapPartitions(self, function):
            self.values = list(function(iter((None,))))
            return self

        def collect(self):
            return self.values

    class DiscoveryContext:
        def parallelize(self, values, slices):
            assert values == [None]
            assert slices == 1
            return DiscoveryRDD()

    monkeypatch.setattr(
        job,
        "discover_prefix_shards",
        lambda *_args, **_kwargs: iter(("user-a/", "no_session_id/")),
    )
    config = SparkNormalizeConfig(
        "s3://fake/in/",
        "s3://fake/out/",
        auto_prefix_shards=True,
    )
    assert job._resolve_prefix_shards(DiscoveryContext(), config) == (
        "user-a/",
        "no_session_id/",
    )


def test_runtime_consistency_handshake_collects_bounded_executor_identity(
    monkeypatch,
):
    class ProbeRDD:
        def __init__(self, values):
            self.values = values

        def mapPartitions(self, function):
            self.values = list(function(iter(self.values)))
            return self

        def collect(self):
            return self.values

    class ProbeContext:
        defaultParallelism = 3

        def parallelize(self, values, partitions):
            assert list(values) == [0, 1, 2]
            assert partitions == 3
            return ProbeRDD(values)

    expected = {
        "normalizer_revision": "test-revision",
        "source_hash": "test-hash",
    }
    monkeypatch.setattr(job, "_runtime_identity", lambda: expected)

    def reports(_part, revision, source_hash):
        for partition in range(3):
            yield {
                "revision": revision,
                "source_hash": source_hash,
                "host": f"executor-{partition}",
                "partition": partition,
                "pid": str(partition),
            }

    monkeypatch.setattr(job, "_executor_runtime_identity", reports)
    result = job._runtime_consistency_handshake(ProbeContext())
    assert result == {
        "normalizer_revision": "test-revision",
        "source_hash": "test-hash",
        "executor_reports": 3,
        "executor_hosts": ["executor-0", "executor-1", "executor-2"],
    }


def test_runtime_consistency_handshake_fails_on_executor_source_mismatch(monkeypatch):
    class ProbeRDD:
        def mapPartitions(self, function):
            self.values = list(function(iter((0,))))
            return self

        def collect(self):
            return self.values

    class ProbeContext:
        defaultParallelism = 1

        def parallelize(self, values, partitions):
            assert list(values) == [0]
            assert partitions == 1
            return ProbeRDD()

    monkeypatch.setattr(
        job,
        "_runtime_identity",
        lambda: {"normalizer_revision": "driver", "source_hash": "driver-hash"},
    )
    monkeypatch.setattr(
        job,
        "_executor_runtime_identity",
        lambda _part, _revision, _source_hash: iter(
            [
                {
                    "revision": "executor-old",
                    "source_hash": "executor-old-hash",
                    "host": "stale-executor",
                    "partition": 0,
                    "pid": "123",
                }
            ]
        ),
    )
    with pytest.raises(RuntimeError, match="code consistency handshake failed"):
        job._runtime_consistency_handshake(ProbeContext())


def test_nonempty_prefix_is_rejected_without_put():
    calls = []
    client = SimpleNamespace(
        get_paginator=lambda _: SimpleNamespace(
            paginate=lambda **_: [{"Contents": [{"Key": "out/old"}]}]
        ),
        put_object=lambda **kwargs: calls.append(kwargs),
    )
    with pytest.raises(ValueError, match="fresh exclusive prefix"):
        job._ensure_empty_output_prefix(client, parse_s3_uri("s3://fake/out/"))
    assert calls == []


@pytest.fixture(scope="module")
def local_spark(tmp_path_factory):
    pyspark = pytest.importorskip(
        "pyspark", reason="run with the installed Spark 3.5 Python paths"
    )
    from pyspark.sql import SparkSession

    # This suite never connects to platform listeners, a cluster, or real S3.
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setenv(
        "SPARK_CONF_DIR", str(tmp_path_factory.mktemp("empty-spark-conf"))
    )
    monkeypatch.setenv("SPARK_LOCAL_IP", "127.0.0.1")
    monkeypatch.setenv("PYSPARK_PYTHON", sys.executable)
    monkeypatch.setenv("PYSPARK_DRIVER_PYTHON", sys.executable)
    monkeypatch.delenv("PYSPARK_SUBMIT_ARGS", raising=False)
    conf = (
        pyspark.SparkConf(loadDefaults=False)
        .setMaster("local[2]")
        .setAppName("trajfoundry-synthetic-test")
    )
    for key, value in {
        "spark.ui.enabled": "false",
        "spark.driver.host": "127.0.0.1",
        "spark.driver.bindAddress": "127.0.0.1",
        "spark.default.parallelism": "2",
        "spark.sql.shuffle.partitions": "2",
        "spark.executor.instances": "1",
        "spark.executor.cores": "1",
        "spark.executor.memory": "512m",
        "spark.submit.deployMode": "client",
        "spark.pyspark.python": sys.executable,
        "spark.extraListeners": "",
        "spark.plugins": "",
    }.items():
        conf.set(key, value)
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        yield spark
    finally:
        spark.stop()
        monkeypatch.undo()


def test_real_spark_local_two_end_to_end_and_effective_submit_settings(
    local_spark, tmp_path
):
    config = SparkNormalizeConfig(
        "s3://fake/in/", "s3://fake/out/", prefix_shards=("alice/", "bob/")
    )
    records = [
        ("alice/req_valid.json", json.dumps(_tokenplan_envelope()).encode()),
        ("bob/req_invalid.json", b"not-json"),
    ]
    result = job.run_spark_job(
        config,
        spark=local_spark,
        source_reader=partial(synthetic_reader, records=records),
        client_factory=partial(LocalObjectStore, str(tmp_path)),
    )
    assert result["input_files"] == 2
    assert result["parse_failures"] == 1
    assert result["trajectories"] == 1
    assert result["verified_trajectory_objects"] == 1
    assert result["shuffle_partitions"] == 2
    effective = local_spark.sparkContext.getConf()
    assert effective.get("spark.submit.deployMode") == "client"
    assert effective.get("spark.executor.instances") == "1"
    assert effective.get("spark.executor.memory") == "512m"
    output = tmp_path / "fake" / "out"
    assert validate_output(output).valid
    assert result["diagnostics"]["path"] == "diagnostics/input_diagnostics.ndjson"
    assert {path.name for path in output.iterdir()} == {
        "session-1_sub_0.jsonl",
        "manifest.json",
        "lineage.jsonl",
        "diagnostics",
    }
    diagnostic = json.loads((output / result["diagnostics"]["path"]).read_bytes())
    assert diagnostic["source_ref"] == "bob/req_invalid.json"
    assert not list(output.rglob("._*"))
    before = {
        path.relative_to(output): path.read_bytes()
        for path in output.rglob("*")
        if path.is_file()
    }
    with pytest.raises(ValueError, match="not empty"):
        job.run_spark_job(
            config,
            spark=local_spark,
            source_reader=partial(synthetic_reader, records=records),
            client_factory=partial(LocalObjectStore, str(tmp_path)),
        )
    assert {
        path.relative_to(output): path.read_bytes()
        for path in output.rglob("*")
        if path.is_file()
    } == before


def test_real_spark_driver_limit_prevents_manifest(local_spark, tmp_path):
    config = SparkNormalizeConfig(
        "s3://fake/in/",
        "s3://fake/out/",
        prefix_shards=("user/",),
        max_output_objects=1,
    )
    records = [
        (
            f"user/req_{index}.json",
            json.dumps(_tokenplan_envelope(session_id=f"session-{index}")).encode(),
        )
        for index in range(2)
    ]
    with pytest.raises(RuntimeError, match="driver output descriptor limit"):
        job.run_spark_job(
            config,
            spark=local_spark,
            source_reader=partial(synthetic_reader, records=records),
            client_factory=partial(LocalObjectStore, str(tmp_path)),
        )
    assert not (tmp_path / "fake" / "out" / "manifest.json").exists()


@pytest.mark.parametrize(
    "corrupt_kind", ["trajectory", "lineage.jsonl", "input_diagnostics.ndjson"]
)
def test_s3_verification_failure_prevents_success_manifest(
    local_spark, tmp_path, corrupt_kind
):
    records = [
        ("user/req_valid.json", json.dumps(_tokenplan_envelope()).encode()),
        ("user/req_bad.json", b"invalid-json"),
    ]
    with pytest.raises(Exception, match="hash or size"):
        job.run_spark_job(
            SparkNormalizeConfig(
                "s3://fake/in/", "s3://fake/out/", prefix_shards=("user/",)
            ),
            spark=local_spark,
            source_reader=partial(synthetic_reader, records=records),
            client_factory=partial(
                CorruptObjectStore, str(tmp_path), corrupt_kind=corrupt_kind
            ),
        )
    assert not (tmp_path / "fake" / "out" / "manifest.json").exists()


def test_real_spark_recomputed_parse_counts_are_data_not_accumulator_updates(
    local_spark,
):
    payload = json.dumps(_tokenplan_envelope()).encode()
    records = [("alice/req.json", payload, ""), ("bob/req.json", b"bad-json", "")]
    parsed = local_spark.sparkContext.parallelize(records, 2).mapPartitions(
        partial(job._parse_partition, input_format="tokenplan")
    )
    # Force an earlier action, then recompute the unpersisted parse stage.
    assert parsed.count() == 2
    events = (
        parsed.map(job._key_parsed)
        .groupByKey(2)
        .mapPartitions(job._build_partition)
        .collect()
    )
    counts = [value for kind, value in events if kind == "counts"]
    assert sum(item.get("input_files", 0) for item in counts) == 2
    assert sum(item.get("skipped_inputs", 0) for item in counts) == 1


def test_real_spark_empty_shards_publish_zero_feedback(local_spark, tmp_path):
    config = SparkNormalizeConfig(
        "s3://fake/in/", "s3://fake/out/", prefix_shards=("wrong/",)
    )
    result = job.run_spark_job(
        config,
        spark=local_spark,
        source_reader=partial(synthetic_reader, records=[]),
        client_factory=partial(LocalObjectStore, str(tmp_path)),
    )
    manifest = json.loads((tmp_path / "fake" / "out" / "manifest.json").read_bytes())
    assert result["trajectories"] == 0
    assert manifest["status"] == "SUCCEEDED"
    assert manifest["record_count"] == manifest["total_bytes"] == 0
    assert manifest["parse_failure_count"] == 0


def test_real_spark_user_scopes_and_final_identity_deduplication(local_spark, tmp_path):
    records = []
    for index, (session, user, thread) in enumerate(
        [
            ("duplicate-session", "alice", "first"),
            ("duplicate-session", "alice", "second"),
            ("no_session_id", "alice", "first"),
            ("no_session_id", "bob", "first"),
        ]
    ):
        envelope = _tokenplan_envelope(session_id=session)
        envelope["metadata"]["source_user_id"] = user
        envelope["client_request"]["capture"]["body"]["metadata"] = {
            "session_id": session,
            "thread_id": thread,
        }
        records.append((f"user/req_{index}.json", json.dumps(envelope).encode()))
    result = job.run_spark_job(
        SparkNormalizeConfig(
            "s3://fake/in/", "s3://fake/out/", prefix_shards=("user/",)
        ),
        spark=local_spark,
        source_reader=partial(synthetic_reader, records=records),
        client_factory=partial(LocalObjectStore, str(tmp_path)),
    )
    assert result["input_files"] == 4
    assert result["trajectories"] == 3
    output = tmp_path / "fake" / "out"
    report = validate_output(output)
    assert report.valid, report.errors
    lineage = [
        json.loads(line)
        for line in (output / "lineage.jsonl").read_bytes().splitlines()
    ]
    assert sorted(item["origin_count"] for item in lineage) == [1, 1, 2]
    assert len(list(output.glob("no_session_id_*.jsonl"))) == 2
