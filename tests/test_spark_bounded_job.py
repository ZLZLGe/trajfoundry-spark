import json
from functools import partial, reduce

from trajfoundry.identity import aggregation_scope
from trajfoundry.models import Snapshot
from trajfoundry.pipeline import _snapshot_record
from trajfoundry.spark.adapters import parse_payload
from trajfoundry.spark.build import build_scope
from trajfoundry.spark.byte_plan import make_chunk_id, record_order_key
from trajfoundry.spark.component_plan import (
    component_key,
    merge_summaries,
    summarize_snapshot,
)
from trajfoundry.spark.config import SparkNormalizeConfig
from trajfoundry.spark.job import run_spark_job
from trajfoundry.spark.output import ManifestAccumulator
from trajfoundry.validation import validate_output

from .test_spark_adapters import _tokenplan_envelope
from .test_spark_job import local_spark, synthetic_reader  # noqa: F401
from .test_spark_local_output import _forbidden_s3_client, _synthetic_records


def test_forced_disk_build_matches_memory_build_end_to_end(local_spark, tmp_path):  # noqa: F811 - imported pytest fixture
    manifests = []
    for name, threshold in (("memory", 1000), ("disk", 1)):
        output = tmp_path / name
        result = run_spark_job(
            SparkNormalizeConfig(
                "s3://fake/input/",
                str(output),
                prefix_shards=("alice/", "bob/"),
                max_scope_snapshots=threshold,
            ),
            spark=local_spark,
            source_reader=partial(synthetic_reader, records=_synthetic_records()),
            client_factory=_forbidden_s3_client,
        )
        if name == "disk":
            assert result["performance"]["disk_scopes"] > 0
        manifests.append(json.loads((output / "manifest.json").read_bytes()))
    assert manifests[0]["counts"] == manifests[1]["counts"]
    assert manifests[0]["files"] == manifests[1]["files"]


def _sessionless_record(path, question, *, depth, thread, order):
    envelope = _tokenplan_envelope(session_id="no_session_id" if order % 2 else "")
    envelope["metadata"].update(
        source_user_id="shared-user",
        request_id=f"request-{order}",
        received_at_ms=order * 1000 + 1000,
        completed_at_ms=order * 1000 + 1234,
    )
    body = envelope["client_request"]["capture"]["body"]
    body["metadata"] = {"thread_id": thread}
    body["messages"] = [{"role": "user", "content": question}]
    for turn in range(depth):
        body["messages"].extend(
            [
                {"role": "assistant", "content": "done"},
                {"role": "user", "content": f"follow-up-{turn}"},
            ]
        )
    return path, json.dumps(envelope).encode()


def _parsed_snapshot(record):
    source_ref, payload = record
    return Snapshot.model_validate_json(
        parse_payload(source_ref, payload, input_format="tokenplan").snapshot_json
    )


def test_same_missing_user_components_mix_disk_and_memory_with_unsplit_parity(
    local_spark,  # noqa: F811 - imported pytest fixture
    tmp_path,
):
    # Five captures form one prefix component: two intermediate prefixes and
    # three terminal copies in different threads. Flat thread identity keeps
    # the copies separate until final canonical dedup merges all five origins.
    disk_records = [
        _sessionless_record(
            f"shared-user/req_disk_{index}.json",
            "disk conversation",
            depth=depth,
            thread=f"thread-{index}",
            order=index,
        )
        for index, depth in enumerate((0, 1, 2, 2, 2))
    ]
    disk_snapshots = list(map(_parsed_snapshot, disk_records))
    summary = reduce(merge_summaries, map(summarize_snapshot, disk_snapshots))
    disk_key = component_key(disk_snapshots[0], summary)
    assert {component_key(item, summary) for item in disk_snapshots} == {disk_key}

    # One byte chunk can retain separate safe prefix components. Its large
    # component uses the disk builder and the independent short one stays in
    # memory, without relying on Driver/worker-dependent Python hash placement.
    memory_record = _sessionless_record(
        "shared-user/req_memory.json",
        "independent conversation",
        depth=0,
        thread="memory-thread",
        order=5,
    )
    assert component_key(_parsed_snapshot(memory_record), summary) != disk_key
    capture = disk_snapshots[0]
    scope = aggregation_scope(capture.session_id, capture.user_id, capture.captured_at)
    records = [*disk_records, memory_record]
    snapshots = list(map(_parsed_snapshot, records))
    assert {item.user_id for item in snapshots} == {"shared-user"}
    assert {item.session_id for item in snapshots} == {""}

    # With one shuffle partition the user is assigned one byte chunk. Give
    # the independent unsplit reference the same deterministic identity while
    # bypassing both component planning and the disk builder.
    first_key = min(
        record_order_key(item.captured_at, item.source_path, item.source_sha256)
        for item in snapshots
    )
    chunk_id = make_chunk_id(scope, first_key)
    for item in snapshots:
        item.sessionless_chunk_id = chunk_id
    baseline = ManifestAccumulator("s3://fake/input/", "tokenplan", "test-baseline")
    baseline.stats.input_files = len(records)
    for item in snapshots:
        record = _snapshot_record(item)
        if record is not None:
            baseline.add_record(record)
    expected = [
        baseline.add_trajectory(item.node, item.origins)
        for item in build_scope(snapshots)
    ]
    assert len(expected) == 2
    assert sorted(len(item.origins) for item in expected) == [1, 5]
    assert all(
        json.loads(item.payload)["metadata"]["sub_session_id"] == 0 for item in expected
    )

    output = tmp_path / "mixed-components"
    result = run_spark_job(
        SparkNormalizeConfig(
            "s3://fake/input/",
            str(output),
            prefix_shards=("shared-user/",),
            default_parallelism=2,
            shuffle_partitions=1,
            max_scope_snapshots=4,
        ),
        spark=local_spark,
        source_reader=partial(synthetic_reader, records=list(reversed(records))),
        client_factory=_forbidden_s3_client,
    )
    assert result["performance"]["largest_scope_records"] == 6
    assert result["performance"]["splittable_scopes"] == 1
    assert result["performance"]["largest_component_records"] == 5
    assert result["performance"]["disk_scopes"] == 1
    assert result["performance"]["memory_scopes"] == 1
    assert result["input_files"] == 6
    assert result["parse_failures"] == 0
    assert result["trajectories"] == 2
    assert result["accepted"] == baseline.stats.accepted
    assert result["quarantined_trajectories"] == baseline.stats.quarantined_trajectories
    assert result["quarantined_records"] == baseline.stats.quarantined_records
    assert result["diagnostics"] is None

    # Compare complete bytes, filenames, hashes and sizes for every trajectory.
    assert {
        path.name: path.read_bytes() for path in output.glob("no_session_id_*.jsonl")
    } == {item.filename: item.payload for item in expected}
    manifest = json.loads((output / "manifest.json").read_bytes())
    assert manifest["counts"] == baseline.stats.__dict__
    assert manifest["counts"]["duplicate_trajectories"] == 4
    assert [
        entry for entry in manifest["files"] if entry["path"] != "lineage.jsonl"
    ] == [description.manifest_entry() for description in baseline.objects]

    # The existing Spark contract orders lineage by partition then trajectory;
    # compare every full record independent of that physical partition order.
    actual_lineage = [
        json.loads(line)
        for line in (output / "lineage.jsonl").read_bytes().splitlines()
    ]
    assert sorted(actual_lineage, key=lambda item: item["trajectory_id"]) == sorted(
        (item.lineage for item in expected), key=lambda item: item["trajectory_id"]
    )
    assert {
        origin["source_ref"] for item in actual_lineage for origin in item["origins"]
    } == {path for path, _payload in records}
    report = validate_output(output)
    assert report.valid, report.errors
