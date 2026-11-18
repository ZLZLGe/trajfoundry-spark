"""Hermetic Spark scheduler checks; no cluster or business S3 is accessed."""

from __future__ import annotations

import json
import time
from functools import partial
from itertools import chain
from pathlib import Path

from trajfoundry.spark import job
from trajfoundry.spark.config import SparkNormalizeConfig
from trajfoundry.validation import validate_output

from .test_spark_adapters import _tokenplan_envelope
from .test_spark_job import local_spark as local_spark  # noqa: PLC0414 - pytest fixture
from .test_spark_job import synthetic_reader


def _forbidden_output_client(*_args, **_kwargs):
    raise AssertionError("filesystem output must not create an S3 client")


def test_build_publish_tasks_run_concurrently_and_cached_receipts_do_not_republish(
    local_spark, tmp_path, monkeypatch
) -> None:
    # Ask the actual worker hash implementation for one real scope in each
    # partition. This avoids depending on Driver/worker PYTHONHASHSEED values.
    buckets = (
        local_spark.sparkContext.parallelize(
            [f"parallel-{index}" for index in range(32)], 2
        )
        .map(
            lambda value: (job._scope_partition(("session", value, "whole")) % 2, value)
        )
        .collect()
    )
    selected = {}
    for partition_id, session in buckets:
        selected.setdefault(partition_id, session)
    assert set(selected) == {0, 1}
    records = [
        (
            f"input/req_{index}.json",
            json.dumps(_tokenplan_envelope(session_id=session)).encode(),
        )
        for index, session in sorted(selected.items())
    ]

    gate = tmp_path / "task-gate"
    gate.mkdir()
    gate_path = str(gate)
    original_publish = job._publish_partition

    def rendezvous_publish(partition_id, artifacts, **kwargs):
        # Consume a real trajectory first, not merely an empty partition. The
        # first task cannot finish until the other build task is also active.
        iterator = iter(artifacts)
        first = next(iterator)
        assert first[0] == "trajectory"
        directory = Path(gate_path)
        with (directory / f"started-{partition_id}").open("x") as handle:
            handle.write("started\n")
        deadline = time.monotonic() + 30
        while not all((directory / f"started-{index}").is_file() for index in (0, 1)):
            if time.monotonic() >= deadline:
                raise AssertionError("build/publish partitions were scheduled serially")
            time.sleep(0.02)
        for receipt in original_publish(
            partition_id, chain([first], iterator), **kwargs
        ):
            # Exclusive markers also detect repeated execution when the Driver
            # reads toLocalIterator after the distributed completion action.
            with (directory / f"finished-{partition_id}").open("x") as handle:
                handle.write("finished\n")
            yield receipt

    monkeypatch.setattr(job, "_publish_partition", rendezvous_publish)
    output = tmp_path / "output"
    result = job.run_spark_job(
        SparkNormalizeConfig(
            input_uri="s3://synthetic/input/",
            output_uri=str(output),
            prefix_shards=("input/",),
            default_parallelism=2,
            shuffle_partitions=2,
        ),
        spark=local_spark,
        source_reader=partial(synthetic_reader, records=records),
        client_factory=_forbidden_output_client,
    )

    assert result["input_files"] == 2
    assert result["trajectories"] == 2
    assert sorted(path.name for path in gate.iterdir()) == [
        "finished-0",
        "finished-1",
        "started-0",
        "started-1",
    ]
    assert validate_output(output).valid
