from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from trajfoundry.spark.backfill import date_uris, readiness, shared_lock
from trajfoundry.spark.storage import S3OutputStore


def observation():
    return {
        "observed_at": 2_000_000_000,
        "latest_modified": 1_000_000_000,
        "full_fingerprint": "a",
        "upstream_manifest_present": False,
        "upstream_succeeded": False,
    }


def test_readiness_requires_complete_stable_inventories_or_upstream_success():
    current = observation()
    previous = {**current, "observed_at": current["observed_at"] - 1800}
    assert readiness(current, previous, day="2026-08-14", now=current["observed_at"])[0]
    assert not readiness(
        current,
        {**previous, "full_fingerprint": "changed"},
        day="2026-08-14",
        now=current["observed_at"],
    )[0]
    assert not readiness(
        current,
        {**previous, "observed_at": current["observed_at"] - 1799},
        day="2026-08-14",
        now=current["observed_at"],
    )[0]
    assert not readiness(current, None, day="2026-08-14", now=current["observed_at"])[0]
    assert not readiness(
        {**current, "latest_modified": current["observed_at"] - 60},
        previous,
        day="2026-08-14",
        now=current["observed_at"],
    )[0]
    assert readiness(
        {**current, "upstream_manifest_present": True, "upstream_succeeded": True},
        None,
        day="2026-08-14",
    )[0]
    assert not readiness(
        {**current, "upstream_manifest_present": True}, previous, day="2026-08-14"
    )[0]


def test_date_paths_do_not_change_old_deepinfra_version():
    assert date_uris("boyue", "2026-10-28") == (
        "s3://agent-trajectory/lakehouse/boyue-rich/masked-raw/v002/2026-10-28/",
        "s3://agent-trajectory/lakehouse/boyue-rich/normalized/v001/dt=2026-10-28/",
    )
    assert date_uris("deepinfra", "2026-10-23")[1].endswith(
        "deep-infra/normalized/v002/dt=2026-10-23/"
    )
    with pytest.raises(ValueError):
        date_uris("boyue", "../../bad")


def test_shared_lock_excludes_another_controller(tmp_path):
    path = tmp_path / "global.lock"
    with shared_lock(path), pytest.raises(BlockingIOError), shared_lock(path):
        pytest.fail("overlapping controllers acquired the same lock")
    with shared_lock(path):
        pass


def owned_client(*, owner="a" * 64, name="session_sub_0.jsonl", payload=b"same"):
    client = Mock()
    client.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Key": "out/" + name}]}
    ]
    client.head_object.return_value = {
        "Metadata": {"tf-owner": owner, "sha256": hashlib.sha256(payload).hexdigest()},
        "ContentLength": len(payload),
    }
    return client


def test_owned_retry_accepts_identical_bytes_only():
    client = owned_client()
    store = S3OutputStore(
        "s3://bucket/out/", client, output_owner="a" * 64, resume_owned_output=True
    )
    store.initialize_run("run")
    store.put_bytes("session_sub_0.jsonl", b"same")
    client.put_object.assert_not_called()
    with pytest.raises(FileExistsError):
        store.put_bytes("session_sub_0.jsonl", b"different")
    client.delete_object.assert_not_called()
    client.put_object.assert_not_called()


@pytest.mark.parametrize(
    "owner,name", [("b" * 64, "session_sub_0.jsonl"), ("a" * 64, "manifest.json")]
)
def test_retry_preserves_unowned_and_completed_outputs(owner, name):
    client = owned_client(owner=owner, name=name)
    store = S3OutputStore(
        "s3://bucket/out/", client, output_owner="a" * 64, resume_owned_output=True
    )
    with pytest.raises(FileExistsError):
        store.initialize_run("run")
    client.put_object.assert_not_called()
    client.delete_object.assert_not_called()


@pytest.fixture
def controller(monkeypatch):
    directory = Path(__file__).resolve().parents[1] / "spark"
    monkeypatch.syspath_prepend(str(directory))
    spec = importlib.util.spec_from_file_location(
        "backfill_lakehouse", directory / "backfill_lakehouse.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "phase,finished",
    [("Running", False), ("Pending", False), ("Failed", True), ("Succeeded", True)],
)
def test_failed_submitter_requires_terminal_owned_driver(
    controller, monkeypatch, tmp_path, phase, finished
):
    import validate_cluster

    (tmp_path / "plan.json").write_text(
        json.dumps({"driver_pod": "driver", "run_id": "run"})
    )
    pod = {
        "metadata": {
            "name": "driver",
            "namespace": "ns",
            "uid": "uid",
            "labels": {"spark-role": "driver", "trajfoundry-run-id": "run"},
        },
        "status": {"phase": phase},
    }
    fetch = Mock(return_value=pod)
    monkeypatch.setattr(validate_cluster, "fetch_driver_pod", fetch)
    args = SimpleNamespace(namespace="ns")
    assert controller.submission_has_finished(args, tmp_path, {}) is finished
    pod["metadata"]["labels"]["trajfoundry-run-id"] = "another-run"
    assert not controller.submission_has_finished(args, tmp_path, {})
    fetch.side_effect = OSError("API unavailable")
    assert not controller.submission_has_finished(args, tmp_path, {})
    assert controller.submission_has_finished(
        args, tmp_path, {"cleanup_required": False}
    )


def test_controller_stops_all_dates_after_uncertain_submission(
    controller, monkeypatch, tmp_path
):
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {
                "submit_args": [
                    "--endpoint-url",
                    "https://example.invalid",
                    "--region-name",
                    "region",
                    "--input-credentials-file",
                    "/synthetic/credentials",
                ]
            }
        )
    )
    monkeypatch.setattr(controller, "load_s3_credentials", lambda _: None)
    monkeypatch.setattr(controller, "create_s3_client", Mock())
    called = []

    def uncertain(client, options, fixed, source, day):
        called.append(day)
        controller.atomic_json(
            options.state_root / f"{source}-{day}.state.json", {"status": "running"}
        )
        raise OSError("submitter lost contact with its Driver")

    monkeypatch.setattr(controller, "run_date", uncertain)
    with pytest.raises(RuntimeError, match="unreconciled_running_driver"):
        controller.main(
            [
                "--config",
                str(config),
                "--state-root",
                str(tmp_path / "state"),
                "--source",
                "boyue",
                "--date",
                "2026-10-01",
                "--date",
                "2026-10-02",
                "--cutoff",
                "2026-10-31",
                "--execute",
            ]
        )
    assert called == ["2026-10-01"]
