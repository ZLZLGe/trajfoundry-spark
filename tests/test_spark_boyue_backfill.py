from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest


@pytest.fixture
def supervisor(monkeypatch):
    directory = Path(__file__).resolve().parents[1] / "spark"
    monkeypatch.syspath_prepend(str(directory))
    spec = importlib.util.spec_from_file_location(
        "backfill_boyue", directory / "backfill_boyue.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def batch(supervisor, tmp_path):
    root, work = tmp_path / "state", tmp_path / "boyue-only"
    root.mkdir()
    work.mkdir()
    (work / "controllers").mkdir()
    options = SimpleNamespace(
        state_root=root, batch_root=work, config=tmp_path / "config.json"
    )
    plan = {
        "source": "boyue",
        "cutoff": "2026-10-31",
        "dates": ["2026-10-01", "2026-10-02"],
    }
    return supervisor.BoyueBatch(options, [], tmp_path / "frozen-release", Mock(), plan)


def completed_state(day):
    return {
        "source": "boyue",
        "date": day,
        "status": "succeeded",
        "final": {
            "job": {
                "input_files": 3,
                "trajectories": 2,
                "accepted": 1,
                "quarantined_trajectories": 1,
            }
        },
    }


def test_only_boyue_submitted_and_existing_dates_skipped(
    supervisor, batch, monkeypatch
):
    first, second = batch.plan["dates"]
    supervisor.atomic_json(
        batch.root / f"boyue-{second}.state.json", completed_state(second)
    )
    verify = Mock(return_value={"valid": True})
    monkeypatch.setattr(supervisor, "validate_result", verify)
    called = []

    def launch(command, **kwargs):
        assert command[command.index("--source") + 1] == "boyue"
        assert command[command.index("--cutoff") + 1] == "2026-10-31"
        assert command[1] == str(batch.release / "spark/backfill_lakehouse.py")
        day = command[command.index("--date") + 1]
        called.append(day)
        supervisor.atomic_json(
            batch.root / f"boyue-{day}.state.json", completed_state(day)
        )
        return Mock(wait=Mock(return_value=0))

    monkeypatch.setattr(supervisor.subprocess, "Popen", launch)
    assert batch.run() == 0
    assert called == [first]
    assert verify.call_count == 2
    report = supervisor.load(batch.batch / "status.json")
    assert report["source"] == "boyue" and report["counts"] == {"succeeded": 2}
    assert report["totals"]["trajectories"] == 4


def test_uncertain_remote_driver_blocks_following_dates(supervisor, batch, monkeypatch):
    called = []

    def launch(command, **kwargs):
        day = command[command.index("--date") + 1]
        called.append(day)
        supervisor.atomic_json(
            batch.root / f"boyue-{day}.state.json", {"status": "running"}
        )
        return Mock(wait=Mock(return_value=1))

    monkeypatch.setattr(supervisor.subprocess, "Popen", launch)
    with pytest.raises(RuntimeError, match="unreconciled_running_driver"):
        batch.run()
    assert called == [batch.plan["dates"][0]]
    assert supervisor.load(batch.batch / "status.json")["status"] == "blocked"
    assert batch.state(called[0])["status"] == "running"


def test_deepinfra_running_state_blocks_boyue_submission(
    supervisor, batch, monkeypatch
):
    supervisor.atomic_json(
        batch.root / "deepinfra-2026-10-03.state.json", {"status": "running"}
    )
    launch = Mock()
    monkeypatch.setattr(supervisor.subprocess, "Popen", launch)
    with pytest.raises(RuntimeError, match="unreconciled_running_driver"):
        batch.run()
    launch.assert_not_called()


def test_two_failed_business_attempts_are_not_retried_again(
    supervisor, batch, monkeypatch
):
    day = batch.plan["dates"][0]
    called = []

    def launch(command, **kwargs):
        called.append(command)
        kwargs["stdout"].write(json.dumps({"event": "inventory", "ready": True}) + "\n")
        kwargs["stdout"].flush()
        supervisor.atomic_json(
            batch.root / f"boyue-{day}.state.json",
            {
                "status": "failed",
                "attempt": len(called),
            },
        )
        return Mock(wait=Mock(return_value=1))

    monkeypatch.setattr(supervisor.subprocess, "Popen", launch)
    batch.run_date(day)
    assert batch.outcomes[day]["status"] == "failed"
    batch.run_date(day)
    assert batch.outcomes[day]["status"] == "blocked"
    batch.run_date(day)
    assert len(called) == 2


def test_conflicting_output_is_blocked_without_retry(supervisor, batch, monkeypatch):
    day = batch.plan["dates"][0]

    def launch(command, **kwargs):
        kwargs["stdout"].write(
            json.dumps(
                {
                    "event": "date_blocked",
                    "error_code": "unowned_nonempty_output_preserved",
                }
            )
            + "\n"
        )
        kwargs["stdout"].flush()
        return Mock(wait=Mock(return_value=1))

    monkeypatch.setattr(supervisor.subprocess, "Popen", launch)
    batch.run_date(day)
    assert batch.outcomes[day] == {
        "status": "blocked",
        "reason": "unowned_nonempty_output_preserved",
        "controller_errors": 1,
    }


def test_pause_during_inventory_never_starts_next_date(supervisor, batch, monkeypatch):
    process = Mock()
    calls = []

    def wait(timeout):
        if not batch.stopping:
            batch.stop()
            raise subprocess.TimeoutExpired("inventory", timeout)
        return -15

    process.wait.side_effect = wait
    monkeypatch.setattr(
        supervisor.subprocess,
        "Popen",
        lambda command, **kwargs: calls.append(command) or process,
    )
    assert batch.run() == 0
    process.terminate.assert_called_once()
    assert len(calls) == 1
    assert supervisor.load(batch.batch / "status.json")["status"] == "paused"


def test_dry_run_does_not_create_batch_or_submit(supervisor, tmp_path, monkeypatch):
    options = [
        "--config",
        str(tmp_path / "config.json"),
        "--state-root",
        str(tmp_path),
        "--batch-root",
        str(tmp_path / "new-batch"),
        "--cutoff",
        "2026-10-31",
    ]
    flag = lambda name: {
        "--endpoint-url": "https://example.invalid",
        "--region-name": "region",
        "--input-credentials-file": "/synthetic",
    }[name]
    monkeypatch.setattr(
        supervisor, "configuration", lambda _: ([], flag, tmp_path / "release")
    )
    monkeypatch.setattr(supervisor, "verify_acceptance", lambda _: None)
    monkeypatch.setattr(supervisor, "load_s3_credentials", lambda _: None)
    monkeypatch.setattr(supervisor, "create_s3_client", Mock())
    monkeypatch.setattr(supervisor, "discover_dates", lambda *args: ["2026-10-01"])
    launch = Mock()
    monkeypatch.setattr(supervisor.subprocess, "Popen", launch)
    assert supervisor.main(options) == 0
    assert not (tmp_path / "new-batch").exists()
    launch.assert_not_called()


def test_configuration_rejects_routing_override(supervisor, tmp_path):
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps({"submit_args": ["--job-arg=--input-format", "--job-arg=deepinfra"]})
    )
    with pytest.raises(ValueError, match="managed_argument_in_configuration"):
        supervisor.configuration(path)


@pytest.fixture
def output(supervisor, tmp_path):
    from trajfoundry.models import Snapshot
    from trajfoundry.output_contract import project_trajectory
    from trajfoundry.spark.adapters import parse_payload
    from trajfoundry.spark.build import build_scope
    from trajfoundry.spark.manifest import DiskManifestAccumulator
    from trajfoundry.spark.output import SparkObjectDescription

    day = "2026-10-01"
    input_uri, output_uri = supervisor.date_uris("boyue", day)
    raw = {
        "user_id": "synthetic",
        "relay_log_id": "request",
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
        "user_headers": {},
    }
    snapshot = Snapshot.model_validate_json(
        parse_payload(
            "00/r/raw.json", json.dumps(raw).encode(), input_format="boyue"
        ).snapshot_json
    )
    trajectory = project_trajectory(next(iter(build_scope([snapshot]))).node)
    payload = json.dumps(trajectory).encode()
    entry = SparkObjectDescription(
        "synthetic_sub_0.jsonl", hashlib.sha256(payload).hexdigest(), len(payload)
    )
    with DiskManifestAccumulator(
        tmp_path / "counts.sqlite",
        input_root=input_uri,
        input_format="boyue",
        config_hash="a" * 64,
    ) as accumulator:
        accumulator.stats.input_files = accumulator.stats.accepted = 1
        accumulator.add_object(entry)
        accumulator.write_manifest(tmp_path / "manifest.json")
    manifest = (tmp_path / "manifest.json").read_bytes()
    state = {
        "source": "boyue",
        "date": day,
        "status": "succeeded",
        "owner": "owner",
        "identity": {"input_uri": input_uri, "output_uri": output_uri},
        "final": {
            "passed": True,
            "job": {
                "output_uri": output_uri,
                "verified_trajectory_objects": 1,
                "trajectories": 1,
                "accepted": 1,
                "quarantined_trajectories": 0,
                "parse_failures": 0,
                "input_files": 1,
                "manifest_sha256": hashlib.sha256(manifest).hexdigest(),
            },
        },
    }
    values = {"manifest.json": manifest, entry.path: payload}
    client = Mock()

    def get_object(**kwargs):
        name = kwargs["Key"].rsplit("/", 1)[-1]
        return {
            "Body": io.BytesIO(values[name]),
            "Metadata": {"tf-owner": state["owner"]},
        }

    client.get_object.side_effect = get_object
    return client, state, values, trajectory


def test_independent_output_validation_includes_schema_and_tokens(supervisor, output):
    client, state, _, _ = output
    result = supervisor.validate_result(client, state)
    assert result["valid"] and result["sampled_trajectories"] == 1
    assert result["trajectories"] == 1
    client.put_object.assert_not_called()
    client.delete_object.assert_not_called()


@pytest.mark.parametrize(
    "problem", ["manifest_hash", "counts", "trajectory_hash", "owner", "field"]
)
def test_corrupt_or_conflicting_outputs_fail_validation(supervisor, output, problem):
    client, state, values, trajectory = output
    if problem == "manifest_hash":
        state["final"]["job"]["manifest_sha256"] = "b" * 64
    elif problem == "counts":
        state["final"]["job"]["accepted"] = 0
    elif problem == "trajectory_hash":
        values["synthetic_sub_0.jsonl"] += b"corrupt"
    elif problem == "owner":
        client.get_object.side_effect = None
        client.get_object.return_value = {
            "Body": io.BytesIO(values["manifest.json"]),
            "Metadata": {"tf-owner": "another-owner"},
        }
    else:
        trajectory["total_tokens"] = -1
        payload = json.dumps(trajectory).encode()
        values["synthetic_sub_0.jsonl"] = payload
        manifest = json.loads(values["manifest.json"])
        manifest["files"][0].update(
            sha256=hashlib.sha256(payload).hexdigest(), bytes=len(payload)
        )
        values["manifest.json"] = json.dumps(manifest).encode()
        state["final"]["job"]["manifest_sha256"] = hashlib.sha256(
            values["manifest.json"]
        ).hexdigest()
    with pytest.raises(ValueError):
        supervisor.validate_result(client, state)
    client.put_object.assert_not_called()
    client.delete_object.assert_not_called()
