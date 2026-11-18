from __future__ import annotations

import importlib.util
import json
import os
import platform
import socket
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def load_script(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "spark" / f"{name}.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


driver = load_script("scheduler_cluster_job")
normalize = load_script("tokenplan_normalize")
POD = "trajfoundry-cluster-" + "a" * 16
UID = 607031114


@pytest.fixture
def invocation(monkeypatch):
    monkeypatch.setattr(driver.os, "geteuid", lambda: UID)
    monkeypatch.setattr(driver.socket, "gethostname", lambda: POD)
    monkeypatch.setenv("TF_CLUSTER_EXPECTED_UID", str(UID))
    monkeypatch.setenv("TF_CLUSTER_DRIVER_POD", POD)
    return [
        "--tf-run-id",
        "a" * 32,
        "--tf-driver-pod",
        POD,
        "--tf-pod-uid",
        str(UID),
        "--input-uri",
        "s3://input/tokenplan/dt=2026-09-15/",
        "--output-uri",
        "s3://output/tokenplan/dt=2026-09-15/",
        "--input-credentials-file",
        "/private/input.json",
        "--output-credentials-file",
        "/private/output.json",
        "--endpoint-url",
        "https://s3.example",
        "--auto-prefix-shards",
    ]


@pytest.fixture
def job_result():
    return {
        "output_backend": "s3",
        "output_uri": "s3://output/tokenplan/dt=2026-09-15/",
        "manifest_sha256": "b" * 64,
        "input_files": 5,
        "trajectories": 3,
        "accepted": 2,
        "quarantined_trajectories": 1,
        "verified_trajectory_objects": 3,
    }


def support(monkeypatch, job_result, *, preflight=None, verify=None):
    calls = []

    def check(args):
        calls.append("preflight")
        assert os.geteuid() == UID
        if preflight:
            preflight(args)

    def run(argv):
        calls.append("normalize")
        assert not any(flag.startswith("--tf-") for flag in argv)
        return job_result

    def manifest(args, output_uri):
        calls.append("manifest")
        assert output_uri == job_result["output_uri"]
        return verify(args) if verify else job_result["manifest_sha256"]

    monkeypatch.setattr(normalize, "main", run)
    monkeypatch.setattr(driver, "load_support", lambda: (normalize, check, manifest))
    return calls


def test_pod_checks_storage_then_runs_existing_job_and_verifies_actual_manifest(
    invocation, job_result, monkeypatch, capsys
):
    calls = support(monkeypatch, job_result)
    assert driver.main(invocation) == 0
    output = capsys.readouterr().out.splitlines()
    assert calls == ["preflight", "normalize", "manifest"]
    assert len(output) == 1 and output[0].startswith("TF_CLUSTER_COMPLETION ")
    assert json.loads(output[0].partition(" ")[2]) == {
        "run_id": "a" * 32,
        "driver_pod": POD,
        "uid": UID,
        "output_uri": job_result["output_uri"],
        "manifest_sha256": "b" * 64,
        "input_files": 5,
        "trajectories": 3,
        "passed": True,
    }


@pytest.mark.parametrize(
    "field,value",
    [
        ("output_backend", "filesystem"),
        ("output_uri", "s3://unexpected/"),
        ("input_files", -1),
        ("trajectories", True),
        ("accepted", 99),
        ("quarantined_trajectories", -1),
        ("verified_trajectory_objects", 1),
        ("manifest_sha256", "not-a-hash"),
    ],
)
def test_invalid_job_result_cannot_emit_completion(
    invocation, job_result, monkeypatch, capsys, field, value
):
    job_result[field] = value
    calls = support(monkeypatch, job_result)
    assert driver.main(invocation) == 2
    output = capsys.readouterr().out
    assert calls == ["preflight", "normalize"]
    assert "TF_CLUSTER_COMPLETION" not in output
    assert "TF_CLUSTER_JOB_ERROR" in output


def test_manifest_must_be_fetched_and_match_returned_sha(
    invocation, job_result, monkeypatch, capsys
):
    calls = support(monkeypatch, job_result, verify=lambda _: "c" * 64)
    assert driver.main(invocation) == 2
    assert calls == ["preflight", "normalize", "manifest"]
    assert "manifest_hash_mismatch" in capsys.readouterr().out


@pytest.mark.parametrize("stage", ["preflight", "manifest"])
def test_sdk_errors_are_sanitized_and_never_emit_success(
    invocation, job_result, monkeypatch, capsys, stage
):
    def fail(_):
        error = RuntimeError("private-credential-DO_NOT_PRINT")
        error.code = "provider-secret-DO_NOT_PRINT"
        raise error

    calls = support(
        monkeypatch, job_result, **{stage if stage == "preflight" else "verify": fail}
    )
    assert driver.main(invocation) == 2
    assert calls == (
        ["preflight"]
        if stage == "preflight"
        else ["preflight", "normalize", "manifest"]
    )
    output = capsys.readouterr().out
    assert "DO_NOT_PRINT" not in output and "TF_CLUSTER_COMPLETION" not in output
    assert "RuntimeError" in output


@pytest.mark.parametrize(
    "change", ["uid", "hostname", "env_uid", "env_pod", "run_id", "duplicate_run_id"]
)
def test_identity_is_bound_to_actual_pod_before_any_s3_access(
    invocation, job_result, monkeypatch, capsys, change
):
    calls = support(monkeypatch, job_result)
    if change == "uid":
        monkeypatch.setattr(driver.os, "geteuid", lambda: 1007)
    elif change == "hostname":
        monkeypatch.setattr(driver.socket, "gethostname", lambda: "other-pod")
    elif change == "env_uid":
        monkeypatch.setenv("TF_CLUSTER_EXPECTED_UID", "1007")
    elif change == "env_pod":
        monkeypatch.setenv("TF_CLUSTER_DRIVER_POD", "other-pod")
    elif change == "run_id":
        invocation[1] = "invalid"
    else:
        invocation.extend(["--tf-run-id", "b" * 32])
    assert driver.main(invocation) == 2
    assert calls == []
    assert "TF_CLUSTER_COMPLETION" not in capsys.readouterr().out


@pytest.mark.parametrize(
    "extra",
    [
        ["--allow-existing-output"],
        ["--secret=DO_NOT_PRINT"],
        ["--shuffle-partitions", "DO_NOT_PRINT"],
        ["--help"],
    ],
)
def test_invalid_business_arguments_fail_quietly_before_s3(
    invocation, job_result, monkeypatch, capsys, extra
):
    calls = support(monkeypatch, job_result)
    assert driver.main(invocation + extra) == 2
    captured = capsys.readouterr()
    assert calls == []
    assert "DO_NOT_PRINT" not in captured.out + captured.err
    assert not captured.err and "TF_CLUSTER_COMPLETION" not in captured.out


def test_normalizer_main_accepts_explicit_argv_and_returns_unchanged_result(
    invocation, job_result, monkeypatch, capsys
):
    configs = []

    def run(config):
        configs.append(config)
        return job_result

    monkeypatch.setattr(normalize, "run_spark_job", run)
    assert (
        normalize.main(invocation[6:] + ["--shuffle-partitions", "512"]) is job_result
    )
    assert configs[0].shuffle_partitions == 512
    assert configs[0].auto_prefix_shards
    assert json.loads(capsys.readouterr().out) == job_result


@pytest.mark.parametrize(
    "architecture,expected_uid,passed",
    [
        ("aarch64", UID, True),
        ("x86_64", UID, False),
        ("arm64", 1007, False),
    ],
)
def test_stdout_wrapper_runtime_checks_without_private_log_redirection(
    monkeypatch, capsys, architecture, expected_uid, passed
):
    path = ROOT / "spark/scheduler_driver_entrypoint.sh"
    source = path.read_text()
    assert "TF_CLUSTER_LOG_PATH" not in source and "exec >" not in source
    assert subprocess.run(["bash", "-n", str(path)], check=False).returncode == 0
    python = source.split('"$TF_CLUSTER_PYTHON" -c \'\n', 1)[1].split(
        "\n'\n", 1
    )[0]
    monkeypatch.setattr(platform, "machine", lambda: architecture)
    monkeypatch.setattr(socket, "gethostname", lambda: POD)
    monkeypatch.setattr(os, "geteuid", lambda: UID)
    monkeypatch.setenv("TF_CLUSTER_SOURCE_DIR", str(ROOT / "src"))
    monkeypatch.setenv("TF_CLUSTER_EXPECTED_UID", str(expected_uid))
    monkeypatch.setenv("TF_CLUSTER_DRIVER_POD", POD)
    monkeypatch.setenv("SPARK_DRIVER_BIND_ADDRESS", "10.112.1.23")
    with pytest.raises(SystemExit) as exit_info:
        exec(compile(python, str(path), "exec"), {})  # noqa: S102 - repository-owned inline probe
    assert exit_info.value.code == (0 if passed else 2)
    output = capsys.readouterr().out
    assert output.startswith("TF_CLUSTER_DRIVER_RUNTIME ")
    report = json.loads(output.partition(" ")[2])
    assert report["passed"] is passed
    assert report["driver_ip"] == "10.112.1.23"
