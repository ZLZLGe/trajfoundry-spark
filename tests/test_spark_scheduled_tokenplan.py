from __future__ import annotations

import importlib.util
import json
import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "scheduled_tokenplan", ROOT / "spark/run_scheduled_tokenplan.py"
)
assert spec and spec.loader
scheduled = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scheduled)


@pytest.fixture
def config(tmp_path, monkeypatch):
    monkeypatch.setattr(sys, "path", list(sys.path))
    template = tmp_path / "pod.json"
    template.write_text(
        json.dumps(
            {
                "kind": "Pod",
                "spec": {
                    "containers": [
                        {
                            "name": "spark-kubernetes",
                            "volumeMounts": [
                                {"name": "project", "mountPath": str(ROOT)},
                                {"name": "test", "mountPath": str(tmp_path)},
                            ],
                        }
                    ],
                    "volumes": [
                        {"name": n, "persistentVolumeClaim": {"claimName": n}}
                        for n in ("project", "test")
                    ],
                },
            }
        )
    )
    credentials = tmp_path / "credentials.json"
    credentials.write_text("credential-values-must-not-be-read-in-plan")
    credentials.chmod(0o600)
    arguments = [
        "--project-root",
        str(ROOT),
        "--profile",
        "half",
        "--arm-python",
        str(tmp_path / "arm/bin/python"),
        "--driver-pod-template",
        str(template),
        "--executor-pod-template",
        str(template),
        "--image",
        "example/spark:3.5",
        "--queue",
        "root.example",
        "--master",
        "k8s://https://example:443",
        "--namespace",
        "test",
        "--service-account",
        "test",
        "--ca-cert",
        str(tmp_path / "ca"),
        "--oauth-token-file",
        str(tmp_path / "token"),
        "--run-as-user",
        str(os.geteuid() or 1),
        "--auto-prefix-shards",
        "--endpoint-url",
        "https://s3.example",
        "--input-credentials-file",
        str(credentials),
        "--output-credentials-file",
        str(credentials),
    ]
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                "submit_args": arguments,
                "runs_parent": str(tmp_path),
                "input_base_uri": "s3://input/tokenplan/v002/",
                "output_base_uri": "s3://output/tokenplan/v005/",
            }
        )
    )
    return path


def cli(config, *extra):
    return ["--config", str(config), "--biz-date", "2026-09-15", *extra]


def forbidden(*args, **kwargs):
    raise AssertionError("unexpected external action")


@pytest.mark.parametrize("extra", [(), ("--plan",)])
def test_plan_reuses_source_validation_without_auth_network_or_directories(
    config, monkeypatch, capsys, extra
):
    original = Path.read_text

    def protected_read(path, *args, **kwargs):
        if path.name == "credentials.json":
            raise AssertionError("plan read credentials")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", protected_read)
    monkeypatch.setattr(scheduled, "check_storage", forbidden)
    monkeypatch.setattr(scheduled.subprocess, "Popen", forbidden)
    before = set(config.parent.iterdir())
    assert scheduled.main(cli(config, *extra)) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["event"] == "plan" and report["execute"] is False
    assert report["input_uri"] == "s3://input/tokenplan/v002/dt=2026-09-15/"
    assert report["output_uri"] == "s3://output/tokenplan/v005/dt=2026-09-15/"
    assert report["total_executor_cores"] == 256
    assert report["total_executor_memory_gib"] == 2048
    assert report["normalizer_revision"]
    assert set(config.parent.iterdir()) == before
    assert "credentials" not in report and "command" not in report


@pytest.mark.parametrize("input_format", ["tokenplan", "boyue", "deepinfra"])
@pytest.mark.parametrize("joined_flag", [False, True], ids=["split", "equals"])
def test_source_date_layout_is_kept_in_args_command_and_report(
    config, input_format, joined_flag
):
    value = json.loads(config.read_text())
    value["input_base_uri"] = f"s3://input/{input_format}/masked-raw/v002/"
    value["output_base_uri"] = f"s3://output/{input_format}/normalized/v001/"
    value["submit_args"].extend(
        [f"--input-format={input_format}"]
        if joined_flag
        else ["--input-format", input_format]
    )
    config.write_text(json.dumps(value))
    before = set(config.parent.iterdir())
    args, _, command, public = scheduled.make_schedule(config, "2026-09-15")
    partition = "2026-09-15" if input_format == "boyue" else "dt=2026-09-15"
    expected_input = value["input_base_uri"] + partition + "/"
    expected_output = value["output_base_uri"] + "dt=2026-09-15/"
    assert args.input_uri == public["input_uri"] == expected_input
    assert command[command.index("--input-uri") + 1] == expected_input
    assert args.output_uri == public["output_uri"] == expected_output
    assert command[command.index("--output-uri") + 1] == expected_output
    assert set(config.parent.iterdir()) == before


@pytest.mark.parametrize(
    "value",
    [
        "2026-9-15",
        "2026-09-1",
        "2026-02-30",
        "${biz_date}",
        "20260915",
        "2026-09-15/..",
        "2026-09-15T00:00:00",
    ],
)
def test_strict_explicit_business_date(config, value, capsys):
    assert scheduled.main(["--config", str(config), "--biz-date", value]) == 2
    assert json.loads(capsys.readouterr().out)["error_code"] == "invalid_biz_date"


@pytest.mark.parametrize(
    "flag",
    [
        "--execute",
        "--control-dir=/bad",
        "--input-uri=s3://bad/",
        "--output-uri=s3://bad/",
        "--output-dir=/bad",
        "--allow-existing-output",
        "--out=s3://bad/",
        "--allow-e",
        "--job-arg=--allow-existing-output",
        "--job-arg=--allow-ex",
        "-h",
        "--help",
    ],
)
def test_config_cannot_override_execution_or_date_routing(config, flag, capsys):
    value = json.loads(config.read_text())
    value["submit_args"].append(flag)
    config.write_text(json.dumps(value))
    assert scheduled.main(cli(config)) == 2
    report = json.loads(capsys.readouterr().out)
    assert report["error_code"] == "managed_flag_in_config"


def test_missing_date_and_unknown_arguments_are_safe(config, capsys):
    assert scheduled.main(["--config", str(config)]) == 2
    assert scheduled.main(cli(config, "--private-password=DO_NOT_PRINT")) == 2
    captured = capsys.readouterr()
    assert "DO_NOT_PRINT" not in captured.out + captured.err


def test_uid_mismatch_before_storage_or_mkdir(config, monkeypatch, capsys):
    monkeypatch.setattr(scheduled.os, "geteuid", lambda: 999999)
    monkeypatch.setattr(scheduled, "check_storage", forbidden)
    monkeypatch.setattr(scheduled, "execute", forbidden)
    before = set(config.parent.iterdir())
    assert scheduled.main(cli(config, "--execute")) == 2
    report = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert report["error_code"] == "submitter_pod_uid_mismatch"
    assert set(config.parent.iterdir()) == before


@pytest.mark.parametrize(
    "case", ["ok", "output_exists", "input_empty", "input_marker_only"]
)
def test_storage_preflight_only_lists_and_closes_clients(config, monkeypatch, case):
    from trajfoundry import credentials, s3

    args, *_ = scheduled.make_schedule(config, "2026-09-15")
    clients = []

    class Client:
        closed = False

        def list_objects_v2(self, **kwargs):
            assert kwargs == {
                "Bucket": "output",
                "Prefix": "tokenplan/v005/dt=2026-09-15/",
                "MaxKeys": 1,
            }
            return (
                {"Contents": [{"Key": "existing", "Size": 0}]}
                if case == "output_exists"
                else {}
            )

        def get_paginator(self, name):
            assert name == "list_objects_v2"
            return self

        def paginate(self, **kwargs):
            assert kwargs["Bucket"] == "input"
            objects = [] if case == "input_empty" else [{"Key": "folder/", "Size": 0}]
            if case == "ok":
                objects.append({"Key": "folder/a.json", "Size": 12})
            return [{"Contents": objects}]

        def close(self):
            self.closed = True

    def make_client(*args, **kwargs):
        client = Client()
        clients.append(client)
        return client

    monkeypatch.setattr(credentials, "load_s3_credentials", lambda _: object())
    monkeypatch.setattr(s3, "create_s3_client", make_client)
    if case == "ok":
        scheduled.check_storage(args)
    else:
        with pytest.raises(scheduled.ScheduledError) as error:
            scheduled.check_storage(args)
        assert error.value.code == (
            "output_not_empty" if case == "output_exists" else "input_empty"
        )
    assert clients and all(c.closed for c in clients)


@pytest.mark.parametrize(
    "child_exit,final,expected",
    [
        (0, {"passed": True, "exit_code": 0}, 0),
        (1, {"passed": True, "exit_code": 0}, 2),
        (0, {"passed": False, "exit_code": 0}, 2),
        (0, {"passed": True, "exit_code": 1}, 2),
        (0, {"passed": True, "exit_code": False}, 2),
        (0, None, 2),
    ],
)
def test_execute_waits_for_real_completion_and_preserves_environment(
    config, monkeypatch, capsys, child_exit, final, expected
):
    monkeypatch.setattr(scheduled, "check_storage", lambda _: None)
    monkeypatch.setenv("SPARK_HOME", "/opt/soft/spark")
    monkeypatch.setenv("JAVA_HOME", "/worker/jdk")
    monkeypatch.setenv("PYSPARK_PYTHON", "/wrong/python")
    monkeypatch.setenv("PYSPARK_DRIVER_PYTHON", "/wrong/driver")
    calls = []

    def popen(command, **kwargs):
        calls.append((command, kwargs))
        control = Path(command[command.index("--control-dir") + 1])
        assert not control.exists()
        control.mkdir()
        (control / "driver.log").write_text(
            'private-error-DO_NOT_PRINT\nTF_SPARK_PROGRESS {"phase":"build_publish"}\n'
        )
        if final is not None:
            (control / "final.json").write_text(json.dumps(final))

        class Process:
            waits = 0

            def wait(self, timeout):
                assert timeout == 60
                self.waits += 1
                if self.waits == 1:
                    raise subprocess.TimeoutExpired(command, timeout)
                return child_exit

        return Process()

    monkeypatch.setattr(scheduled.subprocess, "Popen", popen)
    assert scheduled.main(cli(config, "--execute")) == expected
    command, kwargs = calls[0]
    assert command[0] == sys.executable
    assert command[1] == str(ROOT / "spark/submit_cluster.py")
    assert command[-1] == "--execute"
    environment = kwargs["env"]
    assert environment["SPARK_HOME"] == "/opt/spark"
    assert environment["JAVA_HOME"] == "/worker/jdk"
    assert environment["PYTHONPATH"] == str(ROOT / "src")
    assert environment["PYTHONUNBUFFERED"] == "1"
    assert (
        "PYSPARK_PYTHON" not in environment
        and "PYSPARK_DRIVER_PYTHON" not in environment
    )
    run_dir = Path(command[command.index("--control-dir") + 1]).parent
    assert stat.S_IMODE(run_dir.stat().st_mode) == 0o700
    assert json.loads((run_dir / "arguments.json").read_text()) == command[2:]
    assert (run_dir / "launcher.log").is_file()
    output = capsys.readouterr().out
    assert 'TF_SPARK_PROGRESS {"phase": "build_publish"}' in output
    assert "DO_NOT_PRINT" not in output


def test_sdk_errors_are_sanitized_and_preflight_does_not_create_run(
    config, monkeypatch, capsys
):
    def fail(_):
        error = RuntimeError("credential-secret-DO_NOT_PRINT")
        error.code = "provider-code-DO_NOT_PRINT"
        raise error

    monkeypatch.setattr(scheduled, "check_storage", fail)
    before = set(config.parent.iterdir())
    assert scheduled.main(cli(config, "--execute")) == 2
    output = capsys.readouterr().out
    assert "RuntimeError" in output and "storage_preflight" in output
    assert "DO_NOT_PRINT" not in output
    assert set(config.parent.iterdir()) == before
