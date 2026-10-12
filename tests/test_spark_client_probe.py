from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "client_probe", ROOT / "spark/client_probe.py"
)
assert SPEC is not None and SPEC.loader is not None
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)


def report():
    return {
        "master": "k8s://https://api.example:443",
        "deploy_mode": "client",
        "executor_python_command": "/arm311/bin/python",
        "driver": {"hostname": "jupyter", "ready": True, "python_major_minor": [3, 11]},
        "executors": [
            {
                "hostname": "executor-1",
                "ready": True,
                "python_major_minor": [3, 11],
                "partition": 0,
                "sum": 1225,
            },
            {
                "hostname": "executor-2",
                "ready": True,
                "python_major_minor": [3, 11],
                "partition": 1,
                "sum": 3725,
            },
        ],
    }


def test_synthetic_report_requires_distributed_and_runtime_evidence():
    assert probe.evaluate_report(
        report(), expected_executor_python="/arm311/bin/python"
    )["passed"]


@pytest.mark.parametrize(
    "failure",
    [
        "same_host",
        "driver_host",
        "wrong_version",
        "wrong_python",
        "bad_dependency",
        "cluster_mode",
        "bad_sum",
    ],
)
def test_report_rejects_incomplete_evidence(failure):
    value = report()
    if failure == "same_host":
        value["executors"][1]["hostname"] = "executor-1"
    elif failure == "driver_host":
        value["executors"][1]["hostname"] = "jupyter"
    elif failure == "wrong_version":
        value["executors"][1]["python_major_minor"] = [3, 10]
    elif failure == "wrong_python":
        value["executor_python_command"] = "/x86/bin/python"
    elif failure == "bad_dependency":
        value["executors"][1]["ready"] = False
    elif failure == "cluster_mode":
        value["deploy_mode"] = "cluster"
    else:
        value["executors"][1]["sum"] = 0
    assert not probe.evaluate_report(
        value, expected_executor_python="/arm311/bin/python"
    )["passed"]


def test_venvs_sharing_a_base_interpreter_are_not_treated_as_equal(tmp_path):
    first = tmp_path / "first-python"
    second = tmp_path / "second-python"
    first.symlink_to(sys.executable)
    second.symlink_to(sys.executable)
    assert not probe.same_executable(str(first), str(second))


@pytest.mark.parametrize("failure", [None, "wrong_project", "pydantic_v1"])
def test_runtime_probe_checks_project_origin_and_dependencies(
    tmp_path, monkeypatch, failure
):
    source_dir = tmp_path / "spark-project" / "src"
    source_dir.mkdir(parents=True)
    package_origin = source_dir / "trajfoundry" / "__init__.py"
    if failure == "wrong_project":
        package_origin = (
            tmp_path / "python-project" / "src" / "trajfoundry" / "__init__.py"
        )
    imported = []

    def import_module(name):
        imported.append(name)
        return SimpleNamespace(
            __file__=str(package_origin),
            __version__="1.10.0"
            if name == "pydantic" and failure == "pydantic_v1"
            else "2.0.0",
        )

    monkeypatch.setattr(probe.importlib, "import_module", import_module)
    monkeypatch.setattr(sys, "path", list(sys.path))
    actual = probe.check_runtime(
        "driver",
        source_dir=str(source_dir),
        expected_python=sys.executable,
        expected_architecture=probe.platform.machine(),
    )
    assert actual["ready"] is (failure is None)
    if failure == "wrong_project":
        assert actual["project_error_type"] == "ValueError"
        assert "trajfoundry.spark.job" not in imported
    if failure == "pydantic_v1":
        assert not actual["dependencies"]["pydantic"]["ok"]


def test_submission_script_configures_client_without_starting_spark(tmp_path):
    fake_submit = tmp_path / "fake-spark-submit"
    fake_submit.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "print(json.dumps({'args': sys.argv[1:], 'driver_env': os.environ.get('PYSPARK_DRIVER_PYTHON'), 'executor_env': os.environ.get('PYSPARK_PYTHON')}))\n"
    )
    fake_submit.chmod(0o755)
    template = tmp_path / "pod.yaml"
    template.write_text("apiVersion: v1\nkind: Pod\n")
    ca = tmp_path / "public-ca.crt"
    ca.write_text("placeholder for launcher argument validation\n")
    env = {
        **os.environ,
        "PYSPARK_DRIVER_PYTHON": "unused-driver",
        "PYSPARK_PYTHON": "unused-executor",
    }
    result = subprocess.run(
        [
            "bash",
            str(ROOT / "spark/submit_client_probe.sh"),
            "--spark-submit",
            str(fake_submit),
            "--project-root",
            str(ROOT),
            "--driver-python",
            sys.executable,
            "--executor-python",
            "/arm311/bin/python",
            "--executor-pod-template",
            str(template),
            "--executor-queue",
            "root.example.test",
            "--master",
            "k8s://https://api.example:443",
            "--namespace",
            "test-namespace",
            "--driver-host",
            "10.0.0.10",
            "--ca-cert",
            str(ca),
            "--executor-image",
            "example/spark:arm311",
            "--oauth-token-file",
            str(ca),
            "--executor-service-account",
            "spark-example",
            "--driver-pod-name",
            "jupyter-example",
            "--driver-port",
            "32001",
            "--block-manager-port",
            "32002",
        ],
        env=env,
        text=True,
        capture_output=True,
        check=True,
    )
    actual = json.loads(result.stdout)
    args = actual["args"]
    settings = dict(
        args[index + 1].split("=", 1)
        for index, value in enumerate(args)
        if value == "--conf"
    )
    assert args[args.index("--deploy-mode") + 1] == "client"
    assert settings["spark.pyspark.driver.python"] == sys.executable
    assert settings["spark.pyspark.python"] == "/arm311/bin/python"
    assert settings["spark.kubernetes.trust.certificates"] == "false"
    assert settings["spark.kubernetes.authenticate.caCertFile"] == str(ca)
    assert settings["spark.executor.instances"] == "2"
    assert settings["spark.executor.cores"] == "1"
    assert settings["spark.executor.memory"] == "2g"
    assert settings["spark.executor.memoryOverhead"] == "2g"
    assert settings["spark.kubernetes.executor.label.queue"] == "root.example.test"
    assert (
        settings["spark.kubernetes.executor.container.image"] == "example/spark:arm311"
    )
    assert settings["spark.kubernetes.authenticate.oauthTokenFile"] == str(ca)
    assert (
        settings["spark.kubernetes.authenticate.executor.serviceAccountName"]
        == "spark-example"
    )
    assert settings["spark.kubernetes.driver.pod.name"] == "jupyter-example"
    assert settings["spark.driver.port"] == "32001"
    assert settings["spark.blockManager.port"] == "32002"
    assert actual["driver_env"] is None and actual["executor_env"] is None
    assert env["PYSPARK_PYTHON"] == "unused-executor"
    assert not any(value.startswith("s3://") for value in args)


def test_submission_requires_parameters_before_running_anything():
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("TF_SPARK_")
    }
    result = subprocess.run(
        ["bash", str(ROOT / "spark/submit_client_probe.sh")],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 2
    assert "--master" in result.stderr
