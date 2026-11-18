from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "local_output_smoke", ROOT / "spark/local_output_smoke.py"
)
assert SPEC is not None and SPEC.loader is not None
smoke = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(smoke)


def test_smoke_cannot_create_output_s3_client():
    with pytest.raises(RuntimeError, match="must never create an output S3 client"):
        smoke.forbidden_output_s3_client("https://unused", "unused")


def test_smoke_requires_absolute_paths():
    assert smoke.absolute_path("/shared/output") == "/shared/output"
    with pytest.raises(argparse.ArgumentTypeError):
        smoke.absolute_path("relative/output")


def job_result():
    return {
        "input_files": 73,
        "parse_failures": 2,
        "accepted": 12,
        "quarantined_trajectories": 3,
        "quarantined_records": 1,
        "trajectories": 15,
    }


@pytest.mark.parametrize("mismatch", [False, True])
def test_inspection_checks_generic_counts_without_reading_trajectories(
    tmp_path, monkeypatch, mismatch
):
    from trajfoundry import validation

    counts = {**job_result(), "skipped_inputs": 2}
    if mismatch:
        counts["input_files"] += 1
    (tmp_path / "manifest.json").write_text(json.dumps({"counts": counts}))
    monkeypatch.setattr(
        validation,
        "validate_output",
        lambda _: SimpleNamespace(valid=True, errors=[], counts={"manifest_files": 16}),
    )
    result = smoke.inspect_output(tmp_path, job_result())
    assert result["manifest_counts_ok"] is (not mismatch)
    assert result["validation"] == {
        "valid": True,
        "error_count": 0,
        "counts": {"manifest_files": 16},
    }
    assert "errors" not in result["validation"]


def report():
    return {
        "master": "k8s://https://test-api:443",
        "deploy_mode": "client",
        "driver": {
            "hostname": "driver",
            "architecture": "x86_64",
            "python_major_minor": [3, 11],
            "uid": 123,
            "gid": 100,
        },
        "executors": [
            {
                "hostname": f"executor-{index}",
                "architecture": "aarch64",
                "python_major_minor": [3, 11],
                "uid": 123,
                "gid": 100,
                "partition": index,
                "items": 1,
            }
            for index in range(2)
        ],
    }


def test_distributed_evidence_accepts_two_compatible_executors():
    assert smoke.distributed_evidence(report())


def cluster_report():
    value = report()
    value.update(
        expected_deploy_mode="cluster",
        deploy_mode="client",
        submit_in_driver=True,
        driver_pod_name=value["driver"]["hostname"],
    )
    value["driver"]["architecture"] = "arm64"
    return value


def test_distributed_evidence_accepts_cluster_with_three_independent_arm_pods():
    assert smoke.distributed_evidence(cluster_report())


@pytest.mark.parametrize("raw_mode", ["client", "cluster"])
def test_cluster_mode_uses_automatic_driver_flag_and_pod_identity(raw_mode):
    value = cluster_report()
    value["deploy_mode"] = raw_mode
    assert smoke.effective_deploy_mode(value) == "cluster"
    assert smoke.distributed_evidence(value)


@pytest.mark.parametrize("flag", [None, False, "true", "false", "wrong"])
def test_pod_name_without_verified_driver_flag_is_not_cluster(flag):
    value = cluster_report()
    if flag is None:
        value.pop("submit_in_driver")
    else:
        value["submit_in_driver"] = flag
    assert smoke.effective_deploy_mode(value) == "client"
    assert not smoke.distributed_evidence(value)
    value["expected_deploy_mode"] = "client"
    assert smoke.distributed_evidence(value)


def test_raw_cluster_mode_without_automatic_flag_is_unverified():
    value = cluster_report()
    value.update(deploy_mode="cluster", submit_in_driver=False)
    assert smoke.effective_deploy_mode(value) == "unverified"
    assert not smoke.distributed_evidence(value)


def test_automatic_driver_flag_without_kubernetes_master_is_unverified():
    value = cluster_report()
    value["master"] = "local[2]"
    assert smoke.effective_deploy_mode(value) == "unverified"
    assert not smoke.distributed_evidence(value)


@pytest.mark.parametrize("role", ["driver", 0, 1])
@pytest.mark.parametrize("architecture", ["x86_64", "amd64", ""])
def test_cluster_rejects_wrong_architecture(role, architecture):
    value = cluster_report()
    runtime = value["driver"] if role == "driver" else value["executors"][role]
    runtime["architecture"] = architecture
    assert not smoke.distributed_evidence(value)


@pytest.mark.parametrize("pod_name", [None, "", "different-driver", "executor-0"])
def test_cluster_requires_driver_runtime_to_match_independent_pod(pod_name):
    value = cluster_report()
    value["driver_pod_name"] = pod_name
    assert smoke.effective_deploy_mode(value) == "unverified"
    assert not smoke.distributed_evidence(value)


@pytest.mark.parametrize(
    "expected_mode,actual_mode", [("client", "cluster"), ("cluster", "client")]
)
def test_distributed_evidence_rejects_deploy_mode_mismatch(expected_mode, actual_mode):
    value = cluster_report() if actual_mode == "cluster" else report()
    value["expected_deploy_mode"] = expected_mode
    assert not smoke.distributed_evidence(value)


@pytest.mark.parametrize(
    "failure", ["driver_host", "same_host", "uid", "version", "old_python"]
)
def test_cluster_keeps_existing_identity_and_python_guards(failure):
    value = cluster_report()
    if failure == "driver_host":
        value["executors"][0]["hostname"] = value["driver"]["hostname"]
    elif failure == "same_host":
        value["executors"][1]["hostname"] = value["executors"][0]["hostname"]
    elif failure == "uid":
        value["executors"][0]["uid"] += 1
    elif failure == "version":
        value["executors"][0]["python_major_minor"] = [3, 12]
    else:
        for runtime in [value["driver"], *value["executors"]]:
            runtime["python_major_minor"] = [3, 10]
    assert not smoke.distributed_evidence(value)


@pytest.mark.parametrize(
    "failure",
    ["same_host", "driver_host", "version", "uid", "local", "cluster", "items"],
)
def test_distributed_evidence_rejects_false_positive(failure):
    value = report()
    if failure == "same_host":
        value["executors"][1]["hostname"] = "executor-0"
    elif failure == "driver_host":
        value["executors"][1]["hostname"] = "driver"
    elif failure == "version":
        value["executors"][1]["python_major_minor"] = [3, 10]
    elif failure == "uid":
        value["executors"][1]["uid"] = 456
    elif failure == "local":
        value["master"] = "local[2]"
    elif failure == "cluster":
        value["deploy_mode"] = "cluster"
    else:
        value["executors"][1]["items"] = 0
    assert not smoke.distributed_evidence(value)


@pytest.mark.parametrize(
    "expected_mode,actual_mode,failure,failure_stage",
    [
        (None, "client", None, None),
        ("client", "client", None, None),
        ("cluster", "cluster", None, None),
        (None, "cluster", None, "spark_start"),
        ("client", "cluster", None, "spark_start"),
        ("cluster", "client", None, "spark_start"),
        (None, "client", "master", "spark_start"),
        ("cluster", "cluster", "driver_arch", "executor_runtime"),
        ("cluster", "cluster", "executor_arch", "executor_runtime"),
        ("cluster", "cluster", "pod_name", "spark_start"),
        ("cluster", "cluster", "missing_flag", "spark_start"),
        ("cluster", "cluster", "false_flag", "spark_start"),
        ("cluster", "cluster", "wrong_flag", "spark_start"),
        ("client", "client", "client_pod_name", None),
    ],
)
@pytest.mark.parametrize("credential_file", [None, "/shared/protected-input.env"])
def test_smoke_uses_real_reader_and_always_stops(
    monkeypatch,
    capsys,
    tmp_path,
    expected_mode,
    actual_mode,
    failure,
    failure_stage,
    credential_file,
):
    from trajfoundry.spark import job

    stopped = []
    calls = []
    runtime = cluster_report() if actual_mode == "cluster" else report()
    if failure == "master":
        runtime["master"] = "local[2]"
    elif failure == "driver_arch":
        runtime["driver"]["architecture"] = "x86_64"
    elif failure == "executor_arch":
        runtime["executors"][0]["architecture"] = "x86_64"
    elif failure == "pod_name":
        runtime["driver_pod_name"] = "different-driver"
    elif failure == "client_pod_name":
        runtime["driver_pod_name"] = runtime["driver"]["hostname"]
    spark_conf = {
        "spark.submit.deployMode": runtime["deploy_mode"],
        "spark.kubernetes.driver.pod.name": runtime.get("driver_pod_name", ""),
    }
    if runtime.get("submit_in_driver"):
        spark_conf["spark.kubernetes.submitInDriver"] = "true"
    if failure == "missing_flag":
        spark_conf.pop("spark.kubernetes.submitInDriver")
    elif failure in {"false_flag", "wrong_flag"}:
        spark_conf["spark.kubernetes.submitInDriver"] = (
            "false" if failure == "false_flag" else "wrong"
        )
    barrier_rdd = SimpleNamespace()
    barrier_rdd.barrier = lambda: barrier_rdd
    barrier_rdd.mapPartitions = lambda _fn: barrier_rdd
    barrier_rdd.collect = lambda: runtime["executors"]
    fake_spark = SimpleNamespace(
        sparkContext=SimpleNamespace(
            setLogLevel=lambda _level: None,
            applicationId="test-application",
            master=runtime["master"],
            pythonExec=sys.executable,
            getConf=lambda: SimpleNamespace(get=spark_conf.get),
            parallelize=lambda _rows, _partitions: barrier_rdd,
        ),
        stop=lambda: stopped.append(True),
    )
    pyspark = ModuleType("pyspark")
    sql = ModuleType("pyspark.sql")
    sql.SparkSession = SimpleNamespace(
        builder=SimpleNamespace(getOrCreate=lambda: fake_spark)
    )
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    monkeypatch.setattr(smoke, "runtime_report", lambda _: runtime["driver"])

    def run_job(config, **kwargs):
        calls.append((config, kwargs))
        return job_result()

    monkeypatch.setattr(job, "run_spark_job", run_job)
    monkeypatch.setattr(
        smoke,
        "inspect_output",
        lambda _path, _result: {
            "validation": {"valid": True},
            "manifest_counts_ok": True,
        },
    )
    arguments = [
        "--source-dir",
        str(ROOT / "src"),
        "--output-dir",
        str(tmp_path),
        "--input-uri",
        "s3://test/day/",
        "--prefix-shard",
        "user-one/",
        "--prefix-shard",
        "user-two/",
        "--endpoint-url",
        "http://test-s3",
        "--region-name",
        "test-region",
    ]
    if credential_file is not None:
        arguments.extend(["--input-credentials-file", credential_file])
    if expected_mode is not None:
        arguments.extend(["--expected-deploy-mode", expected_mode])
    status = smoke.main(arguments)
    assert status == (2 if failure_stage else 0)
    assert stopped == [True]
    result = json.loads(capsys.readouterr().out.removeprefix(smoke.MARKER))
    assert result["data_mode"] == "s3_readonly"
    assert result["expected_deploy_mode"] == (expected_mode or "client")
    assert result["deploy_mode"] == runtime["deploy_mode"]
    assert result["submit_in_driver"] is (
        spark_conf.get("spark.kubernetes.submitInDriver", "false") == "true"
    )
    if failure_stage is None:
        assert result["effective_deploy_mode"] == actual_mode
    assert result["passed"] is (failure_stage is None)
    if failure_stage:
        assert result["stage"] == failure_stage
        assert result["error_type"] == "RuntimeError"
        assert not calls
    else:
        config, kwargs = calls[0]
        assert config.input_uri == "s3://test/day/"
        assert config.prefix_shards == ("user-one/", "user-two/")
        assert config.endpoint_url == "http://test-s3"
        assert config.region_name == "test-region"
        assert config.input_credentials_path == credential_file
        assert config.output_uri == str(tmp_path)
        assert "source_reader" not in kwargs
        assert kwargs["client_factory"] is smoke.forbidden_output_s3_client
        assert config.spark_conf() == {}
