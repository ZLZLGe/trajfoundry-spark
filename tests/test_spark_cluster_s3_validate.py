from __future__ import annotations

import hashlib
import importlib.util
import io
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "s3_validate_cluster", ROOT / "spark/validate_cluster.py"
)
assert SPEC and SPEC.loader
validate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(validate)


class BoundedBody(io.BytesIO):
    def __init__(self, payload):
        super().__init__(payload)
        self.read_sizes = []

    def read(self, size=-1):
        assert 0 < size <= validate.HASH_CHUNK_BYTES
        self.read_sizes.append(size)
        return super().read(size)


class FakeS3:
    def __init__(self, payload):
        self.body = BoundedBody(payload)
        self.response = {"Body": self.body, "ContentLength": len(payload)}
        self.gets = []
        self.closed = False

    def get_object(self, **kwargs):
        self.gets.append(kwargs)
        return self.response

    def close(self):
        self.closed = True


@pytest.fixture
def completed_run(tmp_path, monkeypatch):
    from trajfoundry import credentials, s3

    payload = b'{"test":"' + b"x" * (validate.HASH_CHUNK_BYTES * 3) + b'"}\n'
    client = FakeS3(payload)
    client_calls = []

    def create_client(endpoint, region, **kwargs):
        client_calls.append((endpoint, region, kwargs))
        return client

    monkeypatch.setattr(s3, "create_s3_client", create_client)
    monkeypatch.setattr(
        credentials,
        "load_s3_credentials",
        lambda *_: pytest.fail("default credential chain must not read a file"),
    )
    args = SimpleNamespace(
        control_dir=tmp_path,
        output_dir=None,
        output_uri="s3://output/normalized/v005/dt=2026-09-15/",
        output_credentials_file=None,
        endpoint_url="https://s3.example",
        region_name="us-east-1",
        namespace="spark-ns",
        run_as_user=123,
        arm_python=Path("/share/arm/bin/python3.11"),
        validation_mode="manifest",
        baseline_dir=None,
    )
    plan = {
        "driver_pod": "spark-driver",
        "run_id": "a" * 32,
        "output_uri": args.output_uri,
        "output_dir": None,
    }
    runtime = {
        "passed": True,
        "hostname": plan["driver_pod"],
        "uid": args.run_as_user,
        "architecture": "aarch64",
        "python_major_minor": [3, 11],
        "python_executable": str(args.arm_python),
    }
    job = {
        "input_files": 1_014_558,
        "trajectories": 300_000,
        "output_backend": "s3",
        "output_uri": args.output_uri,
        "manifest_sha256": hashlib.sha256(payload).hexdigest(),
    }
    pod = {
        "metadata": {
            "name": plan["driver_pod"],
            "namespace": args.namespace,
            "uid": "pod-uid",
            "labels": {
                "spark-role": "driver",
                "spark-app-selector": "spark-app",
                "trajfoundry-run-id": plan["run_id"],
            },
        },
        "status": {
            "phase": "Succeeded",
            "containerStatuses": [{"state": {"terminated": {"exitCode": 0}}}],
        },
    }
    monkeypatch.setattr(validate, "fetch_driver_pod", lambda *_: pod)

    def write_evidence():
        (args.control_dir / "driver.log").write_text(
            validate.RUNTIME_MARKER
            + json.dumps(runtime)
            + "\n"
            + json.dumps(job)
            + "\n"
        )

    write_evidence()
    return SimpleNamespace(
        args=args,
        plan=plan,
        runtime=runtime,
        job=job,
        pod=pod,
        payload=payload,
        client=client,
        client_calls=client_calls,
        write_evidence=write_evidence,
    )


def test_s3_full_run_hashes_only_manifest_in_bounded_chunks(completed_run):
    run = completed_run
    report = validate.validate_run(run.args, run.plan)
    assert report["passed"]
    assert report["output_uri"] == run.args.output_uri
    assert report["output_dir"] is None
    assert report["output_backend"] == "s3"
    assert report["artifact_validation"] == "manifest_hash_only"
    assert run.client.gets == [
        {"Bucket": "output", "Key": "normalized/v005/dt=2026-09-15/manifest.json"}
    ]
    assert len(run.client.body.read_sizes) >= 4
    assert run.client.body.closed and run.client.closed
    assert run.client_calls == [
        ("https://s3.example", "us-east-1", {"max_pool_connections": 1})
    ]


def test_s3_explicit_credentials_use_permission_checked_loader(
    completed_run, monkeypatch
):
    from trajfoundry import credentials

    run = completed_run
    secret = object()
    calls = []
    run.args.output_credentials_file = Path("/private/not-read.json")

    def secure_load(path):
        calls.append(path)
        return secret

    monkeypatch.setattr(credentials, "load_s3_credentials", secure_load)
    assert validate.validate_run(run.args, run.plan)["passed"]
    assert calls == [run.args.output_credentials_file]
    assert run.client_calls[0][2]["credentials"] is secret


@pytest.mark.parametrize(
    "failure",
    [
        "wrong_run",
        "wrong_pod",
        "wrong_namespace",
        "failed_driver",
        "exit_code",
        "wrong_runtime",
        "wrong_output_uri",
        "wrong_output_backend",
        "zero_inputs",
        "zero_trajectories",
    ],
)
def test_invalid_completion_evidence_never_reads_s3_or_credentials(
    completed_run, failure
):
    run = completed_run
    if failure == "wrong_run":
        run.pod["metadata"]["labels"]["trajfoundry-run-id"] = "other"
    elif failure == "wrong_pod":
        run.pod["metadata"]["name"] = "other"
    elif failure == "wrong_namespace":
        run.pod["metadata"]["namespace"] = "other"
    elif failure == "failed_driver":
        run.pod["status"]["phase"] = "Failed"
    elif failure == "exit_code":
        run.pod["status"]["containerStatuses"][0]["state"]["terminated"]["exitCode"] = 1
    elif failure == "wrong_runtime":
        run.runtime["architecture"] = "x86_64"
    elif failure == "wrong_output_uri":
        run.job["output_uri"] = "s3://output/other/"
    elif failure == "wrong_output_backend":
        run.job["output_backend"] = "filesystem"
    elif failure == "zero_inputs":
        run.job["input_files"] = 0
    elif failure == "zero_trajectories":
        run.job["trajectories"] = 0
    run.write_evidence()
    report = validate.validate_run(run.args, run.plan)
    assert not report["passed"]
    assert not report["checks"]["manifest_hash_matches"]
    assert not run.client_calls


def test_s3_manifest_digest_mismatch_fails(completed_run):
    run = completed_run
    run.job["manifest_sha256"] = "0" * 64
    run.write_evidence()
    report = validate.validate_run(run.args, run.plan)
    assert not report["passed"]
    assert not report["checks"]["manifest_hash_matches"]


@pytest.mark.parametrize("skipped,allowed", [(2, True), (1, False), (True, False)])
def test_zero_lakehouse_trajectories_require_complete_input_accounting(
    completed_run, skipped, allowed
):
    run = completed_run
    run.args.input_format = "boyue"
    run.job.update(
        input_files=3,
        trajectories=0,
        accepted=0,
        quarantined_trajectories=0,
        quarantined_records=1,
        skipped_inputs=skipped,
        verified_trajectory_objects=0,
    )
    run.write_evidence()
    assert validate.validate_run(run.args, run.plan)["passed"] is allowed


@pytest.mark.parametrize("length", [None, -1, True, 0, 9_999_999])
def test_s3_response_lengths_are_validated_and_resources_closed(completed_run, length):
    run = completed_run
    run.client.response["ContentLength"] = length
    with pytest.raises(RuntimeError, match="^S3 manifest verification failed$"):
        validate.validate_run(run.args, run.plan)
    assert run.client.closed and run.client.body.closed


def test_s3_sdk_errors_are_sanitized_and_resources_closed(completed_run, monkeypatch):
    run = completed_run

    def fail_read(_size):
        raise OSError("authentication-secret-must-not-escape")

    monkeypatch.setattr(run.client.body, "read", fail_read)
    with pytest.raises(RuntimeError) as caught:
        validate.validate_run(run.args, run.plan)
    assert "authentication-secret" not in str(caught.value)
    assert caught.value.__suppress_context__
    assert run.client.closed and run.client.body.closed


def test_bad_explicit_credentials_do_not_fall_back(completed_run, monkeypatch):
    from trajfoundry import credentials

    run = completed_run
    run.args.output_credentials_file = Path("/private/not-read.json")

    def fail_load(_path):
        raise credentials.CredentialFileError("unsafe file")

    monkeypatch.setattr(credentials, "load_s3_credentials", fail_load)
    with pytest.raises(RuntimeError, match="^S3 manifest verification failed$"):
        validate.validate_run(run.args, run.plan)
    assert not run.client_calls


def test_s3_small_mode_rejected_without_network_access(completed_run, monkeypatch):
    run = completed_run
    run.args.validation_mode = "small"
    monkeypatch.setattr(
        validate, "fetch_driver_pod", lambda *_: pytest.fail("must reject first")
    )
    with pytest.raises(ValueError, match="requires filesystem output"):
        validate.validate_run(run.args, run.plan)
    assert not run.client_calls


def test_validation_cannot_target_different_prefix_than_plan(completed_run):
    run = completed_run
    run.plan["output_uri"] = "s3://output/other/"
    with pytest.raises(ValueError, match="differs from submitted plan"):
        validate.validate_run(run.args, run.plan)
    assert not run.client_calls
