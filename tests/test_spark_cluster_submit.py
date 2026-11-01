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


def load_script(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "spark" / f"{name}.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


submit = load_script("submit_cluster")
validate = load_script("validate_cluster")


@pytest.fixture
def invocation(tmp_path, monkeypatch):
    monkeypatch.setattr(sys, "path", list(sys.path))
    template = tmp_path / "pod.json"
    template.write_text(
        json.dumps(
            {
                "apiVersion": "v1",
                "kind": "Pod",
                "spec": {
                    "containers": [
                        {
                            "name": "spark-kubernetes",
                            "env": [
                                {"name": "PYSPARK_PYTHON", "value": "/old/x86/python"}
                            ],
                            "securityContext": {"runAsUser": 999},
                            "volumeMounts": [
                                {"name": "shared", "mountPath": "/share"},
                                {"name": "test-shared", "mountPath": str(tmp_path)},
                            ],
                        }
                    ],
                    "volumes": [
                        {
                            "name": "shared",
                            "persistentVolumeClaim": {"claimName": "shared-pvc"},
                        },
                        {
                            "name": "test-shared",
                            "persistentVolumeClaim": {"claimName": "test-pvc"},
                        },
                    ],
                },
            }
        )
    )
    return [
        "--project-root",
        str(ROOT),
        "--arm-python",
        "/share/arm311/bin/python3.11",
        "--driver-pod-template",
        str(template),
        "--executor-pod-template",
        str(template),
        "--image",
        "example/spark:3.5-arm",
        "--queue",
        "root.example.queue",
        "--master",
        "k8s://https://api.example:443",
        "--namespace",
        "test-ns",
        "--service-account",
        "spark-sa",
        "--ca-cert",
        str(tmp_path / "not-read-ca"),
        "--oauth-token-file",
        str(tmp_path / "not-read-token"),
        "--run-as-user",
        str(os.geteuid() or 1),
        "--control-dir",
        str(tmp_path / "control"),
        "--output-dir",
        str(tmp_path / "output"),
        "--input-uri",
        "s3://input/tokenplan/dt=2026-09-01/",
        "--prefix-shard",
        "user-a/",
        "--prefix-shard",
        "user-b/",
        "--endpoint-url",
        "https://s3.example",
    ]


def parsed_plan(invocation):
    args = submit.parser().parse_args(invocation)
    return args, submit.make_plan(args, run_id="a" * 32)


def conf_of(plan):
    command = plan["command"]
    return dict(
        command[index + 1].split("=", 1)
        for index, value in enumerate(command)
        if value == "--conf"
    )


@pytest.mark.parametrize(
    "profile,instances,total",
    [("starter", 16, 1024), ("half", 32, 2048), ("scale", 48, 3072)],
)
def test_resource_profiles_have_exact_64_gib_envelope(
    invocation, profile, instances, total
):
    _, plan = parsed_plan(invocation + ["--profile", profile])
    conf = conf_of(plan)
    assert plan["total_executor_memory_gib"] == total
    assert plan["total_executor_cores"] == instances * 8
    assert conf["spark.executor.instances"] == str(instances)
    assert conf["spark.executor.cores"] == "8"
    assert conf["spark.executor.memory"] == "16g"
    assert conf["spark.executor.memoryOverhead"] == "48g"
    assert conf["spark.driver.maxResultSize"] == "1g"
    assert "spark.executor.pyspark.memory" not in conf
    assert "spark.kubernetes.submitInDriver" not in conf
    assert conf["spark.memory.offHeap.enabled"] == "false"
    assert conf["spark.kubernetes.trust.certificates"] == "false"
    assert conf["spark.kubernetes.driver.master"] == "https://api.example:443"
    assert conf["spark.kubernetes.submission.waitAppCompletion"] == "true"
    assert conf["spark.task.maxFailures"] == "4"
    assert conf["spark.stage.maxConsecutiveAttempts"] == "4"
    assert conf["spark.excludeOnFailure.enabled"] == "true"
    assert conf["spark.speculation"] == "false"
    assert plan["command"][plan["command"].index("--deploy-mode") + 1] == "cluster"
    properties = Path(plan["command"][plan["command"].index("--properties-file") + 1])
    assert properties.is_file() and properties.name == "cluster-isolated.properties"
    for flag, value in (
        ("--max-routing-mib", "4096"),
        ("--max-routing-snapshots", "2000000"),
        ("--max-root-mib", "2048"),
    ):
        assert plan["command"][plan["command"].index(flag) + 1] == value


def test_event_log_configuration_is_forwarded(invocation):
    _, plan = parsed_plan(invocation)
    conf = conf_of(plan)
    assert conf["spark.eventLog.enabled"] == "true"
    assert conf["spark.eventLog.dir"] == "/spark/eventLog"


def test_yaml_templates_are_supported_without_mutating_source(invocation, tmp_path):
    template = tmp_path / "pod-template.yaml"
    source = f"""\
apiVersion: v1
kind: Pod
metadata:
  labels:
    spark-monitor: enabled
spec:
  containers:
  - name: spark-kubernetes
    volumeMounts:
    - name: shared
      mountPath: /share
    - name: test-shared
      mountPath: {tmp_path}
  volumes:
  - name: shared
    persistentVolumeClaim:
      claimName: shared-pvc
  - name: test-shared
    persistentVolumeClaim:
      claimName: test-pvc
"""
    template.write_text(source)
    values = list(invocation)
    for flag in ("--driver-pod-template", "--executor-pod-template"):
        values[values.index(flag) + 1] = str(template)

    args, plan = parsed_plan(values)

    assert plan["driver_template"]["metadata"]["labels"]["spark-monitor"] == "enabled"
    assert plan["executor_template"]["metadata"]["labels"]["spark-monitor"] == "enabled"
    assert template.read_text() == source
    assert args.driver_pod_template.suffix == ".yaml"


def test_templates_enforce_arm_uid_and_disk_scratch_without_mutating_base(invocation):
    args, plan = parsed_plan(invocation)
    for key in ("driver_template", "executor_template"):
        spec = plan[key]["spec"]
        container = spec["containers"][0]
        environment = {item["name"]: item["value"] for item in container["env"]}
        assert spec["nodeSelector"]["kubernetes.io/arch"] == "arm64"
        assert spec["securityContext"]["runAsUser"] == args.run_as_user
        assert container["securityContext"]["runAsUser"] == args.run_as_user
        assert args.timeout_seconds == 0
        assert "activeDeadlineSeconds" not in spec
        assert environment["TMPDIR"] == submit.LOCAL_DIR
        assert environment["PATH"].startswith(str(args.arm_python.parent) + ":")
        assert environment["PYSPARK_PYTHON"] == str(args.arm_python)
        assert spec["volumes"][-1] == {
            "name": "spark-local-dir-trajfoundry",
            "emptyDir": {},
        }
    driver = plan["driver_template"]["spec"]["containers"][0]
    assert driver["command"][-1].endswith("spark/cluster_driver_entrypoint.sh")
    original = json.loads(args.driver_pod_template.read_text())
    assert original["spec"]["containers"][0]["securityContext"]["runAsUser"] == 999


def test_default_plan_never_submits_or_reads_authentication(
    invocation, monkeypatch, capsys
):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("must not submit")

    monkeypatch.setattr(submit.subprocess, "run", forbidden)
    assert submit.main(invocation) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["execute"] is False
    assert report["profile"] == "starter"
    assert report["retain_s3_auxiliary"] is False
    assert not Path(report["control_dir"]).exists()
    assert not Path(report["output_dir"]).exists()
    assert "command" not in report


@pytest.mark.parametrize(
    "flag",
    [
        "--output-uri",
        "--input-uri=s3://other/",
        "--prefix-shard",
        "--input-credentials-file",
        "--output-credentials-file",
        "--out=s3://other/output/",
        "--input-cred",
        "--retain-s3-auxiliary",
        "--retain-s3-auxiliary=true",
        "--retain-s3-aux",
    ],
)
def test_job_arguments_cannot_override_routing(invocation, flag):
    with pytest.raises(ValueError, match="must not override"):
        parsed_plan(invocation + ["--job-arg=" + flag])


def test_extra_bounded_job_configuration_is_forwarded(invocation):
    _, plan = parsed_plan(
        invocation + ["--job-arg=--max-output-objects", "--job-arg=200000"]
    )
    assert plan["command"][-2:] == ["--max-output-objects", "200000"]


def s3_invocation(invocation, output_uri="s3://output/normalized/v005/dt=2026-09-15/"):
    values = list(invocation)
    index = values.index("--output-dir")
    values[index : index + 2] = ["--output-uri", output_uri]
    return values


def test_s3_plan_uses_same_job_destination_without_local_output_mount(invocation):
    args, plan = parsed_plan(s3_invocation(invocation))
    assert args.output_dir is None
    assert plan["output_dir"] is None
    assert plan["output_uri"] == args.output_uri
    assert plan["command"][plan["command"].index("--output-uri") + 1] == args.output_uri
    assert plan["runtime_unlimited"] is True
    assert plan["retain_s3_auxiliary"] is False
    assert "--retain-s3-auxiliary" not in plan["command"]


def test_s3_auxiliary_retention_is_explicit_and_forwarded(invocation):
    args, plan = parsed_plan(s3_invocation(invocation) + ["--retain-s3-auxiliary"])
    assert args.retain_s3_auxiliary is True
    assert plan["retain_s3_auxiliary"] is True
    assert plan["validation_mode"] == "manifest"
    assert plan["command"].count("--retain-s3-auxiliary") == 1
    assert plan["command"].index("--retain-s3-auxiliary") > next(
        index
        for index, value in enumerate(plan["command"])
        if value.endswith("spark/tokenplan_normalize.py")
    )


def test_automatic_prefix_discovery_is_forwarded_without_explicit_shards(invocation):
    values = [
        value
        for index, value in enumerate(invocation)
        if value != "--prefix-shard"
        and not (index and invocation[index - 1] == "--prefix-shard")
    ]
    args, plan = parsed_plan(values + ["--auto-prefix-shards"])
    assert args.auto_prefix_shards is True
    assert "--prefix-shard" not in plan["command"]
    assert "--auto-prefix-shards" in plan["command"]
    assert plan["command"][plan["command"].index("--max-prefix-shards") + 1] == "1000000"


@pytest.mark.parametrize(
    "s3,validation", [(False, "manifest"), (False, "small"), (True, "small")]
)
def test_retention_rejects_local_output_and_small_validation(
    invocation, s3, validation
):
    values = s3_invocation(invocation) if s3 else invocation
    with pytest.raises(ValueError, match="requires S3 output and manifest validation"):
        parsed_plan(values + ["--retain-s3-auxiliary", "--validation-mode", validation])


def test_retention_is_visible_in_public_dry_run_without_submission(
    invocation, monkeypatch, capsys
):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("dry run must not submit")

    monkeypatch.setattr(submit.subprocess, "run", forbidden)
    assert submit.main(s3_invocation(invocation) + ["--retain-s3-auxiliary"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["retain_s3_auxiliary"] is True
    assert report["execute"] is False
    assert "command" not in report
    assert not Path(report["control_dir"]).exists()


@pytest.mark.parametrize("retain", [False, True])
def test_normalizer_cli_forwards_auxiliary_retention_to_config(
    monkeypatch, capsys, retain
):
    entrypoint = load_script("tokenplan_normalize")
    seen = []

    def capture_config(**kwargs):
        seen.append(kwargs)
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(entrypoint, "SparkNormalizeConfig", capture_config)
    monkeypatch.setattr(entrypoint, "run_spark_job", lambda config: {"passed": True})
    argv = [
        "tokenplan_normalize.py",
        "--input-uri",
        "s3://input/day/",
        "--output-uri",
        "s3://output/day/",
        "--prefix-shard",
        "user/",
    ]
    if retain:
        argv.append("--retain-s3-auxiliary")
    monkeypatch.setattr(sys, "argv", argv)
    entrypoint.main()
    assert seen[0]["retain_s3_auxiliary"] is retain
    assert json.loads(capsys.readouterr().out) == {"passed": True}


def test_normalizer_cli_uses_expanded_admission_defaults():
    entrypoint = load_script("tokenplan_normalize")
    config = entrypoint.build_config(
        [
            "--input-uri",
            "s3://input/day/",
            "--output-uri",
            "s3://output/day/",
            "--prefix-shard",
            "user/",
        ]
    )
    assert config.max_output_objects is None
    assert config.max_partition_objects == 20_000_000
    assert config.scope_memory_bytes == 256 * 1024**2
    assert config.max_scope_snapshots == 10_000
    assert config.max_scope_summaries == 1_000_000
    assert config.max_scope_summary_bytes == 512 * 1024**2
    assert config.max_input_object_bytes == 2 * 1024**3
    assert config.max_routing_bytes == 4 * 1024**3
    assert config.max_routing_snapshots == 2_000_000
    assert config.max_root_bytes == 2 * 1024**3
    assert config.max_prefix_shards == 1_000_000


def test_normalizer_cli_forwards_automatic_prefix_discovery(
    monkeypatch, capsys
):
    entrypoint = load_script("tokenplan_normalize")
    seen = []

    def capture_config(**kwargs):
        seen.append(kwargs)
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(entrypoint, "SparkNormalizeConfig", capture_config)
    monkeypatch.setattr(entrypoint, "run_spark_job", lambda config: {"passed": True})
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "tokenplan_normalize.py",
            "--input-uri",
            "s3://input/day/",
            "--output-uri",
            "s3://output/day/",
            "--auto-prefix-shards",
            "--max-prefix-shards",
            "17",
        ],
    )
    entrypoint.main()
    assert seen[0]["auto_prefix_shards"] is True
    assert seen[0]["prefix_shards"] == ()
    assert seen[0]["max_prefix_shards"] == 17
    assert json.loads(capsys.readouterr().out) == {"passed": True}


@pytest.mark.parametrize(
    "destination",
    [
        "/share/local-output",
        "file:///share/local-output",
        "s3://output/",
        "s3://output/prefix/../escape/",
        "s3://name:secret@output/path/",
        "s3://input/tokenplan/",
        "s3://input/tokenplan/dt=2026-09-01/output/",
    ],
)
def test_s3_plan_rejects_unsafe_or_overlapping_output(invocation, destination):
    with pytest.raises(ValueError):
        parsed_plan(s3_invocation(invocation, destination))


def test_s3_plan_rejects_small_filesystem_validation_before_submitting(invocation):
    with pytest.raises(ValueError, match="small validation requires"):
        parsed_plan(s3_invocation(invocation) + ["--validation-mode", "small"])


def test_output_destinations_are_mutually_exclusive(invocation):
    with pytest.raises(SystemExit):
        submit.parser().parse_args(invocation + ["--output-uri", "s3://output/path/"])


@pytest.mark.parametrize(
    "flag", ["--input-credentials-file", "--output-credentials-file"]
)
def test_protected_credentials_are_forwarded_and_not_read(
    invocation, tmp_path, monkeypatch, flag
):
    credentials = tmp_path / "private.json"
    credentials.write_text("not JSON: planning must never read this")
    credentials.chmod(0o600)
    values = s3_invocation(invocation) + [flag, str(credentials)]
    args = submit.parser().parse_args(values)
    args.run_as_user = os.geteuid()
    original = Path.read_text

    def read_text(path, *args, **kwargs):
        assert path != credentials
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", read_text)
    plan = submit.make_plan(args)
    assert plan["command"][plan["command"].index(flag) + 1] == str(credentials)


@pytest.mark.parametrize("failure", ["permissions", "owner", "symlink", "unshared"])
def test_s3_plan_rejects_unprotected_or_unmounted_credentials(
    invocation, tmp_path, monkeypatch, failure
):
    credentials = tmp_path / "private.json"
    credentials.write_text("never-read")
    credentials.chmod(0o600)
    args = submit.parser().parse_args(
        s3_invocation(invocation) + ["--output-credentials-file", str(credentials)]
    )
    args.run_as_user = os.geteuid()
    if failure == "permissions":
        credentials.chmod(0o644)
    elif failure == "owner":
        args.run_as_user += 1
    elif failure == "symlink":
        link = tmp_path / "link"
        link.symlink_to(credentials)
        args.output_credentials_file = link
    else:
        # Other required paths stay shared: only the credential metadata check
        # is isolated so an unmapped path need not be created on the real host.
        args.output_credentials_file = Path("/unshared-private/credentials.json")
        monkeypatch.setattr(submit, "check_credentials_file", lambda *_: None)
    with pytest.raises(ValueError):
        submit.make_plan(args)


def test_filesystem_output_cannot_receive_s3_output_credentials(invocation, tmp_path):
    with pytest.raises(ValueError, match="output credentials require S3"):
        parsed_plan(
            invocation + ["--output-credentials-file", str(tmp_path / "secret")]
        )


@pytest.mark.parametrize("timeout", [0, 7200])
def test_overall_timeout_is_opt_in_and_removes_inherited_deadlines(invocation, timeout):
    args = submit.parser().parse_args(invocation + ["--timeout-seconds", str(timeout)])
    template = json.loads(args.driver_pod_template.read_text())
    template["spec"]["activeDeadlineSeconds"] = 123
    args.driver_pod_template.write_text(json.dumps(template))
    plan = submit.make_plan(args)
    for role in ("driver_template", "executor_template"):
        spec = plan[role]["spec"]
        if timeout:
            assert spec["activeDeadlineSeconds"] == timeout
        else:
            assert "activeDeadlineSeconds" not in spec
    assert plan["runtime_unlimited"] is (timeout == 0)
    assert (
        json.loads(args.driver_pod_template.read_text())["spec"][
            "activeDeadlineSeconds"
        ]
        == 123
    )


def test_negative_timeout_is_rejected(invocation):
    with pytest.raises(SystemExit):
        submit.parser().parse_args(invocation + ["--timeout-seconds", "-1"])


@pytest.mark.parametrize(
    "failure",
    [
        "nonempty",
        "control_exists",
        "nested",
        "symlink",
        "baseline_unbounded",
        "unshared",
        "scratch_tmpfs",
    ],
)
def test_plan_rejects_unsafe_paths_and_templates(invocation, tmp_path, failure):
    args = submit.parser().parse_args(invocation)
    if failure == "nonempty":
        args.output_dir.mkdir()
        (args.output_dir / "existing").write_text("preserve")
    elif failure == "control_exists":
        args.control_dir.mkdir()
    elif failure == "nested":
        args.control_dir = args.output_dir / "nested"
    elif failure == "symlink":
        args.output_dir.symlink_to(tmp_path)
    elif failure == "baseline_unbounded":
        args.baseline_dir = tmp_path / "baseline"
    else:
        template = json.loads(args.driver_pod_template.read_text())
        if failure == "unshared":
            template["spec"]["volumes"] = []
        else:
            template["spec"]["volumes"].append(
                {"name": "spark-local-dir-existing", "emptyDir": {"medium": "Memory"}}
            )
        args.driver_pod_template.write_text(json.dumps(template))
    with pytest.raises(ValueError):
        submit.make_plan(args)
    if failure == "nonempty":
        assert (args.output_dir / "existing").read_text() == "preserve"


@pytest.mark.parametrize(
    "master",
    [
        "k8s://http://api.example",
        "k8s://https://name:token@api.example",
        "k8s://https://api.example/?token=secret",
        "local[2]",
    ],
)
def test_only_credential_free_https_api_authorities_are_accepted(master):
    with pytest.raises(ValueError):
        submit.api_url(master)


def test_submit_environment_cannot_select_old_python_or_inject_jvm_flags(monkeypatch):
    for key in (
        "PYSPARK_DRIVER_PYTHON",
        "PYSPARK_PYTHON",
        "SPARK_SUBMIT_OPTS",
        "HTTP_PROXY",
        "JAVA_TOOL_OPTIONS",
    ):
        monkeypatch.setenv(key, "unwanted")
    child = submit.child_environment()
    assert not any(
        key in child
        for key in (
            "PYSPARK_DRIVER_PYTHON",
            "PYSPARK_PYTHON",
            "SPARK_SUBMIT_OPTS",
            "HTTP_PROXY",
            "JAVA_TOOL_OPTIONS",
        )
    )


def evidence(args, plan):
    args.control_dir.mkdir()
    args.output_dir.mkdir()
    (args.output_dir / "manifest.json").write_text('{"test":true}\n')
    runtime = {
        "passed": True,
        "hostname": plan["driver_pod"],
        "uid": args.run_as_user,
        "architecture": "aarch64",
        "python_major_minor": [3, 11],
        "python_executable": str(args.arm_python),
    }
    job = {
        "input_files": 50,
        "trajectories": 10,
        "output_backend": "filesystem",
        "output_uri": str(args.output_dir),
        "manifest_sha256": validate.file_sha256(args.output_dir / "manifest.json"),
    }
    (args.control_dir / "driver.log").write_text(
        validate.RUNTIME_MARKER + json.dumps(runtime) + "\n" + json.dumps(job) + "\n"
    )
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
    return pod


@pytest.mark.parametrize(
    "failure",
    [
        None,
        "failed_phase",
        "exit_code",
        "empty_status",
        "wrong_run",
        "wrong_role",
        "wrong_pod",
        "hash_changed",
    ],
)
def test_post_validation_checks_actual_pod_and_manifest(
    invocation, monkeypatch, failure
):
    args, plan = parsed_plan(invocation)
    pod = evidence(args, plan)
    if failure == "failed_phase":
        pod["status"]["phase"] = "Failed"
    elif failure == "exit_code":
        pod["status"]["containerStatuses"][0]["state"]["terminated"]["exitCode"] = 1
    elif failure == "empty_status":
        pod["status"]["containerStatuses"] = []
    elif failure == "wrong_run":
        pod["metadata"]["labels"]["trajfoundry-run-id"] = "other"
    elif failure == "wrong_role":
        pod["metadata"]["labels"]["spark-role"] = "executor"
    elif failure == "wrong_pod":
        pod["metadata"]["name"] = "other"
    elif failure == "hash_changed":
        (args.output_dir / "manifest.json").write_text("changed")
    monkeypatch.setattr(validate, "fetch_driver_pod", lambda *_: pod)
    report = validate.validate_run(args, plan)
    assert report["passed"] is (failure is None)
    assert report["artifact_validation"] == "manifest_hash_only"


@pytest.mark.parametrize("failure", ["missing", "duplicate", "oversized"])
def test_driver_log_evidence_is_unique_and_bounded(tmp_path, failure):
    log = tmp_path / "driver.log"
    if failure == "missing":
        log.write_text("unrelated startup\n")
    elif failure == "duplicate":
        log.write_text((validate.RUNTIME_MARKER + "{}\n") * 2)
    else:
        log.write_text("x" * (validate.MAX_LOG_LINE + 1))
    with pytest.raises(ValueError):
        validate.read_reports(log)


def test_small_validation_refuses_large_manifest_without_materializing_it(
    tmp_path, monkeypatch
):
    manifest = tmp_path / "manifest.json"
    with manifest.open("wb") as handle:
        handle.truncate(validate.MAX_SMALL_MANIFEST + 1)
    monkeypatch.setattr(
        validate.json, "load", lambda _: pytest.fail("must bound before parsing")
    )
    with pytest.raises(ValueError, match="small-validation envelope"):
        validate.small_manifest(tmp_path)


def small_artifacts(root, *, disposition="accepted", reverse=False):
    root.mkdir()
    origins = [
        {"source_ref": "a", "disposition": disposition},
        {"source_ref": "b", "disposition": "accepted"},
    ]
    if reverse:
        origins.reverse()
    (root / "lineage.jsonl").write_text(
        json.dumps({"trajectory_id": "one", "origins": origins}) + "\n"
    )
    (root / "one.jsonl").write_text('{"synthetic":true}\n')
    counts = {
        "input_files": 2,
        "accepted": 1,
        "quarantined_trajectories": 0,
        "quarantined_records": 0,
        "skipped_inputs": 0,
    }
    entries = [
        {
            "path": path.name,
            "bytes": path.stat().st_size,
            "sha256": validate.file_sha256(path),
        }
        for path in (root / "lineage.jsonl", root / "one.jsonl")
    ]
    (root / "manifest.json").write_text(
        json.dumps({"counts": counts, "files": entries, "input_root": "s3://test/day/"})
    )
    return {**counts, "parse_failures": 0}


@pytest.mark.parametrize("mismatch", [False, True])
def test_small_baseline_comparison_includes_complete_audits_and_ignores_origin_order(
    tmp_path, monkeypatch, mismatch
):
    from trajfoundry import validation

    args = SimpleNamespace(
        output_dir=tmp_path / "out", baseline_dir=tmp_path / "baseline"
    )
    job = small_artifacts(args.output_dir)
    small_artifacts(
        args.baseline_dir,
        disposition="quarantined" if mismatch else "accepted",
        reverse=True,
    )
    calls = []

    def validate_output(path):
        calls.append(path)
        return SimpleNamespace(valid=True)

    monkeypatch.setattr(validation, "validate_output", validate_output)
    checks = validate.validate_small_output(args, job)
    assert checks["baseline_lineage_match"] is (not mismatch)
    assert all(checks.values()) is (not mismatch)
    assert calls == [args.output_dir, args.baseline_dir]


def test_small_validation_bounds_actual_file_size_not_only_declared_manifest(tmp_path):
    output = tmp_path / "out"
    small_artifacts(output)
    with (output / "one.jsonl").open("wb") as handle:
        handle.truncate(validate.MAX_SMALL_FILE + 1)
    with pytest.raises(ValueError, match="artifact bytes"):
        validate.small_manifest(output)


def test_manifest_hash_is_streamed_without_read_bytes(tmp_path, monkeypatch):
    path = tmp_path / "manifest.json"
    path.write_text("manifest content")
    expected = validate.file_sha256(path)
    monkeypatch.setattr(Path, "read_bytes", lambda _: pytest.fail("no full read_bytes"))
    assert validate.file_sha256(path) == expected


@pytest.mark.parametrize("submit_exit,validated", [(0, False), (1, True), (0, True)])
def test_execute_does_not_trust_zero_submit_status(
    invocation, monkeypatch, capsys, submit_exit, validated
):
    monkeypatch.setattr(
        submit.os,
        "geteuid",
        lambda: int(invocation[invocation.index("--run-as-user") + 1]),
    )
    monkeypatch.setattr(
        submit.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=submit_exit),
    )
    monkeypatch.setitem(sys.modules, "validate_cluster", validate)
    monkeypatch.setattr(
        validate,
        "validate_run",
        lambda *_: {"passed": validated, "checks": {"actual_driver": validated}},
    )
    status = submit.main(invocation + ["--execute"])
    expected = submit_exit == 0 and validated
    assert status == (0 if expected else 2)
    args = submit.parser().parse_args(invocation)
    final = json.loads((args.control_dir / "final.json").read_text())
    assert final["passed"] is expected
    assert final["submit_exit_code"] == submit_exit
    assert final["finished_at"] >= final["started_at"]
    assert final["exit_code"] == status
    capsys.readouterr()


@pytest.mark.parametrize("timeout", [0, 3600])
def test_submit_watch_has_no_default_runtime_timeout(
    invocation, monkeypatch, timeout, capsys
):
    monkeypatch.setattr(
        submit.os,
        "geteuid",
        lambda: int(invocation[invocation.index("--run-as-user") + 1]),
    )
    captured = {}

    def run(*_args, **kwargs):
        captured.update(kwargs)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(submit.subprocess, "run", run)
    monkeypatch.setitem(sys.modules, "validate_cluster", validate)
    monkeypatch.setattr(validate, "validate_run", lambda *_: {"passed": True})
    assert (
        submit.main(invocation + ["--execute", "--timeout-seconds", str(timeout)]) == 0
    )
    assert captured["timeout"] == (timeout + 60 if timeout else None)
    capsys.readouterr()


@pytest.mark.parametrize("failure_stage", ["submit_or_wait", "validate_completion"])
def test_early_execution_failure_always_writes_sanitized_final_report(
    invocation, monkeypatch, failure_stage, capsys
):
    monkeypatch.setattr(
        submit.os,
        "geteuid",
        lambda: int(invocation[invocation.index("--run-as-user") + 1]),
    )

    def fail(*_args, **_kwargs):
        raise RuntimeError("credential_value_must_not_be_logged")

    monkeypatch.setattr(
        submit.subprocess,
        "run",
        fail
        if failure_stage == "submit_or_wait"
        else lambda *_a, **_k: SimpleNamespace(returncode=1),
    )
    monkeypatch.setitem(sys.modules, "validate_cluster", validate)
    monkeypatch.setattr(validate, "validate_run", fail)
    assert submit.main(invocation + ["--execute"]) == 2
    args = submit.parser().parse_args(invocation)
    text = (args.control_dir / "final.json").read_text()
    final = json.loads(text)
    assert final["passed"] is False
    assert final["stage"] == failure_stage
    assert final["error_type"] == "RuntimeError"
    assert final["finished_at"] >= final["started_at"]
    assert final["exit_code"] == 2
    assert final["cleanup_required"] is True
    assert "credential_value_must_not_be_logged" not in text + capsys.readouterr().out


def test_preflight_failure_never_writes_into_existing_control_dir(invocation, capsys):
    args = submit.parser().parse_args(invocation)
    args.control_dir.mkdir()
    (args.control_dir / "final.json").write_text("preserve existing result")
    assert submit.main(invocation + ["--execute"]) == 2
    assert (args.control_dir / "final.json").read_text() == "preserve existing result"
    capsys.readouterr()


def test_driver_entrypoint_has_valid_shell_syntax():
    subprocess.run(
        ["bash", "-n", str(ROOT / "spark/cluster_driver_entrypoint.sh")], check=True
    )
