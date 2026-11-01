from __future__ import annotations

import copy
import importlib.util
import json
import stat
import subprocess
from pathlib import Path
from types import SimpleNamespace
from urllib.error import HTTPError

import pytest

from tests import test_spark_scheduled_tokenplan as shared_fixtures

config = shared_fixtures.config

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "worker_scheduler", ROOT / "spark/submit_scheduler.py"
)
assert spec and spec.loader
scheduler = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scheduler)


def cli(config, *extra):
    return ["--config", str(config), "--biz-date", "2026-09-15", *extra]


def forbidden(*args, **kwargs):
    raise AssertionError("unexpected external action")


def pod(args, plan):
    value = copy.deepcopy(plan["driver_template"])
    value["metadata"] = {
        "name": plan["driver_pod"],
        "namespace": args.namespace,
        "uid": "4918deee-6e67-445b-807f-bf21c7765d25",
        "labels": {
            "spark-role": "driver",
            "spark-app-selector": "spark-" + "a" * 32,
            "spark-app-name": plan["driver_pod"],
            "trajfoundry-run-id": plan["run_id"],
        },
    }
    container = next(
        item
        for item in value["spec"]["containers"]
        if item["name"] == args.container_name
    )
    container["image"] = args.image
    index = plan["command"].index(
        "local://" + str(args.project_root / "spark/scheduler_cluster_job.py")
    )
    container["args"] = [
        "driver",
        "--properties-file",
        "/opt/spark/conf/spark.properties",
        "--class",
        "org.apache.spark.deploy.PythonRunner",
        *plan["command"][index:],
    ]
    container["env"].append(
        {"name": "SPARK_APPLICATION_ID", "value": "spark-" + "a" * 32}
    )
    value["status"] = {
        "phase": "Succeeded",
        "containerStatuses": [
            {"name": args.container_name, "state": {"terminated": {"exitCode": 0}}}
        ],
    }
    return value


def test_default_plan_no_network_secret_reads_or_directories(
    config, monkeypatch, capsys
):
    from trajfoundry import credentials, s3

    monkeypatch.setattr(credentials, "load_s3_credentials", forbidden)
    monkeypatch.setattr(s3, "create_s3_client", forbidden)
    monkeypatch.setattr(scheduler, "api", forbidden)
    monkeypatch.setattr(scheduler, "check_worker", forbidden)
    monkeypatch.setattr(scheduler.tempfile, "mkdtemp", forbidden)
    monkeypatch.setattr(scheduler.subprocess, "Popen", forbidden)
    monkeypatch.setattr(scheduler.os, "geteuid", lambda: 1007)
    original = Path.read_text

    def safe_read(path, *args, **kwargs):
        assert path.name != "credentials.json"
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", safe_read)
    before = set(config.parent.iterdir())
    assert scheduler.main(cli(config)) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["submitter_uid"] == 1007
    assert result["pod_uid"] != 1007
    assert result["total_executor_cores"] == 256
    assert result["total_executor_memory_gib"] == 2048
    assert result["s3_access_from_worker"] is False
    assert result["monitoring"] == "pod_status_only"
    assert result["completion_evidence"] == "driver_exit_after_validation"
    assert result["dynamic_counts_available"] is False
    assert result["execute"] is False and result["check_worker"] is False
    assert "run_dir" not in result and "control_dir" not in result
    assert set(config.parent.iterdir()) == before


def test_worker_overrides_and_driver_command_keep_business_routing(config):
    value = json.loads(config.read_text())
    source = Path(
        value["submit_args"][value["submit_args"].index("--driver-pod-template") + 1]
    )
    attachment = source.with_name("resource-center-pod.json")
    template = json.loads(source.read_text())
    template["metadata"] = {"labels": {"resource-center": "yes"}}
    attachment.write_text(json.dumps(template))
    options = scheduler.parser().parse_args(
        cli(
            config,
            "--spark-submit",
            "/opt/soft/spark/bin/spark-submit",
            "--driver-pod-template",
            str(attachment),
            "--executor-pod-template",
            str(attachment),
        )
    )
    args, plan, _ = scheduler.make_plan(options)
    assert args.spark_submit == Path("/opt/soft/spark/bin/spark-submit")
    assert plan["command"][0] == str(args.spark_submit)
    for role in ("driver", "executor"):
        assert (
            plan[f"{role}_template"]["metadata"]["labels"]["resource-center"] == "yes"
        )
    driver = plan["driver_template"]["spec"]["containers"][0]
    assert driver["command"] == [
        "/bin/bash",
        str(ROOT / "spark/scheduler_driver_entrypoint.sh"),
    ]
    assert "TF_CLUSTER_LOG_PATH" not in {item["name"] for item in driver["env"]}
    assert driver["securityContext"]["runAsUser"] == args.run_as_user
    index = plan["command"].index(
        "local://" + str(ROOT / "spark/scheduler_cluster_job.py")
    )
    assert plan["command"][index + 1 : index + 7] == [
        "--tf-run-id",
        plan["run_id"],
        "--tf-driver-pod",
        plan["driver_pod"],
        "--tf-pod-uid",
        str(args.run_as_user),
    ]
    assert plan["command"][index + 7 : index + 9] == [
        "--input-uri",
        "s3://input/tokenplan/v002/dt=2026-09-15/",
    ]


@pytest.mark.parametrize(
    "value", ["${biz_date}", "2026-02-30", "2026-9-15", "2026-09-15/../other"]
)
def test_dates_still_strict(config, value, capsys):
    assert scheduler.main(["--config", str(config), "--biz-date", value]) == 2
    assert json.loads(capsys.readouterr().out)["error_code"] == "invalid_biz_date"


@pytest.mark.parametrize(
    "flag",
    [
        "--execute",
        "--allow-existing-output",
        "--job-arg=--output-uri=s3://bad",
        "--out=s3://bad",
    ],
)
def test_config_cannot_override_managed_boundaries(config, flag, capsys):
    value = json.loads(config.read_text())
    value["submit_args"].append(flag)
    config.write_text(json.dumps(value))
    assert scheduler.main(cli(config)) == 2
    assert json.loads(capsys.readouterr().out)["error_code"] == "managed_flag_in_config"


@pytest.mark.parametrize(
    "version,denied,expected",
    [
        ("3.5.7", False, None),
        ("3.4.4", False, "unsupported_worker_spark_version"),
        ("4.0.0", False, "unsupported_worker_spark_version"),
        ("3.5.3", True, "worker_kubernetes_permission_denied"),
    ],
)
def test_worker_checks_version_and_namespace_permissions_without_submission(
    config, monkeypatch, capsys, version, denied, expected
):
    args, _, _ = scheduler.make_plan(scheduler.parser().parse_args(cli(config)))
    args.spark_submit = config
    monkeypatch.setattr(scheduler.os, "access", lambda *_: True)
    monkeypatch.setenv("JAVA_HOME", "/worker/java")
    monkeypatch.setattr(scheduler.tempfile, "mkdtemp", forbidden)
    calls = []

    def run(command, **kwargs):
        assert command == [str(config), "--version"]
        assert kwargs["timeout"] == 30
        assert kwargs["env"]["JAVA_HOME"] == "/worker/java"
        return SimpleNamespace(
            returncode=0,
            stdout="",
            stderr=f"private-secret-DO_NOT_PRINT\nSpark version {version}\n",
        )

    def api(args, path, *, payload):
        assert path == "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews"
        assert payload["kind"] == "SelfSubjectAccessReview"
        attributes = payload["spec"]["resourceAttributes"]
        assert attributes["namespace"] == args.namespace
        calls.append(attributes)
        return {
            "status": {
                "allowed": not (
                    denied
                    and attributes["resource"] == "pods"
                    and attributes["verb"] == "create"
                ),
                "reason": "private-server-DO_NOT_PRINT",
            }
        }

    monkeypatch.setattr(scheduler.subprocess, "run", run)
    monkeypatch.setattr(scheduler, "api", api)
    if expected:
        with pytest.raises(scheduler.Error, match=expected):
            scheduler.check_worker(args)
    else:
        scheduler.check_worker(args)
    assert len(calls) == (6 if version.startswith("3.5.") else 0)
    assert all("subresource" not in item for item in calls)
    output = capsys.readouterr().out
    assert "DO_NOT_PRINT" not in output
    assert version in output and "/worker/java/bin/java" in output


def test_check_worker_mode_no_temp_directory_or_execution(config, monkeypatch):
    calls = []
    monkeypatch.setattr(scheduler, "check_worker", lambda args: calls.append(args))
    monkeypatch.setattr(scheduler, "execute", forbidden)
    monkeypatch.setattr(scheduler.tempfile, "mkdtemp", forbidden)
    assert scheduler.main(cli(config, "--check-worker")) == 0
    assert len(calls) == 1


@pytest.mark.parametrize(
    "path,value",
    [
        (None, None),
        (("metadata", "name"), "other"),
        (("metadata", "namespace"), "other"),
        (("metadata", "uid"), None),
        (("metadata", "labels", "trajfoundry-run-id"), "other"),
        (("metadata", "labels", "spark-role"), "executor"),
        (("metadata", "labels", "spark-app-name"), "other"),
        (("metadata", "labels", "spark-app-selector"), ""),
        (("metadata", "labels", "spark-app-selector"), "spark-invalid"),
        (("status", "phase"), "Failed"),
        (("status", "phase"), "Running"),
        (("status", "phase"), "Pending"),
        (("status", "containerStatuses"), []),
        (("status", "containerStatuses", 0, "name"), "sidecar"),
        (("status", "containerStatuses", 0, "state"), {}),
        (("status", "containerStatuses", 0, "state", "terminated", "exitCode"), 1),
        (("status", "containerStatuses", 0, "state", "terminated", "exitCode"), False),
        (("spec", "securityContext", "runAsUser"), 1007),
        (("spec", "securityContext", "runAsUser"), False),
        (("spec", "containers", 0, "name"), "other"),
        (("spec", "containers", 0, "securityContext", "runAsUser"), 1007),
        (("spec", "containers", 0, "securityContext", "runAsUser"), False),
        (("spec", "containers", 0, "image"), "other/image"),
        (("spec", "containers", 0, "command"), ["/bin/true"]),
        (("spec", "containers", 0, "args", 0), "executor"),
        (("spec", "containers", 0, "args", 4), "other.Class"),
        (("spec", "containers", 0, "args", 5), "local:///other.py"),
        (("spec", "containers", 0, "args", 7), "other-run"),
        (("spec", "nodeSelector", "kubernetes.io/arch"), "amd64"),
    ],
)
def test_final_validation_binds_success_to_expected_driver(
    config, monkeypatch, path, value
):
    args, plan, _ = scheduler.make_plan(scheduler.parser().parse_args(cli(config)))
    driver = pod(args, plan)
    if path is not None:
        parent = driver
        for key in path[:-1]:
            parent = parent[key]
        parent[path[-1]] = value

    def api(actual_args, actual_path):
        assert actual_args is args
        assert actual_path == scheduler.pod_path(args, plan)
        return driver

    monkeypatch.setattr(scheduler, "api", api)
    result = scheduler.validate_completion(args, plan, 0)
    assert result["passed"] is (path is None)
    assert result["submit_exit_code"] == 0
    assert result["driver_phase"] == driver["status"]["phase"]
    assert result["monitoring"] == "pod_status_only"
    assert result["completion_evidence"] == "driver_exit_after_validation"
    assert result["manifest_read_by_worker"] is False
    assert result["s3_access_from_worker"] is False
    assert (
        not {"completion", "manifest_sha256", "input_files", "trajectories"}
        & result.keys()
    )


@pytest.mark.parametrize(
    "change",
    [
        "submit_nonzero",
        "duplicate_status",
        "duplicate_container",
        "wrong_env",
        "duplicate_env",
    ],
)
def test_completion_rejects_ambiguous_or_unvalidated_execution(
    config, monkeypatch, change
):
    args, plan, _ = scheduler.make_plan(scheduler.parser().parse_args(cli(config)))
    value = pod(args, plan)
    container = value["spec"]["containers"][0]
    if change == "duplicate_status":
        value["status"]["containerStatuses"] *= 2
    elif change == "duplicate_container":
        value["spec"]["containers"] *= 2
    elif change in {"wrong_env", "duplicate_env"}:
        entry = next(
            item for item in container["env"] if item["name"] == "TF_CLUSTER_PYTHON"
        )
        if change == "wrong_env":
            entry["value"] = "/other/python"
        else:
            container["env"].insert(0, {**entry, "value": "/other/python"})
    monkeypatch.setattr(scheduler, "api", lambda *_: value)
    child_exit = 1 if change == "submit_nonzero" else 0
    assert scheduler.validate_completion(args, plan, child_exit)["passed"] is False


@pytest.mark.parametrize(
    "failure",
    [
        None,
        "monitor_404",
        "monitor_403",
        "monitor_connection",
        "missing_final",
        "child_exit",
        "driver_failed",
        "driver_nonzero",
    ],
)
def test_execute_uses_private_tmp_and_survives_monitor_errors_without_s3_or_logs(
    config, tmp_path, monkeypatch, capsys, failure
):
    from trajfoundry import credentials, s3

    args, plan, _ = scheduler.make_plan(scheduler.parser().parse_args(cli(config)))
    monkeypatch.setattr(scheduler.os, "geteuid", lambda: 1007)
    monkeypatch.setattr(credentials, "load_s3_credentials", forbidden)
    monkeypatch.setattr(s3, "create_s3_client", forbidden)
    original_mkdtemp = scheduler.tempfile.mkdtemp
    controls, processes, api_calls = [], [], []

    def mkdtemp(*, prefix, dir):
        assert dir == "/tmp"
        value = original_mkdtemp(prefix=prefix, dir=tmp_path)
        controls.append(Path(value))
        return value

    def popen(command, **kwargs):
        control = controls[0]
        assert kwargs["cwd"] == ROOT
        assert kwargs["stdout"].name == str(control / "submit.log")
        for role in ("driver", "executor"):
            assert (
                f"spark.kubernetes.{role}.podTemplateFile={control}/{role}-pod.json"
                in command
            )
            assert (
                json.loads((control / f"{role}-pod.json").read_text())
                == plan[f"{role}_template"]
            )

        class Process:
            calls = 0

            def wait(self, timeout):
                assert timeout == 60
                self.calls += 1
                if self.calls == 1:
                    raise subprocess.TimeoutExpired(command, timeout)
                return 1 if failure == "child_exit" else 0

        process = Process()
        processes.append(process)
        return process

    def api(actual_args, path, **kwargs):
        assert actual_args is args
        assert path == scheduler.pod_path(args, plan)
        assert not kwargs
        api_calls.append(path)
        if len(api_calls) == 1:
            if failure in {"monitor_404", "monitor_403"}:
                raise HTTPError(
                    "https://example", int(failure[-3:]), "DO_NOT_PRINT", {}, None
                )
            if failure == "monitor_connection":
                raise ConnectionError("DO_NOT_PRINT")
        elif failure == "missing_final":
            raise HTTPError("https://example", 404, "DO_NOT_PRINT", {}, None)
        value = pod(args, plan)
        value["status"]["message"] = "private-status-DO_NOT_PRINT"
        if len(api_calls) == 1:
            value["status"]["phase"] = "Running"
            value["status"]["containerStatuses"][0]["state"] = {"running": {}}
        elif failure == "driver_failed":
            value["status"]["phase"] = "Failed"
        elif failure == "driver_nonzero":
            value["status"]["containerStatuses"][0]["state"]["terminated"][
                "exitCode"
            ] = 2
        return value

    monkeypatch.setattr(scheduler.tempfile, "mkdtemp", mkdtemp)
    monkeypatch.setattr(scheduler.subprocess, "Popen", popen)
    monkeypatch.setattr(scheduler, "api", api)
    succeeds = failure is None or failure.startswith("monitor_")
    if succeeds:
        scheduler.execute(args, plan)
    else:
        with pytest.raises(scheduler.Error, match="cluster_completion_failed"):
            scheduler.execute(args, plan)
    assert len(processes) == 1 and processes[0].calls == 2
    assert len(api_calls) == 2
    assert stat.S_IMODE(controls[0].stat().st_mode) == 0o700
    result = json.loads((controls[0] / "final.json").read_text())
    assert result["passed"] is succeeds
    assert result["submit_exit_code"] == (1 if failure == "child_exit" else 0)
    assert result["driver_phase"] == (
        "Unknown"
        if failure == "missing_final"
        else "Failed"
        if failure == "driver_failed"
        else "Succeeded"
    )
    assert result["container_exit_codes"] == (
        {}
        if failure == "missing_final"
        else {args.container_name: 2 if failure == "driver_nonzero" else 0}
    )
    if failure == "missing_final":
        assert result["checks"] == {"completion_available": False}
    assert not args.control_dir.exists()
    output = capsys.readouterr().out
    assert "DO_NOT_PRINT" not in output + json.dumps(result)
    completed = next(
        json.loads(line)
        for line in output.splitlines()
        if json.loads(line)["event"] == "completed"
    )
    for field in (
        "submit_exit_code",
        "driver_phase",
        "container_exit_codes",
        "checks",
    ):
        assert completed[field] == result[field]
    if "application_id" in result:
        assert completed["application_id"] == result["application_id"]
    else:
        assert "application_id" not in completed
    if failure is not None and failure.startswith("monitor_"):
        assert "monitor_unavailable" in output


def test_main_executes_when_log_permission_is_denied(config, tmp_path, monkeypatch):
    from trajfoundry import credentials, s3

    original_make_plan = scheduler.make_plan
    original_mkdtemp = scheduler.tempfile.mkdtemp
    captured, permissions, gets, launches = {}, [], [], []

    def make_plan(options):
        args, plan, public = original_make_plan(options)
        captured.update(args=args, plan=plan)
        return args, plan, public

    def api(args, path, *, payload=None):
        assert "/log" not in path
        if payload is not None:
            attributes = payload["spec"]["resourceAttributes"]
            permissions.append(attributes)
            return {"status": {"allowed": attributes.get("subresource") != "log"}}
        assert path == scheduler.pod_path(args, captured["plan"])
        gets.append(path)
        return pod(args, captured["plan"])

    def popen(command, **kwargs):
        launches.append(command)
        return SimpleNamespace(wait=lambda timeout: 0)

    monkeypatch.setattr(credentials, "load_s3_credentials", forbidden)
    monkeypatch.setattr(s3, "create_s3_client", forbidden)
    monkeypatch.setattr(scheduler, "make_plan", make_plan)
    monkeypatch.setattr(scheduler, "api", api)
    monkeypatch.setattr(scheduler.os, "access", lambda *_: True)
    monkeypatch.setenv("JAVA_HOME", "/worker/java")
    monkeypatch.setattr(
        scheduler.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0, stdout="Spark version 3.5.7", stderr=""
        ),
    )
    monkeypatch.setattr(scheduler.subprocess, "Popen", popen)
    monkeypatch.setattr(
        scheduler.tempfile,
        "mkdtemp",
        lambda *, prefix, dir: original_mkdtemp(prefix=prefix, dir=tmp_path),
    )
    assert scheduler.main(cli(config, "--execute", "--spark-submit", str(config))) == 0
    assert len(permissions) == 6
    assert all("subresource" not in item for item in permissions)
    assert len(launches) == len(gets) == 1


@pytest.mark.parametrize("phase", ["Running", "private-phase-DO_NOT_PRINT"])
def test_progress_exposes_only_bounded_phase_and_container_exit_fields(
    config, monkeypatch, capsys, phase
):
    args, plan, _ = scheduler.make_plan(scheduler.parser().parse_args(cli(config)))
    value = pod(args, plan)
    value["status"].update(phase=phase, message="private-status-DO_NOT_PRINT")
    value["status"]["containerStatuses"].extend(
        [
            {"name": "sidecar", "state": {"running": {}}, "message": "DO_NOT_PRINT"},
            {
                "name": "private name DO_NOT_PRINT",
                "state": {"terminated": {"exitCode": 1}},
            },
        ]
    )

    def api(actual_args, path):
        assert path == scheduler.pod_path(args, plan)
        return value

    monkeypatch.setattr(scheduler, "api", api)
    scheduler.read_progress(args, plan)
    output = capsys.readouterr().out
    assert "DO_NOT_PRINT" not in output
    assert json.loads(output) == {
        "event": "driver_status",
        "driver_pod": plan["driver_pod"],
        "application_id": "spark-" + "a" * 32,
        "driver_phase": "Running" if phase == "Running" else "Unknown",
        "container_exit_codes": {args.container_name: 0, "sidecar": None},
    }


def test_api_uses_mounted_kubernetes_auth_with_tls_and_never_s3(config, monkeypatch):
    args, _, _ = scheduler.make_plan(scheduler.parser().parse_args(cli(config)))
    args.oauth_token_file.write_text("synthetic-kubernetes-token")
    context = SimpleNamespace(verify_flags=0)
    calls = []

    def ssl_context(*, cafile):
        assert cafile == str(args.ca_cert)
        return context

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def read(self, limit):
            assert limit == scheduler.MAX_RESPONSE + 1
            return b'{"status":{"allowed":true}}'

    class Opener:
        def open(self, request, timeout):
            assert timeout == 30
            assert (
                request.full_url
                == "https://example:443/apis/authorization.k8s.io/v1/selfsubjectaccessreviews"
            )
            assert (
                request.get_header("Authorization")
                == "Bearer synthetic-kubernetes-token"
            )
            assert json.loads(request.data) == {"test": True}
            assert request.get_method() == "POST"
            calls.append(request)
            return Response()

    monkeypatch.setattr(scheduler.ssl, "create_default_context", ssl_context)
    monkeypatch.setattr(scheduler, "HTTPSHandler", lambda **kwargs: kwargs)
    monkeypatch.setattr(scheduler, "build_opener", lambda *args: Opener())
    assert scheduler.api(
        args,
        "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews",
        payload={"test": True},
    )["status"]["allowed"]
    assert context.verify_flags & scheduler.ssl.VERIFY_X509_PARTIAL_CHAIN
    assert len(calls) == 1


def test_errors_and_unknown_cli_values_never_echo_secrets(config, monkeypatch, capsys):
    assert scheduler.main(cli(config, "--private=DO_NOT_PRINT")) == 2

    def fail(*args):
        raise RuntimeError("private-token-DO_NOT_PRINT")

    monkeypatch.setattr(scheduler, "check_worker", fail)
    assert scheduler.main(cli(config, "--check-worker")) == 2
    output = capsys.readouterr()
    assert "DO_NOT_PRINT" not in output.out + output.err
