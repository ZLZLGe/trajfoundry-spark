"""Submit Cluster jobs from an unprivileged Worker; default is offline planning."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import shutil
import ssl
import subprocess
import tempfile
from pathlib import Path
from urllib.request import HTTPSHandler, ProxyHandler, Request, build_opener

_spec = importlib.util.spec_from_file_location(
    "scheduler_date_plan", Path(__file__).with_name("run_scheduled_tokenplan.py")
)
assert _spec and _spec.loader
scheduled = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(scheduled)
submit = scheduled.submit
Error = scheduled.ScheduledError
emit = scheduled.emit
MAX_RESPONSE = 16 * 1024 * 1024
PERMISSIONS = (
    ("pods", None, "create"),
    ("pods", None, "get"),
    ("pods", None, "list"),
    ("pods", None, "watch"),
    ("services", None, "create"),
    ("configmaps", None, "create"),
)


def parser():
    result = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    result.error = scheduled.invalid_arguments
    result.add_argument("--config", type=submit.absolute, required=True)
    result.add_argument("--biz-date", required=True)
    for name in ("spark-submit", "driver-pod-template", "executor-pod-template"):
        result.add_argument("--" + name, type=submit.absolute)
    mode = result.add_mutually_exclusive_group()
    mode.add_argument("--execute", action="store_true")
    mode.add_argument("--check-worker", action="store_true")
    return result


def make_plan(options):
    args, _, _, public = scheduled.make_schedule(options.config, options.biz_date)
    for name in ("spark_submit", "driver_pod_template", "executor_pod_template"):
        if getattr(options, name) is not None:
            setattr(args, name, getattr(options, name))
    plan = submit.make_plan(args)
    driver = plan["driver_template"]["spec"]["containers"]
    container = next(item for item in driver if item["name"] == args.container_name)
    container["command"] = [
        "/bin/bash",
        str(args.project_root / "spark/scheduler_driver_entrypoint.sh"),
    ]
    container["env"] = [
        item for item in container["env"] if item["name"] != "TF_CLUSTER_LOG_PATH"
    ]
    command = plan["command"]
    index = command.index(
        "local://" + str(args.project_root / "spark/tokenplan_normalize.py")
    )
    command[index : index + 1] = [
        "local://" + str(args.project_root / "spark/scheduler_cluster_job.py"),
        "--tf-run-id",
        plan["run_id"],
        "--tf-driver-pod",
        plan["driver_pod"],
        "--tf-pod-uid",
        str(args.run_as_user),
    ]
    for key in ("run_dir", "control_dir"):
        public.pop(key, None)
    public.update(
        run_id=plan["run_id"],
        driver_pod=plan["driver_pod"],
        spark_submit=str(args.spark_submit),
        worker_control="private /tmp directory on execute",
        s3_access_from_worker=False,
        monitoring="pod_status_only",
        completion_evidence="driver_exit_after_validation",
        dynamic_counts_available=False,
    )
    return args, plan, public


def api(args, path, *, payload=None):
    """Use only the Worker's mounted Kubernetes authentication, never S3 secrets."""
    context = ssl.create_default_context(cafile=str(args.ca_cert))
    context.verify_flags |= ssl.VERIFY_X509_PARTIAL_CHAIN
    opener = build_opener(ProxyHandler({}), HTTPSHandler(context=context))
    headers = {"Authorization": "Bearer " + args.oauth_token_file.read_text().strip()}
    body = None
    if payload is not None:
        body = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"
    request = Request(submit.api_url(args.master) + path, data=body, headers=headers)
    with opener.open(request, timeout=30) as response:
        value = response.read(MAX_RESPONSE + 1)
    if len(value) > MAX_RESPONSE:
        raise Error("kubernetes_response_too_large")
    return json.loads(value)


def check_worker(args):
    """Explicit read-only checks: local version and non-persistent access reviews."""
    if not args.spark_submit.is_file() or not os.access(args.spark_submit, os.X_OK):
        raise Error("spark_submit_not_executable")
    java = (
        str(Path(os.environ["JAVA_HOME"]) / "bin/java")
        if os.environ.get("JAVA_HOME")
        else shutil.which("java")
    )
    if not java or not os.access(java, os.X_OK):
        raise Error("java_not_executable")
    version = subprocess.run(
        [str(args.spark_submit), "--version"],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
        env=scheduled.launch_environment(args),
    )
    match = re.search(
        r"\bversion\s+(\d+\.\d+\.\d+)\b", version.stdout + version.stderr, re.IGNORECASE
    )
    if version.returncode or match is None:
        raise Error("spark_version_unavailable")
    emit(
        "worker_runtime",
        spark_version=match[1],
        java_path=java,
        submitter_uid=os.geteuid(),
        pod_uid=args.run_as_user,
    )
    if not match[1].startswith("3.5."):
        raise Error("unsupported_worker_spark_version")
    allowed = True
    for resource, subresource, verb in PERMISSIONS:
        attributes = {
            "namespace": args.namespace,
            "group": "",
            "resource": resource,
            "verb": verb,
        }
        if subresource:
            attributes["subresource"] = subresource
        review = api(
            args,
            "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews",
            payload={
                "apiVersion": "authorization.k8s.io/v1",
                "kind": "SelfSubjectAccessReview",
                "spec": {"resourceAttributes": attributes},
            },
        )
        passed = review.get("status", {}).get("allowed") is True
        allowed = allowed and passed
        emit(
            "worker_permission",
            resource=resource,
            subresource=subresource,
            verb=verb,
            passed=passed,
        )
    if not allowed:
        raise Error("worker_kubernetes_permission_denied")
    emit("worker_checks_passed", business_resources_created=False, s3_accessed=False)


def pod_path(args, plan):
    return f"/api/v1/namespaces/{args.namespace}/pods/{plan['driver_pod']}"


def pod_status_fields(status):
    """Select phase and numeric exits without exposing any Pod messages."""
    phase = status.get("phase")
    if phase not in {"Pending", "Running", "Succeeded", "Failed", "Unknown"}:
        phase = "Unknown"
    exits = {}
    for item in status.get("containerStatuses", []):
        name = item.get("name", "")
        code = item.get("state", {}).get("terminated", {}).get("exitCode")
        if isinstance(name, str) and re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", name):
            exits[name] = code if type(code) is int else None
    return {"driver_phase": phase, "container_exit_codes": exits}


def pod_application_id(pod):
    labels = pod.get("metadata", {}).get("labels", {})
    value = labels.get("spark-app-selector")
    if isinstance(value, str) and re.fullmatch(r"spark-[a-f0-9]{32}", value):
        return value
    return None


def read_progress(args, plan):
    """Report only Pod status; never fetch container logs."""
    pod = api(args, pod_path(args, plan))
    fields = pod_status_fields(pod.get("status", {}))
    application_id = pod_application_id(pod)
    if application_id is not None:
        fields["application_id"] = application_id
    emit(
        "driver_status",
        driver_pod=plan["driver_pod"],
        **fields,
    )
    return application_id


def driver_configuration_matches(args, plan, pod):
    """Bind exit status to the runtime and validating application we submitted."""
    spec = pod.get("spec", {})
    selected = [
        item
        for item in spec.get("containers", [])
        if item.get("name") == args.container_name
    ]
    if len(selected) != 1:
        return False
    container = selected[0]
    expected = next(
        item
        for item in plan["driver_template"]["spec"]["containers"]
        if item["name"] == args.container_name
    )
    pod_uid = spec.get("securityContext", {}).get("runAsUser")
    container_uid = container.get("securityContext", {}).get("runAsUser")
    expected_environment = {
        item["name"]: item["value"]
        for item in expected["env"]
        if item["name"]
        in {
            "TF_CLUSTER_PYTHON",
            "TF_CLUSTER_SOURCE_DIR",
            "TF_CLUSTER_EXPECTED_UID",
            "TF_CLUSTER_DRIVER_POD",
        }
    }
    runtime_environment = [
        item
        for item in container.get("env", [])
        if item.get("name") in expected_environment
    ]
    script = str(args.project_root / "spark/scheduler_cluster_job.py")
    index = plan["command"].index("local://" + script)
    # Spark 3.5 preserves local:// resources and adds this launcher prefix.
    expected_arguments = [
        "driver",
        "--properties-file",
        "/opt/spark/conf/spark.properties",
        "--class",
        "org.apache.spark.deploy.PythonRunner",
        *plan["command"][index:],
    ]
    return (
        type(pod_uid) is int
        and pod_uid == args.run_as_user
        and type(container_uid) is int
        and container_uid == args.run_as_user
        and container.get("image") == args.image
        and container.get("command") == expected["command"]
        and spec.get("nodeSelector", {}).get("kubernetes.io/arch") == "arm64"
        and len(runtime_environment) == len(expected_environment)
        and {item["name"]: item.get("value") for item in runtime_environment}
        == expected_environment
        and container.get("args") == expected_arguments
    )


def validate_completion(args, plan, child_exit):
    pod = api(args, pod_path(args, plan))
    metadata, status = pod.get("metadata", {}), pod.get("status", {})
    labels = metadata.get("labels", {})
    containers = status.get("containerStatuses", [])
    checks = {
        "submit_exit_zero": type(child_exit) is int and child_exit == 0,
        "driver_identity": metadata.get("name") == plan["driver_pod"]
        and metadata.get("namespace") == args.namespace
        and bool(metadata.get("uid"))
        and labels.get("spark-role") == "driver"
        and isinstance(labels.get("spark-app-selector"), str)
        and bool(re.fullmatch(r"spark-[a-f0-9]{32}", labels["spark-app-selector"]))
        and labels.get("spark-app-name") == plan["driver_pod"]
        and labels.get("trajfoundry-run-id") == plan["run_id"],
        "driver_configuration": driver_configuration_matches(args, plan, pod),
        "driver_succeeded": status.get("phase") == "Succeeded"
        and bool(containers)
        and sum(item.get("name") == args.container_name for item in containers) == 1
        and all(
            type(item.get("state", {}).get("terminated", {}).get("exitCode")) is int
            and item["state"]["terminated"]["exitCode"] == 0
            for item in containers
        ),
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "submit_exit_code": child_exit,
        **pod_status_fields(status),
        "run_id": plan["run_id"],
        "driver_pod": plan["driver_pod"],
        "driver_pod_uid": metadata.get("uid"),
        "output_uri": plan["output_uri"],
        "application_id": pod_application_id(pod),
        "completion_evidence": "driver_exit_after_validation",
        "monitoring": "pod_status_only",
        "manifest_read_by_worker": False,
        "s3_access_from_worker": False,
    }


def execute(args, plan):
    control = Path(tempfile.mkdtemp(prefix="trajfoundry-scheduler-", dir="/tmp"))
    emit("worker_control", control_dir=str(control), driver_pod=plan["driver_pod"])
    command = list(plan["command"])
    for role in ("driver", "executor"):
        path = control / f"{role}-pod.json"
        path.write_text(json.dumps(plan[f"{role}_template"]), encoding="utf-8")
        prefix = f"spark.kubernetes.{role}.podTemplateFile="
        command = [
            prefix + str(path) if item.startswith(prefix) else item for item in command
        ]
    (control / "command.json").write_text(json.dumps(command), encoding="utf-8")
    child_exit = None
    last_application_id = None
    try:
        with (control / "submit.log").open("x") as log:
            process = subprocess.Popen(
                command,
                stdout=log,
                stderr=subprocess.STDOUT,
                env=scheduled.launch_environment(args),
                cwd=args.project_root,
            )
            while True:
                try:
                    child_exit = process.wait(timeout=60)
                    break
                except subprocess.TimeoutExpired:
                    try:
                        observed_application_id = read_progress(args, plan)
                        if observed_application_id is not None:
                            last_application_id = observed_application_id
                    except Exception as error:  # noqa: BLE001 - keep waiting for the real submission
                        emit(
                            "monitor_unavailable",
                            error_type=type(error).__name__,
                            error_code=error.code
                            if isinstance(error, Error)
                            else "operation_failed",
                            driver_pod=plan["driver_pod"],
                        )
                    emit(
                        "waiting",
                        driver_pod=plan["driver_pod"],
                        control_dir=str(control),
                    )
        result = validate_completion(args, plan, child_exit)
    except Exception as error:  # noqa: BLE001 - persist a sanitized failed completion
        result = {
            "passed": False,
            "run_id": plan["run_id"],
            "driver_pod": plan["driver_pod"],
            "submit_exit_code": child_exit,
            "driver_phase": "Unknown",
            "container_exit_codes": {},
            "checks": {"completion_available": False},
            "completion_evidence": "driver_exit_after_validation",
            "monitoring": "pod_status_only",
            "manifest_read_by_worker": False,
            "s3_access_from_worker": False,
            "error_type": type(error).__name__,
            "error_code": error.code
            if isinstance(error, Error)
            else "operation_failed",
        }
        if last_application_id is not None:
            result["application_id"] = last_application_id
    (control / "final.json").write_text(
        json.dumps(result, sort_keys=True), encoding="utf-8"
    )
    emit(
        "completed",
        passed=result["passed"],
        control_dir=str(control),
        driver_pod=plan["driver_pod"],
        **{
            key: result[key]
            for key in (
                "submit_exit_code",
                "driver_phase",
                "container_exit_codes",
                "checks",
                "application_id",
                "error_type",
                "error_code",
            )
            if key in result
        },
    )
    if not result["passed"]:
        raise Error("cluster_completion_failed")


def main(argv=None):
    stage = "arguments"
    try:
        options = parser().parse_args(argv)
        stage = "plan"
        args, plan, public = make_plan(options)
        emit(
            "plan", execute=options.execute, check_worker=options.check_worker, **public
        )
        if not options.execute and not options.check_worker:
            return 0
        stage = "worker_checks"
        check_worker(args)
        if options.execute:
            stage = "execute_and_validate"
            execute(args, plan)
        return 0
    except Exception as error:  # noqa: BLE001 - never log credential-bearing exception text
        emit(
            "failed",
            stage=stage,
            error_type=type(error).__name__,
            error_code=error.code if isinstance(error, Error) else "operation_failed",
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
