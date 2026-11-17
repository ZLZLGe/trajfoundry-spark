"""Plan an isolated ARM Cluster run; submit only with an explicit --execute."""

from __future__ import annotations

import argparse
import copy
import json
import os
import re
import stat
import subprocess
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

PROFILES = {"starter": 16, "half": 32, "scale": 48}
LOCAL_DIR = "/tmp/trajfoundry-spark-local"
EVENT_LOG_DIR = "/spark/eventLog"
PROTECTED_JOB_FLAGS = {
    "--input-uri",
    "--output-uri",
    "--input-format",
    "--prefix-shard",
    "--auto-prefix-shards",
    "--max-prefix-shards",
    "--input-credentials-file",
    "--output-credentials-file",
    "--retain-s3-auxiliary",
    "--endpoint-url",
    "--region-name",
}


def absolute(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts or "\x00" in value:
        raise argparse.ArgumentTypeError("requires an absolute path without traversal")
    return path


def positive(value: str) -> int:
    result = int(value)
    if result <= 0:
        raise argparse.ArgumentTypeError("requires a positive integer")
    return result


def nonnegative(value: str) -> int:
    result = int(value)
    if result < 0:
        raise argparse.ArgumentTypeError("requires a non-negative integer")
    return result


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument(
        "--execute", action="store_true", help="actually submit; default only plans"
    )
    result.add_argument("--profile", choices=PROFILES, default="starter")
    result.add_argument(
        "--spark-submit", type=absolute, default=Path("/opt/spark/bin/spark-submit")
    )
    result.add_argument(
        "--project-root", type=absolute, default=Path(__file__).resolve().parents[1]
    )
    result.add_argument("--arm-python", type=absolute, required=True)
    result.add_argument("--java-home", type=absolute, default=Path("/opt/java/openjdk"))
    result.add_argument(
        "--driver-pod-template",
        type=absolute,
        required=True,
        help="non-secret JSON or YAML Pod template",
    )
    result.add_argument(
        "--executor-pod-template",
        type=absolute,
        required=True,
        help="non-secret JSON or YAML Pod template",
    )
    result.add_argument("--container-name", default="spark-kubernetes")
    result.add_argument("--image", required=True)
    result.add_argument("--queue", required=True)
    result.add_argument("--master", required=True, help="k8s://https://HOST[:PORT]")
    result.add_argument("--namespace", required=True)
    result.add_argument("--service-account", required=True)
    result.add_argument("--ca-cert", type=absolute, required=True)
    result.add_argument(
        "--oauth-token-file",
        type=absolute,
        required=True,
        help="file path only; never a token value",
    )
    result.add_argument("--run-as-user", type=positive, required=True)
    result.add_argument("--run-as-group", type=positive, default=100)
    result.add_argument(
        "--control-dir",
        type=absolute,
        required=True,
        help="new shared directory for templates/logs/evidence, separate from output",
    )
    output = result.add_mutually_exclusive_group(required=True)
    output.add_argument(
        "--output-dir",
        type=absolute,
        help="fresh shared filesystem output",
    )
    output.add_argument(
        "--output-uri", help="fresh, exclusive s3://bucket/prefix/ output"
    )
    result.add_argument("--input-uri", required=True)
    prefix = result.add_mutually_exclusive_group(required=True)
    prefix.add_argument("--prefix-shard", action="append")
    prefix.add_argument(
        "--auto-prefix-shards",
        action="store_true",
        help="discover immediate user prefixes under the input URI on one Spark partition",
    )
    result.add_argument("--max-prefix-shards", type=positive, default=1_000_000)
    result.add_argument(
        "--input-format",
        choices=("tokenplan", "boyue", "deepinfra"),
        default="tokenplan",
    )
    result.add_argument("--endpoint-url", required=True)
    result.add_argument("--region-name", default="us-east-1")
    result.add_argument("--input-credentials-file", type=absolute)
    result.add_argument("--output-credentials-file", type=absolute)
    result.add_argument(
        "--retain-s3-auxiliary",
        action="store_true",
        help="retain run-scoped S3 auxiliary objects; requires S3 output and manifest validation",
    )
    result.add_argument(
        "--allow-existing-output",
        action="store_true",
        help="explicit retry mode for an existing S3 output prefix",
    )
    result.add_argument("--driver-cores", type=positive, default=4)
    result.add_argument("--driver-memory-gib", type=positive, default=16)
    result.add_argument("--driver-overhead-gib", type=positive, default=48)
    result.add_argument(
        "--driver-max-result-size-gib",
        type=positive,
        default=4,
        help="serialized result limit per Spark action in GiB; budget Driver memory separately",
    )
    result.add_argument(
        "--timeout-seconds",
        type=nonnegative,
        default=0,
        help="optional overall runtime limit; 0 (default) leaves runtime unlimited",
    )
    result.add_argument(
        "--validation-mode",
        choices=("manifest", "small"),
        default="manifest",
        help="manifest streams its hash; small additionally does bounded full-output validation",
    )
    result.add_argument(
        "--baseline-dir",
        type=absolute,
        help="optional complete serial baseline, only with --validation-mode small",
    )
    result.add_argument(
        "--job-arg",
        action="append",
        default=[],
        help="repeat for non-secret normalizer arguments, e.g. --job-arg=--max-output-objects --job-arg=100000 or --job-arg=unlimited",
    )
    return result


def api_url(master: str) -> str:
    if not master.startswith("k8s://https://"):
        raise ValueError("master must use Kubernetes HTTPS")
    parsed = urlsplit(master.removeprefix("k8s://"))
    if (
        not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("master must contain only an HTTPS API authority")
    return master.removeprefix("k8s://").rstrip("/")


def safe_directory(path: Path) -> None:
    if path == Path("/") or any(
        parent.is_symlink() for parent in [path, *path.parents]
    ):
        raise ValueError("shared directories must not be roots or symlinks")


def check_credentials_file(path: Path, expected_uid: int) -> None:
    """Check protected-file metadata without reading any credential values."""
    if any(parent.is_symlink() for parent in [path, *path.parents]):
        raise ValueError("credential paths must not contain symlinks")
    metadata = path.stat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) not in {0o400, 0o600}
        or metadata.st_uid != expected_uid
    ):
        raise ValueError(
            "credentials must be a private regular file owned by the Pod UID"
        )


def prepare_template(args, path: Path, *, driver: bool, pod_name: str) -> dict:
    # Templates contain scheduling/mount configuration, not literal credentials.
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() in {".yaml", ".yml"}:
        try:
            import yaml
        except ModuleNotFoundError as error:
            raise ValueError(
                "YAML pod templates require the PyYAML dependency in the submitter environment"
            ) from error
        try:
            value = yaml.safe_load(text)
        except yaml.YAMLError as error:
            raise ValueError("template must be valid YAML") from error
    else:
        try:
            value = json.loads(text)
        except json.JSONDecodeError as error:
            raise ValueError(
                "template must be valid JSON (or use .yaml/.yml)"
            ) from error
    if not isinstance(value, dict):
        raise TypeError("template must be a Kubernetes Pod object")
    if value.get("kind") != "Pod":
        raise ValueError("template must be a Kubernetes Pod")
    value = copy.deepcopy(value)
    spec = value.setdefault("spec", {})
    containers = spec.get("containers", [])
    selected = [item for item in containers if item.get("name") == args.container_name]
    if len(selected) != 1:
        raise ValueError("template must identify exactly one application container")
    container = selected[0]
    spec.setdefault("nodeSelector", {})["kubernetes.io/arch"] = "arm64"
    spec.setdefault("securityContext", {}).update(
        runAsUser=args.run_as_user,
        runAsGroup=args.run_as_group,
        runAsNonRoot=True,
    )
    # Container-level securityContext must not silently override Pod identity.
    container.setdefault("securityContext", {}).update(
        runAsUser=args.run_as_user,
        runAsGroup=args.run_as_group,
        runAsNonRoot=True,
    )
    if args.timeout_seconds:
        spec["activeDeadlineSeconds"] = args.timeout_seconds
    else:
        # A base template may carry a previous smoke-test deadline. Explicitly
        # remove it instead of silently limiting an otherwise unlimited run.
        spec.pop("activeDeadlineSeconds", None)
    volumes = spec.setdefault("volumes", [])
    mounts = container.setdefault("volumeMounts", [])

    # The platform Spark defaults use /spark/eventLog.  The supplied ARM
    # template mounts the shared event PVC at /share (with subPath=dsj), but
    # does not mount that PVC at the event-log path.  Mount the same PVC a
    # second time in the generated per-run template.  This keeps the source
    # YAML unchanged while making the configured event-log directory writable
    # and persistent for the Flamingo History/Listener integration.
    event_volume = next(
        (
            item
            for item in volumes
            if item.get("name") == "spark-event-nfs"
            or (
                isinstance(item.get("persistentVolumeClaim"), dict)
                and item["persistentVolumeClaim"].get("claimName")
                == "spark-event-nfs-pvc"
            )
        ),
        None,
    )
    if event_volume is not None and not any(
        item.get("mountPath") == EVENT_LOG_DIR for item in mounts
    ):
        event_mount = next(
            (
                item
                for item in mounts
                if item.get("name") == event_volume["name"]
                and item.get("mountPath") == "/share"
            ),
            None,
        )
        event_log_mount = {"name": event_volume["name"], "mountPath": EVENT_LOG_DIR}
        # Reuse the template's writable shared subPath (currently `dsj`).
        # Mounting the PVC root would create the directory but is not writable
        # by the application UID on this cluster.
        if event_mount and event_mount.get("subPath"):
            event_subpath = event_mount["subPath"]
            try:
                relative_control = args.control_dir.relative_to(Path("/share"))
            except ValueError:
                relative_control = None
            if relative_control is not None and event_subpath == "dsj":
                # The `/share` mount is backed by the PVC's `dsj` subPath.
                # Bind this run's event directory rather than the unwritable
                # `dsj` root itself.
                event_subpath = str(Path(event_subpath) / relative_control / "eventLog")
            event_log_mount["subPath"] = event_subpath
        mounts.append(event_log_mount)
    shared_volumes = {
        item.get("name")
        for item in volumes
        if any(kind in item for kind in ("persistentVolumeClaim", "nfs", "csi"))
    }
    shared_mounts = [
        Path(item["mountPath"]) for item in mounts if item.get("name") in shared_volumes
    ]
    for required in (
        args.project_root,
        args.arm_python,
        args.control_dir,
        args.output_dir,
        args.input_credentials_file,
        args.output_credentials_file,
    ):
        if required is None:
            continue
        if not any(
            mount.is_absolute() and (mount == required or mount in required.parents)
            for mount in shared_mounts
        ):
            raise ValueError(
                "shared PVC/NFS/CSI mounts must cover source, Python, control, output and credential paths"
            )
    if any(item.get("name", "").startswith("spark-local-dir-") for item in volumes):
        raise ValueError("base template must not contain Spark local-dir volumes")
    if any(item.get("mountPath") == LOCAL_DIR for item in mounts):
        raise ValueError("base template conflicts with private local scratch")
    volumes.append({"name": "spark-local-dir-trajfoundry", "emptyDir": {}})
    mounts.append({"name": "spark-local-dir-trajfoundry", "mountPath": LOCAL_DIR})
    environment = {
        "PATH": f"{args.arm_python.parent}:{args.java_home}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "JAVA_HOME": str(args.java_home),
        "PYSPARK_PYTHON": str(args.arm_python),
        "PYTHONPATH": str(args.project_root / "src"),
        "TMPDIR": LOCAL_DIR,
    }
    if driver:
        container["command"] = [
            "/bin/bash",
            str(args.project_root / "spark/cluster_driver_entrypoint.sh"),
        ]
        container.pop("args", None)
        environment.update(
            TF_CLUSTER_LOG_PATH=str(args.control_dir / "driver.log"),
            TF_CLUSTER_PYTHON=str(args.arm_python),
            TF_CLUSTER_SOURCE_DIR=str(args.project_root / "src"),
            TF_CLUSTER_EXPECTED_UID=str(args.run_as_user),
            TF_CLUSTER_DRIVER_POD=pod_name,
        )
    container["env"] = [
        item for item in container.get("env", []) if item.get("name") not in environment
    ]
    container["env"].extend(
        {"name": key, "value": val} for key, val in environment.items()
    )
    return value


def make_plan(args, *, run_id: str | None = None) -> dict:
    api_url(args.master)
    properties = args.project_root / "spark/cluster-isolated.properties"
    if any(
        line.strip() and not line.lstrip().startswith("#")
        for line in properties.read_text().splitlines()
    ):
        raise ValueError("isolated properties must not contain inherited settings")
    for name in (args.namespace, args.service_account):
        if not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?", name):
            raise ValueError("invalid Kubernetes namespace or ServiceAccount name")
    for path in (args.control_dir, args.output_dir):
        if path is None:
            continue
        safe_directory(path)
    if args.output_dir is not None and (
        args.control_dir == args.output_dir
        or args.control_dir in args.output_dir.parents
        or args.output_dir in args.control_dir.parents
    ):
        raise ValueError(
            "control and output directories must be separate, non-nested paths"
        )
    if args.control_dir.exists():
        raise ValueError("control directory must be new and exclusive")
    if (
        args.output_dir is not None
        and args.output_dir.exists()
        and (not args.output_dir.is_dir() or any(args.output_dir.iterdir()))
    ):
        raise ValueError("output directory must be new or empty")
    if args.baseline_dir and args.validation_mode != "small":
        raise ValueError("serial comparison requires bounded small validation mode")
    if args.retain_s3_auxiliary and (
        not args.output_uri or args.validation_mode != "manifest"
    ):
        raise ValueError(
            "retaining S3 auxiliary objects requires S3 output and manifest validation"
        )
    if args.allow_existing_output and not args.output_uri:
        raise ValueError("allowing existing output requires S3 output")
    if args.output_uri and args.validation_mode == "small":
        raise ValueError("small validation requires shared filesystem output")
    if args.output_credentials_file and not args.output_uri:
        raise ValueError("output credentials require S3 output")
    for path in (args.input_credentials_file, args.output_credentials_file):
        if path is not None:
            check_credentials_file(path, args.run_as_user)
    if any(
        item.startswith("--")
        and any(flag.startswith(item.split("=", 1)[0]) for flag in PROTECTED_JOB_FLAGS)
        for item in args.job_arg
    ):
        raise ValueError(
            "job-arg must not override input/output or authentication routing"
        )
    # Check input scopes and routing using the same contract as the actual job.
    sys.path.insert(0, str(args.project_root / "src"))
    import trajfoundry
    from trajfoundry.s3 import parse_s3_uri
    from trajfoundry.spark.config import SparkNormalizeConfig

    Path(trajfoundry.__file__).resolve().relative_to(
        (args.project_root / "src").resolve()
    )
    output_uri = (
        parse_s3_uri(args.output_uri).uri
        if args.output_uri is not None
        else str(args.output_dir)
    )
    input_location = parse_s3_uri(args.input_uri)
    if args.output_uri is not None:
        output_location = parse_s3_uri(output_uri)
        if input_location.bucket == output_location.bucket and (
            input_location.prefix.startswith(output_location.prefix)
            or output_location.prefix.startswith(input_location.prefix)
        ):
            raise ValueError("input and output S3 prefixes must not overlap")
    SparkNormalizeConfig(
        input_uri=args.input_uri,
        output_uri=output_uri,
        input_format=args.input_format,
        prefix_shards=tuple(args.prefix_shard or ()),
        auto_prefix_shards=args.auto_prefix_shards,
        max_prefix_shards=args.max_prefix_shards,
        retain_s3_auxiliary=args.retain_s3_auxiliary,
        allow_existing_output=args.allow_existing_output,
    )
    run_id = run_id or uuid.uuid4().hex
    if not re.fullmatch(r"[a-f0-9]{32}", run_id):
        raise ValueError("invalid generated run identifier")
    pod_name = "trajfoundry-cluster-" + run_id[:16]
    instances = PROFILES[args.profile]
    cores = instances * 8
    conf = {
        # Use the same verified API authority inside the Driver. Some platform
        # nodes cannot reliably resolve the default kubernetes.default.svc.
        "spark.kubernetes.driver.master": api_url(args.master),
        "spark.kubernetes.namespace": args.namespace,
        "spark.kubernetes.authenticate.submission.caCertFile": str(args.ca_cert),
        "spark.kubernetes.authenticate.submission.oauthTokenFile": str(
            args.oauth_token_file
        ),
        "spark.kubernetes.trust.certificates": "false",
        "spark.kubernetes.driver.connectionTimeout": "20000",
        "spark.kubernetes.driver.requestTimeout": "60000",
        "spark.kubernetes.authenticate.driver.serviceAccountName": args.service_account,
        "spark.kubernetes.authenticate.executor.serviceAccountName": args.service_account,
        "spark.kubernetes.driver.container.image": args.image,
        "spark.kubernetes.executor.container.image": args.image,
        "spark.kubernetes.driver.podTemplateFile": str(
            args.control_dir / "driver-pod.json"
        ),
        "spark.kubernetes.executor.podTemplateFile": str(
            args.control_dir / "executor-pod.json"
        ),
        "spark.kubernetes.driver.podTemplateContainerName": args.container_name,
        "spark.kubernetes.executor.podTemplateContainerName": args.container_name,
        "spark.kubernetes.driver.label.queue": args.queue,
        "spark.kubernetes.executor.label.queue": args.queue,
        "spark.kubernetes.driver.label.trajfoundry-run-id": run_id,
        "spark.kubernetes.executor.label.trajfoundry-run-id": run_id,
        "spark.kubernetes.driver.pod.name": pod_name,
        "spark.pyspark.driver.python": str(args.arm_python),
        "spark.pyspark.python": str(args.arm_python),
        "spark.kubernetes.driverEnv.PYTHONPATH": str(args.project_root / "src"),
        "spark.executorEnv.PYTHONPATH": str(args.project_root / "src"),
        "spark.local.dir": LOCAL_DIR,
        "spark.jars.ivy": LOCAL_DIR + "/ivy",
        "spark.kubernetes.local.dirs.tmpfs": "false",
        "spark.driver.cores": str(args.driver_cores),
        "spark.kubernetes.driver.limit.cores": str(args.driver_cores),
        "spark.driver.memory": f"{args.driver_memory_gib}g",
        "spark.driver.memoryOverhead": f"{args.driver_overhead_gib}g",
        "spark.driver.maxResultSize": f"{args.driver_max_result_size_gib}g",
        "spark.executor.instances": str(instances),
        "spark.executor.cores": "8",
        "spark.kubernetes.executor.request.cores": "8",
        "spark.kubernetes.executor.limit.cores": "8",
        "spark.executor.memory": "16g",
        "spark.executor.memoryOverhead": "48g",
        "spark.memory.offHeap.enabled": "false",
        "spark.dynamicAllocation.enabled": "false",
        "spark.speculation": "false",
        "spark.default.parallelism": str(cores),
        "spark.sql.shuffle.partitions": str(cores * 2),
        "spark.task.cpus": "1",
        # Preserve bounded Spark defaults: one transient block/broadcast fetch
        # failure must not abort the whole date. Failed executors are excluded
        # from repeated attempts; speculative duplicate writers stay disabled.
        "spark.task.maxFailures": "4",
        "spark.stage.maxConsecutiveAttempts": "4",
        "spark.excludeOnFailure.enabled": "true",
        "spark.kubernetes.submission.waitAppCompletion": "true",
        "spark.kubernetes.executor.deleteOnTermination": "true",
        "spark.kubernetes.driver.service.deleteOnTermination": "true",
        # Persist Spark's structured event log for the platform History Server.
        # The supplied Pod template/platform must provide this path as a
        # writable shared mount; otherwise the log remains local to the Pod.
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": EVENT_LOG_DIR,
        # The Spark image currently supplies this listener through
        # spark-defaults.conf.  Keep it explicit so a different Spark conf
        # directory cannot silently remove Flamingo platform monitoring.
        "spark.extraListeners": "flamingo.spark.listener.SparkFlamingoListener",
    }
    # Do not set spark.executor.pyspark.memory: Python shares the explicit 48g
    # overhead envelope; adding it would add another Pod memory request/limit.
    command = [
        str(args.spark_submit),
        "--properties-file",
        str(args.project_root / "spark/cluster-isolated.properties"),
        "--master",
        args.master,
        "--deploy-mode",
        "cluster",
        "--name",
        pod_name,
    ]
    for key, val in conf.items():
        command.extend(["--conf", f"{key}={val}"])
    command.extend(
        [
            "local://" + str(args.project_root / "spark/tokenplan_normalize.py"),
            "--input-uri",
            args.input_uri,
            "--output-uri",
            output_uri,
            "--endpoint-url",
            args.endpoint_url,
            "--region-name",
            args.region_name,
        ]
    )
    if args.auto_prefix_shards:
        command.append("--auto-prefix-shards")
        command.extend(["--max-prefix-shards", str(args.max_prefix_shards)])
    else:
        for shard in args.prefix_shard:
            command.extend(["--prefix-shard", shard])
    if args.input_credentials_file:
        command.extend(["--input-credentials-file", str(args.input_credentials_file)])
    if args.output_credentials_file:
        command.extend(["--output-credentials-file", str(args.output_credentials_file)])
    if args.retain_s3_auxiliary:
        command.append("--retain-s3-auxiliary")
    if args.allow_existing_output:
        command.append("--allow-existing-output")
    command.extend(
        [
            "--max-scope-summaries",
            "5000000",
            "--max-scope-summary-mib",
            "4096",
            "--max-routing-mib",
            # A known no-session hotspot has a serialized routing plan of
            # about 1.25 GiB. Keep a finite guard with room for larger scopes.
            "4096",
            "--max-routing-snapshots",
            "2000000",
            "--max-root-mib",
            "2048",
        ]
    )
    command.extend(["--input-format", args.input_format])
    command.extend(args.job_arg)
    return {
        "run_id": run_id,
        "driver_pod": pod_name,
        "profile": args.profile,
        "executor_instances": instances,
        "executor_cores": 8,
        "executor_jvm_gib": 16,
        "executor_overhead_gib": 48,
        "total_executor_cores": cores,
        "total_executor_memory_gib": instances * 64,
        "control_dir": str(args.control_dir),
        "output_dir": str(args.output_dir) if args.output_dir is not None else None,
        "output_uri": output_uri,
        "retain_s3_auxiliary": args.retain_s3_auxiliary,
        "timeout_seconds": args.timeout_seconds,
        "runtime_unlimited": args.timeout_seconds == 0,
        "validation_mode": args.validation_mode,
        "command": command,
        "driver_template": prepare_template(
            args, args.driver_pod_template, driver=True, pod_name=pod_name
        ),
        "executor_template": prepare_template(
            args, args.executor_pod_template, driver=False, pod_name=pod_name
        ),
    }


def child_environment() -> dict[str, str]:
    excluded = {
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "PYSPARK_DRIVER_PYTHON",
        "PYSPARK_PYTHON",
        "SPARK_SUBMIT_OPTS",
        "JAVA_TOOL_OPTIONS",
        "_JAVA_OPTIONS",
    }
    return {key: val for key, val in os.environ.items() if key not in excluded}


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    started_at = datetime.now(UTC).isoformat()
    plan = None
    control_created = False
    submission_attempted = False
    submit_exit_code = None
    stage = "preflight"
    try:
        plan = make_plan(args)
        public = {
            key: val
            for key, val in plan.items()
            if key not in {"command", "driver_template", "executor_template"}
        }
        print(
            json.dumps({"execute": args.execute, **public}, sort_keys=True), flush=True
        )
        if not args.execute:
            return 0
        if os.geteuid() != args.run_as_user:
            raise ValueError(
                "private control directory requires submitter and Pod UID to match"
            )
        args.control_dir.mkdir(mode=0o700, parents=False, exist_ok=False)
        # This directory is part of the shared PVC mounted into the Driver.
        # It is created before Kubernetes evaluates the generated subPath
        # mount for /spark/eventLog.
        (args.control_dir / "eventLog").mkdir(mode=0o700, exist_ok=False)
        control_created = True
        stage = "prepare_submission"
        for filename, content in (
            ("driver-pod.json", plan["driver_template"]),
            ("executor-pod.json", plan["executor_template"]),
            ("plan.json", public),
        ):
            with (args.control_dir / filename).open("x") as handle:
                json.dump(content, handle, sort_keys=True)
                handle.write("\n")
        with (args.control_dir / "submit.log").open("x") as log:
            stage = "submit_or_wait"
            submission_attempted = True
            process = subprocess.run(
                plan["command"],
                stdout=log,
                stderr=subprocess.STDOUT,
                env=child_environment(),
                timeout=args.timeout_seconds + 60 if args.timeout_seconds else None,
                check=False,
            )
        submit_exit_code = process.returncode
        # The Spark submission watcher may return zero even when the Driver died.
        from validate_cluster import validate_run

        stage = "validate_completion"
        result = validate_run(args, plan)
        result["submit_exit_code"] = process.returncode
        result["passed"] = result["passed"] and process.returncode == 0
        result.update(
            started_at=started_at,
            finished_at=datetime.now(UTC).isoformat(),
            exit_code=0 if result["passed"] else 2,
        )
        with (args.control_dir / "final.json").open("x") as handle:
            json.dump(result, handle, sort_keys=True, indent=2)
            handle.write("\n")
        print(json.dumps(result, sort_keys=True), flush=True)
        return 0 if result["passed"] else 2
    except Exception as error:  # noqa: BLE001 - never log credential-bearing exception text
        result = {
            "passed": False,
            "error_type": type(error).__name__,
            "stage": stage,
            "control_dir": str(args.control_dir),
            "cleanup_required": submission_attempted,
            "submit_exit_code": submit_exit_code,
            "started_at": started_at,
            "finished_at": datetime.now(UTC).isoformat(),
            "exit_code": 2,
        }
        if plan is not None:
            result.update(run_id=plan["run_id"], driver_pod=plan["driver_pod"])
        # A Driver failure commonly has no completion report. Preserve a final
        # status even then, but never touch a pre-existing/unowned control dir.
        if control_created:
            try:
                with (args.control_dir / "final.json").open("x") as handle:
                    json.dump(result, handle, sort_keys=True, indent=2)
                    handle.write("\n")
            except OSError:
                result["final_report_write_failed"] = True
        print(json.dumps(result, sort_keys=True), flush=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
