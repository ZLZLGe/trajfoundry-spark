"""Read-only, fail-closed checks after Spark's potentially misleading watcher exit."""

from __future__ import annotations

import hashlib
import json
import ssl
from contextlib import suppress
from pathlib import Path
from urllib.request import HTTPSHandler, ProxyHandler, Request, build_opener

RUNTIME_MARKER = "TF_CLUSTER_DRIVER_RUNTIME "
MAX_LOG_LINE = 1024 * 1024
MAX_SMALL_MANIFEST = 16 * 1024 * 1024
MAX_SMALL_LINEAGE = 32 * 1024 * 1024
MAX_SMALL_INPUTS = 10_000
MAX_SMALL_FILE = 32 * 1024 * 1024
MAX_SMALL_TOTAL = 256 * 1024 * 1024
HASH_CHUNK_BYTES = 256 * 1024


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(HASH_CHUNK_BYTES), b""):
            digest.update(block)
    return digest.hexdigest()


def s3_manifest_sha256(args, output_uri: str) -> str:
    """Hash just the published manifest with constant memory and no local copy.

    This is completion evidence, not an independent full-output validation.
    Explicit credentials use the same permission-checked loader as executors;
    absence of an explicit file preserves the SDK default credential chain.
    """
    from trajfoundry.credentials import load_s3_credentials
    from trajfoundry.s3 import create_s3_client, parse_s3_uri

    location = parse_s3_uri(output_uri)
    client = body = None
    try:
        options = {"max_pool_connections": 1}
        credentials_file = getattr(args, "output_credentials_file", None)
        if credentials_file is not None:
            options["credentials"] = load_s3_credentials(credentials_file)
        client = create_s3_client(
            getattr(args, "endpoint_url", None) or "https://s3.amazonaws.com",
            getattr(args, "region_name", "us-east-1"),
            **options,
        )
        response = client.get_object(
            Bucket=location.bucket, Key=location.key("manifest.json")
        )
        body = response.get("Body")
        length = response.get("ContentLength")
        if (
            body is None
            or not callable(getattr(body, "read", None))
            or type(length) is not int
            or length < 0
        ):
            raise ValueError("invalid S3 manifest response")
        digest = hashlib.sha256()
        size = 0
        while True:
            block = body.read(HASH_CHUNK_BYTES)
            if not isinstance(block, bytes) or len(block) > HASH_CHUNK_BYTES:
                raise ValueError("invalid S3 manifest response body")
            if not block:
                break
            size += len(block)
            if size > length:
                raise ValueError("S3 manifest length mismatch")
            digest.update(block)
        if size != length:
            raise ValueError("S3 manifest length mismatch")
        return digest.hexdigest()
    except Exception:  # noqa: BLE001 - SDK errors may contain authentication details
        raise RuntimeError("S3 manifest verification failed") from None
    finally:
        for resource in (body, client):
            close = getattr(resource, "close", None)
            if callable(close):
                # Cleanup must not expose SDK error strings or mask the result.
                with suppress(Exception):
                    close()


def read_reports(path: Path) -> tuple[dict, dict]:
    runtime = []
    jobs = []
    with path.open() as handle:
        while line := handle.readline(MAX_LOG_LINE + 1):
            if len(line) > MAX_LOG_LINE:
                raise ValueError("Driver log line exceeds bounded evidence size")
            if line.startswith(RUNTIME_MARKER):
                runtime.append(json.loads(line[len(RUNTIME_MARKER) :]))
            elif line.startswith("{"):
                try:
                    value = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if (
                    isinstance(value, dict)
                    and "manifest_sha256" in value
                    and "input_files" in value
                ):
                    jobs.append(value)
            if len(runtime) > 1 or len(jobs) > 1:
                raise ValueError("ambiguous Driver evidence")
    if len(runtime) != 1 or len(jobs) != 1:
        raise ValueError("missing Driver runtime or completion evidence")
    return runtime[0], jobs[0]


def fetch_driver_pod(args, pod_name: str) -> dict:
    context = ssl.create_default_context(cafile=str(args.ca_cert))
    # Trust the configured CA/intermediate, with TLS hostname verification kept.
    context.verify_flags |= ssl.VERIFY_X509_PARTIAL_CHAIN
    opener = build_opener(ProxyHandler({}), HTTPSHandler(context=context))
    request = Request(
        args.master.removeprefix("k8s://").rstrip("/")
        + "/api/v1/namespaces/"
        + args.namespace
        + "/pods/"
        + pod_name,
        headers={
            "Authorization": "Bearer " + args.oauth_token_file.read_text().strip()
        },
    )
    with opener.open(request, timeout=30) as response:
        return json.load(response)


def small_manifest(root: Path) -> dict:
    path = root / "manifest.json"
    if path.stat().st_size > MAX_SMALL_MANIFEST:
        raise ValueError("manifest exceeds small-validation envelope")
    with path.open() as handle:
        value = json.load(handle)
    counts = value["counts"]
    if (
        counts["input_files"] > MAX_SMALL_INPUTS
        or counts["accepted"] + counts["quarantined_trajectories"] > MAX_SMALL_INPUTS
    ):
        raise ValueError("full validation is restricted to a small bounded sample")
    if (root / "lineage.jsonl").stat().st_size > MAX_SMALL_LINEAGE:
        raise ValueError("lineage exceeds small-validation envelope")
    total = 0
    for entry in value["files"]:
        relative = Path(entry["path"])
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("invalid manifest file path")
        path = root / relative
        if path.is_symlink() or any(parent.is_symlink() for parent in path.parents):
            raise ValueError("small output must not reference symlinks")
        size = path.stat().st_size
        total += size
        if size > MAX_SMALL_FILE or total > MAX_SMALL_TOTAL:
            raise ValueError("artifact bytes exceed small-validation envelope")
    return value


def small_lineage(root: Path) -> dict:
    records = {}
    with (root / "lineage.jsonl").open() as handle:
        for line in handle:
            value = json.loads(line)
            value["origins"].sort(key=lambda origin: json.dumps(origin, sort_keys=True))
            if value["trajectory_id"] in records:
                raise ValueError("duplicate lineage ID")
            records[value["trajectory_id"]] = value
    return records


def validate_small_output(args, job: dict) -> dict[str, bool]:
    from trajfoundry.validation import validate_output

    manifest = small_manifest(args.output_dir)
    counts = manifest["counts"]
    checks = {
        "output_valid": validate_output(args.output_dir).valid,
        "manifest_counts_match": all(
            counts[key] == job[result_key]
            for key, result_key in (
                ("input_files", "input_files"),
                ("skipped_inputs", "parse_failures"),
                ("accepted", "accepted"),
                ("quarantined_trajectories", "quarantined_trajectories"),
                ("quarantined_records", "quarantined_records"),
            )
        ),
    }
    if args.baseline_dir:
        baseline = small_manifest(args.baseline_dir)

        def files(value):
            return {
                item["path"]: (item["sha256"], item["bytes"])
                for item in value["files"]
                if item["path"] != "lineage.jsonl"
            }

        checks.update(
            baseline_valid=validate_output(args.baseline_dir).valid,
            baseline_files_match=files(manifest) == files(baseline),
            baseline_lineage_match=small_lineage(args.output_dir)
            == small_lineage(args.baseline_dir),
            baseline_counts_match=counts == baseline["counts"],
            baseline_input_root_match=manifest["input_root"] == baseline["input_root"],
        )
    return checks


def validate_run(args, plan: dict) -> dict:
    from trajfoundry.spark.storage import local_output_path

    output_uri = getattr(args, "output_uri", None)
    output_dir = getattr(args, "output_dir", None)
    if output_uri is None:
        if output_dir is None:
            raise ValueError("missing output destination")
        output_uri = str(output_dir)
    elif output_dir is not None:
        raise ValueError("ambiguous output destination")
    root = local_output_path(output_uri)
    backend = "filesystem" if root is not None else "s3"
    if backend == "s3" and args.validation_mode == "small":
        raise ValueError("small-output validation requires filesystem output")
    if plan.get("output_uri", output_uri) != output_uri:
        raise ValueError("output destination differs from submitted plan")
    pod = fetch_driver_pod(args, plan["driver_pod"])
    metadata = pod.get("metadata", {})
    labels = metadata.get("labels", {})
    status = pod.get("status", {})
    containers = status.get("containerStatuses", [])
    runtime, job = read_reports(args.control_dir / "driver.log")
    zero_trajectories_accounted = (
        getattr(args, "input_format", None) in {"boyue", "deepinfra"}
        and all(
            type(job.get(name)) is int and job[name] >= 0
            for name in (
                "input_files",
                "trajectories",
                "accepted",
                "quarantined_trajectories",
                "quarantined_records",
                "skipped_inputs",
                "verified_trajectory_objects",
            )
        )
        and job["trajectories"]
        == job["accepted"]
        == job["quarantined_trajectories"]
        == job["verified_trajectory_objects"]
        == 0
        and job["input_files"] == job["quarantined_records"] + job["skipped_inputs"]
    )
    checks = {
        "driver_identity": metadata.get("name") == plan["driver_pod"]
        and metadata.get("namespace") == args.namespace
        and bool(metadata.get("uid"))
        and labels.get("spark-role") == "driver"
        and bool(labels.get("spark-app-selector"))
        and labels.get("trajfoundry-run-id") == plan["run_id"],
        "driver_succeeded": status.get("phase") == "Succeeded"
        and bool(containers)
        and all(
            item.get("state", {}).get("terminated", {}).get("exitCode") == 0
            for item in containers
        ),
        "arm_driver_runtime": runtime.get("passed") is True
        and runtime.get("hostname") == plan["driver_pod"]
        and runtime.get("uid") == args.run_as_user
        and runtime.get("architecture", "").lower() in {"arm64", "aarch64"}
        and tuple(runtime.get("python_major_minor", [])) >= (3, 11)
        and runtime.get("python_executable") == str(args.arm_python),
        "job_completed": job.get("output_backend") == backend
        and job.get("output_uri") == output_uri
        and type(job.get("input_files")) is int
        and job["input_files"] > 0
        and type(job.get("trajectories")) is int
        and (job["trajectories"] > 0 or zero_trajectories_accounted),
    }
    # Do not touch artifacts/credentials for the wrong or an unsuccessful run.
    if all(checks.values()):
        actual_hash = (
            file_sha256(root / "manifest.json")
            if root is not None
            else s3_manifest_sha256(args, output_uri)
        )
        checks["manifest_hash_matches"] = job.get("manifest_sha256") == actual_hash
    else:
        checks["manifest_hash_matches"] = False
    if all(checks.values()) and args.validation_mode == "small":
        checks.update(validate_small_output(args, job))
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "validation_mode": args.validation_mode,
        "artifact_validation": "bounded_full_output"
        if args.validation_mode == "small"
        else "manifest_hash_only",
        "driver_pod": plan["driver_pod"],
        "driver_pod_uid": metadata.get("uid"),
        "application_id": labels.get("spark-app-selector"),
        "run_id": plan["run_id"],
        "output_dir": str(root) if root is not None else None,
        "output_uri": output_uri,
        "output_backend": backend,
        "job": job,
    }
