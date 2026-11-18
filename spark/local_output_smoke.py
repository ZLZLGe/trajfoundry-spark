#!/usr/bin/env python3
"""Read complete small S3 user shards and validate output in a shared directory.

Submit with this checkout's source directory in Driver and Executor PYTHONPATH.
Client mode is the default; explicitly select the expected Cluster mode for an
all-ARM Pod run. Inputs use the normal read-only S3 reader and credential chain.
Outputs never use an S3 client. Select small complete user shards and a fresh
exclusive shared output directory before submitting.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import socket
import sys
from pathlib import Path
from typing import Any

MARKER = "TF_SPARK_LOCAL_OUTPUT_SMOKE "


def forbidden_output_s3_client(*_args, **_kwargs):
    raise RuntimeError("filesystem output must never create an output S3 client")


def runtime_report(source_dir: str) -> dict[str, Any]:
    source = Path(source_dir).resolve(strict=True)
    sys.path.insert(0, str(source))
    import trajfoundry
    import trajfoundry.spark.job

    origin = Path(trajfoundry.__file__).resolve(strict=True)
    origin.relative_to(source)
    return {
        "hostname": socket.gethostname(),
        "uid": os.geteuid(),
        "gid": os.getegid(),
        "architecture": platform.machine(),
        "python_version": platform.python_version(),
        "python_major_minor": list(sys.version_info[:2]),
        "python_executable": sys.executable,
        "project_file": str(origin),
    }


def executor_runtime(rows, *, source_dir: str):
    from pyspark import BarrierTaskContext

    context = BarrierTaskContext.get()
    context.barrier()
    result = runtime_report(source_dir)
    result["partition"] = context.partitionId()
    result["items"] = len(list(rows))
    yield result


def effective_deploy_mode(report: dict[str, Any]) -> str:
    # The official container entrypoint runs an inner spark-submit in Client
    # mode, even for an external Cluster submission. Spark's Cluster submitter
    # supplies submitInDriver automatically; a Pod name alone is not evidence.
    if report.get("submit_in_driver") is True:
        if (
            report["master"].startswith("k8s://")
            and report.get("driver_pod_name")
            and report["driver_pod_name"] == report["driver"]["hostname"]
        ):
            return "cluster"
        return "unverified"
    return "client" if report["deploy_mode"] == "client" else "unverified"


def distributed_evidence(report: dict[str, Any]) -> bool:
    executors = report["executors"]
    driver = report["driver"]
    hosts = {item["hostname"] for item in executors}
    expected_mode = report.get("expected_deploy_mode", "client")
    # Spark supplies the actual Driver Pod name to its Cluster-mode Driver.
    # Match that identity to the runtime hostname, not merely a Kubernetes
    # environment variable which may also be present in a Client notebook Pod.
    cluster_runtime_ok = expected_mode != "cluster" or bool(
        report.get("driver_pod_name")
        and report["driver_pod_name"] == driver["hostname"]
        and all(
            item.get("architecture", "").lower() in {"aarch64", "arm64"}
            for item in [driver, *executors]
        )
    )
    return bool(
        report["master"].startswith("k8s://")
        and expected_mode in {"client", "cluster"}
        and effective_deploy_mode(report) == expected_mode
        and cluster_runtime_ok
        and len(executors) == len(hosts) == 2
        and driver["hostname"] not in hosts
        and {item["partition"] for item in executors} == {0, 1}
        and sum(item["items"] for item in executors) == 2
        and tuple(driver["python_major_minor"]) >= (3, 11)
        and all(
            item["python_major_minor"] == driver["python_major_minor"]
            and item["uid"] == driver["uid"]
            for item in executors
        )
    )


def inspect_output(output_dir: Path, result: dict[str, Any]) -> dict[str, Any]:
    from trajfoundry.validation import validate_output

    validation = validate_output(output_dir)
    manifest = json.loads((output_dir / "manifest.json").read_bytes())
    counts = manifest["counts"]
    checks = {
        "input_files": counts.get("input_files") == result["input_files"],
        "parse_failures": counts.get("skipped_inputs") == result["parse_failures"],
        "accepted": counts.get("accepted") == result["accepted"],
        "quarantined_trajectories": counts.get("quarantined_trajectories")
        == result["quarantined_trajectories"],
        "quarantined_records": counts.get("quarantined_records")
        == result["quarantined_records"],
        "trajectories": counts.get("accepted", 0)
        + counts.get("quarantined_trajectories", 0)
        == result["trajectories"],
    }
    # No raw trajectories, user identities, or diagnostic source records in logs.
    return {
        "validation": {
            "valid": validation.valid,
            "error_count": len(validation.errors),
            "counts": validation.counts,
        },
        "manifest_count_checks": checks,
        "manifest_counts_ok": all(checks.values()),
        "manifest_path": str(output_dir / "manifest.json"),
    }


def absolute_path(value: str) -> str:
    if not Path(value).is_absolute():
        raise argparse.ArgumentTypeError("must be an absolute filesystem path")
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-uri", required=True)
    parser.add_argument(
        "--prefix-shard",
        action="append",
        required=True,
        help="complete relative user prefix, e.g. USER_ID/; repeat for each user",
    )
    parser.add_argument("--output-dir", type=absolute_path, required=True)
    parser.add_argument("--source-dir", type=absolute_path, required=True)
    parser.add_argument("--endpoint-url", required=True)
    parser.add_argument("--region-name", default="us-east-1")
    parser.add_argument(
        "--expected-deploy-mode",
        choices=("client", "cluster"),
        default="client",
        help="require this actual Spark deploy mode; Cluster also requires all ARM Pods",
    )
    parser.add_argument(
        "--input-credentials-file",
        type=absolute_path,
        help="optional protected shared input credential file; only its path is passed",
    )
    args = parser.parse_args(argv)
    report: dict[str, Any] = {
        "data_mode": "s3_readonly",
        "expected_deploy_mode": args.expected_deploy_mode,
        "output_dir": args.output_dir,
        "prefix_shard_count": len(args.prefix_shard),
        "stage": "driver_runtime",
        "passed": False,
    }
    spark = None
    try:
        report["driver"] = runtime_report(args.source_dir)
        from pyspark.sql import SparkSession

        from trajfoundry.spark.config import SparkNormalizeConfig
        from trajfoundry.spark.job import run_spark_job

        config = SparkNormalizeConfig(
            input_uri=args.input_uri,
            output_uri=args.output_dir,
            input_format="tokenplan",
            prefix_shards=tuple(args.prefix_shard),
            endpoint_url=args.endpoint_url,
            region_name=args.region_name,
            input_credentials_path=args.input_credentials_file,
        )
        # Resource and deploy configuration remains owned by spark-submit.
        report["stage"] = "spark_start"
        spark = SparkSession.builder.getOrCreate()
        sc = spark.sparkContext
        sc.setLogLevel("WARN")
        report.update(
            application_id=sc.applicationId,
            master=sc.master,
            deploy_mode=sc.getConf().get("spark.submit.deployMode", ""),
            driver_pod_name=sc.getConf().get("spark.kubernetes.driver.pod.name", ""),
            submit_in_driver=sc.getConf()
            .get("spark.kubernetes.submitInDriver", "false")
            .lower()
            == "true",
            executor_python_command=sc.pythonExec,
        )
        report["effective_deploy_mode"] = effective_deploy_mode(report)
        if (
            not report["master"].startswith("k8s://")
            or report["effective_deploy_mode"] != args.expected_deploy_mode
        ):
            raise RuntimeError(
                "Kubernetes submission does not match expected deploy mode"
            )
        report["stage"] = "executor_runtime"
        source_dir = args.source_dir
        report["executors"] = (
            sc.parallelize(range(2), 2)
            .barrier()
            .mapPartitions(lambda rows: executor_runtime(rows, source_dir=source_dir))
            .collect()
        )
        report["distributed_ok"] = distributed_evidence(report)
        if not report["distributed_ok"]:
            raise RuntimeError("two compatible independent Executors were not verified")
        report["stage"] = "normalize"
        # No source_reader injection: actual inputs use iter_shard_payloads.
        report["job"] = run_spark_job(
            config,
            spark=spark,
            client_factory=forbidden_output_s3_client,
        )
        report["stage"] = "validate_output"
        report["output"] = inspect_output(Path(args.output_dir), report["job"])
        report["passed"] = bool(
            report["job"]["input_files"] > 0
            and report["job"]["trajectories"] > 0
            and report["output"]["validation"]["valid"]
            and report["output"]["manifest_counts_ok"]
        )
        report["stage"] = "complete"
    except Exception as error:  # noqa: BLE001 - safe diagnostic boundary
        report["error_type"] = type(error).__name__
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception as error:  # noqa: BLE001 - cleanup failure is a failure
                report["stop_error_type"] = type(error).__name__
                report["passed"] = False
        print(
            MARKER + json.dumps(report, ensure_ascii=False, sort_keys=True), flush=True
        )
    return 0 if report["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
