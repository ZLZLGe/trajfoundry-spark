#!/usr/bin/env python3
"""Synthetic Client-mode probe; imports project code without running a data job."""

from __future__ import annotations

import argparse
import importlib
import json
import os
import platform
import shutil
import socket
import sys
import uuid
from pathlib import Path
from typing import Any

MARKER = "TF_SPARK_CLIENT_PROBE "
DEPENDENCIES = (
    "boto3",
    "botocore",
    "orjson",
    "pydantic",
    "pydantic_core",
    "typer",
    "zstandard",
)


def emit(value: dict[str, Any]) -> None:
    print(MARKER + json.dumps(value, ensure_ascii=False, sort_keys=True), flush=True)


def normalized_architecture(value: str) -> str:
    return {"arm64": "aarch64", "amd64": "x86_64"}.get(value.lower(), value.lower())


def same_executable(actual: str, expected: str) -> bool:
    # Do not resolve symlinks: two venvs may share a base interpreter but have
    # different native dependencies and editable project installations.
    return bool(actual and expected) and os.path.abspath(actual) == os.path.abspath(
        expected
    )


def check_runtime(
    role: str,
    *,
    source_dir: str,
    expected_python: str,
    expected_architecture: str,
    require_pyarrow: bool = False,
) -> dict[str, Any]:
    info: dict[str, Any] = {
        "role": role,
        "hostname": socket.gethostname(),
        "pid": os.getpid(),
        "python_version": platform.python_version(),
        "python_major_minor": list(sys.version_info[:2]),
        "python_version_ok": sys.version_info >= (3, 11),
        "python_executable": sys.executable,
        "expected_python": expected_python,
        "python_executable_ok": same_executable(sys.executable, expected_python),
        "architecture": platform.machine(),
        "architecture_ok": normalized_architecture(platform.machine())
        == normalized_architecture(expected_architecture),
        "source_dir": source_dir,
        "source_visible": Path(source_dir).is_dir(),
        "scratch_free_bytes": shutil.disk_usage("/tmp").free,
        "dependencies": {},
        "project_import_ok": False,
    }
    if info["source_visible"]:
        sys.path.insert(0, source_dir)
    for name in (*DEPENDENCIES, *(("pyarrow",) if require_pyarrow else ())):
        try:
            module = importlib.import_module(name)
            version = str(getattr(module, "__version__", "unknown"))
            valid = name != "pydantic" or version.split(".")[0] == "2"
            info["dependencies"][name] = {"ok": valid, "version": version}
        except Exception as error:  # noqa: BLE001 - report import failures without sensitive details
            info["dependencies"][name] = {
                "ok": False,
                "error_type": type(error).__name__,
            }
    try:
        package = importlib.import_module("trajfoundry")
        origin = Path(package.__file__).resolve()
        info["project_file"] = str(origin)
        origin.relative_to(Path(source_dir).resolve())
        importlib.import_module("trajfoundry.spark.job")
        info["project_import_ok"] = True
    except Exception as error:  # noqa: BLE001 - report wrong source/native library failures
        info["project_error_type"] = type(error).__name__
    info["ready"] = bool(
        info["python_version_ok"]
        and info["python_executable_ok"]
        and info["architecture_ok"]
        and info["source_visible"]
        and info["project_import_ok"]
        and all(item["ok"] for item in info["dependencies"].values())
    )
    return info


def executor_probe(
    rows, *, source_dir, expected_python, expected_architecture, require_pyarrow=False
):
    from pyspark import BarrierTaskContext

    context = BarrierTaskContext.get()
    context.barrier()
    result = check_runtime(
        "executor",
        source_dir=source_dir,
        expected_python=expected_python,
        expected_architecture=expected_architecture,
        require_pyarrow=require_pyarrow,
    )
    result["partition"] = context.partitionId()
    result["sum"] = sum(rows)
    yield result


def evaluate_report(
    report: dict[str, Any], *, expected_executor_python: str
) -> dict[str, Any]:
    driver = report["driver"]
    executors = report["executors"]
    hosts = {item["hostname"] for item in executors}
    report["distinct_executor_hosts"] = len(hosts)
    report["distributed_ok"] = bool(
        report["master"].startswith("k8s://")
        and report["deploy_mode"] == "client"
        and len(executors) == 2
        and len(hosts) == 2
        and driver["hostname"] not in hosts
        and {item["partition"] for item in executors} == {0, 1}
        and sum(item["sum"] for item in executors) == 4950
    )
    report["environment_ok"] = bool(
        driver["ready"]
        and all(item["ready"] for item in executors)
        and all(
            item["python_major_minor"] == driver["python_major_minor"]
            for item in executors
        )
        and same_executable(report["executor_python_command"], expected_executor_python)
    )
    report["passed"] = report["distributed_ok"] and report["environment_ok"]
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--driver-python", required=True)
    parser.add_argument("--executor-python", required=True)
    parser.add_argument("--driver-architecture", default="x86_64")
    parser.add_argument("--executor-architecture", default="aarch64")
    parser.add_argument("--require-pyarrow", action="store_true")
    args = parser.parse_args(argv)
    run_id = uuid.uuid4().hex[:16]
    report: dict[str, Any] = {
        "kind": "result",
        "run_id": run_id,
        "data_mode": "synthetic",
        "passed": False,
        "distributed_ok": False,
        "environment_ok": False,
    }
    spark = None
    try:
        report["driver"] = check_runtime(
            "driver",
            source_dir=args.source_dir,
            expected_python=args.driver_python,
            expected_architecture=args.driver_architecture,
            require_pyarrow=args.require_pyarrow,
        )
        emit({"kind": "driver_runtime", "run_id": run_id, "driver": report["driver"]})
        if not report["driver"]["ready"]:
            report["failure_stage"] = "driver_runtime"
        else:
            from pyspark.sql import SparkSession

            # Submission owns master, deploy mode, authentication and resources.
            # Never call the business entrypoint or SparkNormalizeConfig here.
            spark = SparkSession.builder.getOrCreate()
            sc = spark.sparkContext
            sc.setLogLevel("WARN")
            report.update(
                {
                    "spark_version": spark.version,
                    "application_id": sc.applicationId,
                    "master": sc.master,
                    "deploy_mode": sc.getConf().get("spark.submit.deployMode", ""),
                    "executor_python_command": sc.pythonExec,
                }
            )
            emit(
                {
                    "kind": "spark_started",
                    "run_id": run_id,
                    **{
                        key: report[key]
                        for key in (
                            "spark_version",
                            "application_id",
                            "master",
                            "deploy_mode",
                            "executor_python_command",
                        )
                    },
                }
            )
            if report["deploy_mode"] != "client" or not sc.master.startswith("k8s://"):
                report["failure_stage"] = "submission_mode"
            elif not same_executable(sc.pythonExec, args.executor_python):
                report["failure_stage"] = "executor_python_command"
            else:
                source_dir = args.source_dir
                executor_python = args.executor_python
                executor_architecture = args.executor_architecture
                require_pyarrow = args.require_pyarrow
                report["executors"] = (
                    sc.parallelize(range(100), 2)
                    .barrier()
                    .mapPartitions(
                        lambda rows: executor_probe(
                            rows,
                            source_dir=source_dir,
                            expected_python=executor_python,
                            expected_architecture=executor_architecture,
                            require_pyarrow=require_pyarrow,
                        )
                    )
                    .collect()
                )
                evaluate_report(report, expected_executor_python=executor_python)
    except Exception as error:  # noqa: BLE001 - diagnostic boundary; no raw Spark exception text
        # Spark exceptions can include transport credentials; emit only the type.
        report["error_type"] = type(error).__name__
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception as error:  # noqa: BLE001 - cleanup failure must fail the probe
                report["stop_error_type"] = type(error).__name__
                report["passed"] = False
        emit(report)
    return 0 if report["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
