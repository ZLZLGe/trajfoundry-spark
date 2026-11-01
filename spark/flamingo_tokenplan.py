"""Run the TokenPlan normalizer with a Flamingo-created Spark session.

This is the client-mode entrypoint for Jupyter/Flamingo.  It deliberately
does not call ``spark-submit`` and it does not upload the project to
DolphinScheduler's resource center: the Driver and Executors import the
checkout directly from the shared ``/share`` tree.

The helper supports both SDK shapes currently deployed on the platform:

* pyflamingo 0.1.x: ``SparkAppSpec(venv_path=...)`` plus
  ``generate_spark_conf``;
* pyflamingo 1.x: ``SparkApp.create(..., mode="in_cluster_client")`` with
  ``SparkAppSpec(virtualenv=...)``.

The business code remains in ``tokenplan_normalize.py``.  This module only
creates the platform session and injects it into ``run_spark_job``.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

if sys.version_info < (3, 11):  # noqa: UP036 - provide a clear runtime error
    raise RuntimeError(
        "TrajFoundry Spark requires Python 3.11+; use a Python 3.11 "
        "Flamingo-enabled Jupyter kernel"
    )


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = PROJECT_ROOT / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))
if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

import tokenplan_normalize

_PROTECTED_CONF_KEYS = {
    "spark.hadoop.fs.s3a.access.key",
    "spark.hadoop.fs.s3a.secret.key",
    "spark.hadoop.fs.s3a.session.token",
}


def _model_fields(model: type) -> set[str]:
    """Return constructor fields for both Pydantic v1-style SDKs and v2."""

    fields = getattr(model, "model_fields", None)
    if fields is None:
        fields = getattr(model, "__fields__", None)
    return set(fields or ())


def _runtime_values(virtualenv: str) -> tuple[str, str]:
    """Return ``(absolute path, SDK virtualenv name)`` without exposing secrets."""

    candidate = Path(virtualenv)
    if candidate.is_absolute():
        path = candidate
        name = candidate.name
        # An explicit path is authoritative for this notebook process.  The
        # SDK only accepts a name in its newer API and resolves that name
        # below FLAMINGO_VENV_HOME.
        os.environ["FLAMINGO_VENV_HOME"] = str(candidate.parent)
    else:
        home = Path(os.environ.get("FLAMINGO_VENV_HOME", "/share/envs"))
        name = virtualenv
        path = home / name
    return str(path), name


def _build_spec(
    *,
    app_name: str,
    queue: str,
    virtualenv: str,
    driver_cores: int,
    driver_memory: str,
    driver_memory_overhead: str,
    executor_instances: int,
    executor_cores: int,
    executor_memory: str,
    executor_memory_overhead: str,
    parallelism: int,
    extra_conf: dict[str, Any],
) -> Any:
    """Build a SparkAppSpec compatible with installed pyflamingo versions."""

    from flamingo import SparkAppSpec

    venv_path, venv_name = _runtime_values(virtualenv)
    fields = _model_fields(SparkAppSpec)
    values: dict[str, Any] = {
        "app_name": app_name,
        "queue": queue,
        "driver_cores": driver_cores,
        "driver_memory": driver_memory,
        "driver_memory_overhead": driver_memory_overhead,
        "executor_instances": executor_instances,
        "executor_cores": executor_cores,
        "executor_memory": executor_memory,
        "executor_memory_overhead": executor_memory_overhead,
        "parallelism": parallelism,
        "deploy_mode": "client",
        "extra_conf": extra_conf,
    }
    # pyflamingo 0.1.x calls this an absolute venv_path.  pyflamingo 1.x
    # calls it virtualenv and resolves the name under FLAMINGO_VENV_HOME.
    if "venv_path" in fields:
        values["venv_path"] = venv_path
    elif "virtualenv" in fields:
        values["virtualenv"] = venv_name
    else:  # pragma: no cover - protects against an incompatible SDK release
        raise RuntimeError("installed Flamingo SDK has no virtualenv field")
    return SparkAppSpec(
        **{key: value for key, value in values.items() if key in fields}
    )


def _create_legacy_client_session(spec: Any) -> Any:
    """Create a client-mode session for pyflamingo 0.1.x."""

    from flamingo import generate_spark_conf
    from pyspark import SparkConf
    from pyspark.sql import SparkSession

    generated = generate_spark_conf(spec)
    conf = SparkConf()
    conf.setAll((key, str(value)) for key, value in generated.items())
    # The platform listener is normally supplied by the Spark installation's
    # spark-defaults.conf.  Keep these explicit as well so event logging is
    # not lost when a notebook selects a different Spark conf directory.
    conf.set("spark.submit.deployMode", "client")
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.dir", "/spark/eventLog")
    conf.set("spark.extraListeners", "flamingo.spark.listener.SparkFlamingoListener")
    master = "k8s://https://{host}:{port}".format(
        host=os.environ.get("KUBERNETES_SERVICE_HOST", "10.96.0.1"),
        port=os.environ.get("KUBERNETES_SERVICE_PORT", "443"),
    )
    return (
        SparkSession.builder.master(master)
        .appName(spec.app_name)
        .config(conf=conf)
        .getOrCreate()
    )


def create_flamingo_session(
    *,
    app_name: str,
    queue: str,
    virtualenv: str,
    driver_cores: int = 4,
    driver_memory: str = "8g",
    driver_memory_overhead: str = "4g",
    executor_instances: int = 32,
    executor_cores: int = 8,
    executor_memory: str = "16g",
    executor_memory_overhead: str = "48g",
    parallelism: int = 256,
    extra_conf: dict[str, Any] | None = None,
    pod_template_file: str | None = None,
) -> Any:
    """Create a Flamingo-managed Spark client session.

    ``extra_conf`` is limited to non-secret Spark settings.  S3 credentials
    must remain an executor-readable protected file and are passed to the
    normalizer as a path, never as Spark configuration values.
    """

    source_path = str(SOURCE_ROOT)
    conf: dict[str, Any] = {
        "spark.submit.deployMode": "client",
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": "/spark/eventLog",
        # listener-1.0.0.jar is supplied by the platform Spark image.
        "spark.extraListeners": "flamingo.spark.listener.SparkFlamingoListener",
        "spark.local.dir": "/tmp/trajfoundry-spark-local",
        "spark.kubernetes.local.dirs.tmpfs": "false",
        "spark.dynamicAllocation.enabled": "false",
        "spark.speculation": "false",
        "spark.memory.offHeap.enabled": "false",
        "spark.driver.maxResultSize": "1g",
        "spark.task.cpus": "1",
        "spark.task.maxFailures": "4",
        "spark.stage.maxConsecutiveAttempts": "4",
        "spark.excludeOnFailure.enabled": "true",
        # The source is on the shared filesystem.  This is the executor-side
        # equivalent of adding a local Python package with --py-files.
        "spark.executorEnv.PYTHONPATH": source_path,
        "spark.kubernetes.driverEnv.PYTHONPATH": source_path,
    }
    if extra_conf:
        if _PROTECTED_CONF_KEYS.intersection(extra_conf):
            raise ValueError(
                "S3 credential values must be supplied through the protected "
                "credential-file arguments, not Spark configuration"
            )
        conf.update(extra_conf)
    if pod_template_file is not None:
        template = Path(pod_template_file)
        if not template.is_absolute():
            raise ValueError("pod_template_file must be an absolute path")
        if not template.is_file():
            raise FileNotFoundError(f"pod template not found: {template}")
        # This is the caller's own ARM template.  It overrides only the
        # generated queue template; image, namespace and service-account
        # constants remain controlled by the Flamingo platform.
        conf["spark.kubernetes.executor.podTemplateFile"] = str(template)
        conf["spark.kubernetes.executor.podTemplateContainerName"] = (
            "spark-kubernetes"
        )
    spec = _build_spec(
        app_name=app_name,
        queue=queue,
        virtualenv=virtualenv,
        driver_cores=driver_cores,
        driver_memory=driver_memory,
        driver_memory_overhead=driver_memory_overhead,
        executor_instances=executor_instances,
        executor_cores=executor_cores,
        executor_memory=executor_memory,
        executor_memory_overhead=executor_memory_overhead,
        parallelism=parallelism,
        extra_conf=conf,
    )

    # pyflamingo 1.x owns the in-cluster client-mode session creation.
    try:
        from flamingo import SparkApp
    except ImportError:
        SparkApp = None
    if SparkApp is not None and hasattr(SparkApp, "create"):
        return SparkApp.create(spec, mode="in_cluster_client")
    return _create_legacy_client_session(spec)


def run_normalization(
    normalizer_args: Sequence[str],
    **session_kwargs: Any,
) -> dict[str, Any]:
    """Run the existing normalizer using a Flamingo-created Spark session."""

    spark = create_flamingo_session(**session_kwargs)
    try:
        return tokenplan_normalize.main(list(normalizer_args), spark=spark)
    finally:
        spark.stop()


def _cli(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--app-name", default="trajfoundry-tokenplan-flamingo")
    parser.add_argument("--queue", required=True)
    parser.add_argument("--virtualenv", required=True)
    parser.add_argument("--pod-template-file")
    parser.add_argument(
        "normalizer_args",
        nargs=argparse.REMAINDER,
        help="arguments for tokenplan_normalize.py after '--'",
    )
    args = parser.parse_args(argv)
    normalizer_args = list(args.normalizer_args)
    if normalizer_args[:1] == ["--"]:
        normalizer_args = normalizer_args[1:]
    if not normalizer_args:
        parser.error("normalizer arguments are required after '--'")
    run_normalization(
        normalizer_args,
        app_name=args.app_name,
        queue=args.queue,
        virtualenv=args.virtualenv,
        pod_template_file=args.pod_template_file,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())

