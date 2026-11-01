from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import ClassVar

import pytest

ROOT = Path(__file__).resolve().parents[1]
TEST_QUEUE = "root.test.trajfoundry"
TEST_VENV = "/share/gezhilong/trajfoundry-test-venv"


def load_script(name):
    spec = importlib.util.spec_from_file_location(
        name, ROOT / "spark" / f"{name}.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


flamingo_launcher = load_script("flamingo_tokenplan")


class OldSpec:
    model_fields: ClassVar[dict[str, object]] = {
        "app_name": None,
        "queue": None,
        "driver_cores": None,
        "driver_memory": None,
        "driver_memory_overhead": None,
        "executor_instances": None,
        "executor_cores": None,
        "executor_memory": None,
        "executor_memory_overhead": None,
        "deploy_mode": None,
        "venv_path": None,
        "parallelism": None,
        "extra_conf": None,
    }

    def __init__(self, **values):
        self.__dict__.update(values)


class NewSpec(OldSpec):
    model_fields: ClassVar[dict[str, object]] = {
        key: value for key, value in OldSpec.model_fields.items() if key != "venv_path"
    }
    model_fields["virtualenv"] = None


def _spec_module(spec_type):
    return SimpleNamespace(SparkAppSpec=spec_type)


def test_build_spec_supports_old_sdk(monkeypatch):
    monkeypatch.setitem(sys.modules, "flamingo", _spec_module(OldSpec))
    spec = flamingo_launcher._build_spec(
        app_name="app",
        queue=TEST_QUEUE,
        virtualenv=TEST_VENV,
        driver_cores=4,
        driver_memory="8g",
        driver_memory_overhead="4g",
        executor_instances=2,
        executor_cores=8,
        executor_memory="16g",
        executor_memory_overhead="48g",
        parallelism=256,
        extra_conf={"spark.eventLog.enabled": "true"},
    )
    assert spec.venv_path == TEST_VENV
    assert spec.deploy_mode == "client"


def test_build_spec_supports_new_sdk(monkeypatch):
    monkeypatch.setitem(sys.modules, "flamingo", _spec_module(NewSpec))
    spec = flamingo_launcher._build_spec(
        app_name="app",
        queue=TEST_QUEUE,
        virtualenv=TEST_VENV,
        driver_cores=4,
        driver_memory="8g",
        driver_memory_overhead="4g",
        executor_instances=2,
        executor_cores=8,
        executor_memory="16g",
        executor_memory_overhead="48g",
        parallelism=256,
        extra_conf={},
    )
    assert spec.virtualenv == Path(TEST_VENV).name


def test_create_session_uses_new_sdk_and_keeps_credentials_out(monkeypatch):
    seen = {}

    class FakeSparkApp:
        @staticmethod
        def create(spec, **kwargs):
            seen["spec"] = spec
            seen["kwargs"] = kwargs
            return "spark-session"

    class FakeSpec(NewSpec):
        pass

    monkeypatch.setitem(
        sys.modules,
        "flamingo",
        SimpleNamespace(SparkAppSpec=FakeSpec, SparkApp=FakeSparkApp),
    )
    assert (
        flamingo_launcher.create_flamingo_session(
            app_name="app", queue="queue", virtualenv="env"
        )
        == "spark-session"
    )
    assert seen["kwargs"] == {"mode": "in_cluster_client"}
    assert "spark.hadoop.fs.s3a.secret.key" not in seen["spec"].extra_conf
    assert seen["spec"].extra_conf["spark.extraListeners"].endswith(
        "SparkFlamingoListener"
    )
    assert seen["spec"].extra_conf["spark.driver.maxResultSize"] == "1g"


def test_create_session_rejects_s3_secret_spark_config(monkeypatch):
    monkeypatch.setitem(sys.modules, "flamingo", _spec_module(NewSpec))
    with pytest.raises(ValueError, match="credential-file"):
        flamingo_launcher.create_flamingo_session(
            app_name="app",
            queue="queue",
            virtualenv="env",
            extra_conf={"spark.hadoop.fs.s3a.secret.key": "do-not-use"},
        )


def test_create_session_accepts_own_executor_template(monkeypatch, tmp_path):
    seen = {}

    class FakeSparkApp:
        @staticmethod
        def create(spec, **kwargs):
            seen["spec"] = spec
            seen["kwargs"] = kwargs
            return "spark-session"

    template = tmp_path / "pod-template-arm.yaml"
    template.write_text("apiVersion: v1\nkind: Pod\n", encoding="utf-8")
    monkeypatch.setitem(
        sys.modules,
        "flamingo",
        SimpleNamespace(SparkAppSpec=NewSpec, SparkApp=FakeSparkApp),
    )
    assert (
        flamingo_launcher.create_flamingo_session(
            app_name="app",
            queue=TEST_QUEUE,
            virtualenv=TEST_VENV,
            pod_template_file=str(template),
        )
        == "spark-session"
    )
    assert seen["spec"].extra_conf["spark.kubernetes.executor.podTemplateFile"] == str(
        template
    )

