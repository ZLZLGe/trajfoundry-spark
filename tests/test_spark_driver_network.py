"""Exercise the actual Driver wrappers at the image-entrypoint boundary."""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def run_wrapper(tmp_path, wrapper_name, pod_ip, *, role="driver"):
    image_entrypoint = tmp_path / "image-entrypoint.py"
    image_entrypoint.write_text(
        f"#!{sys.executable}\n"
        "import json, sys\n"
        "print(json.dumps(sys.argv[1:]))\n"
        "raise SystemExit(37)\n"
    )
    image_entrypoint.chmod(0o700)
    runtime_probe = tmp_path / "runtime-probe.sh"
    runtime_probe.write_text("#!/bin/bash\nexit 0\n")
    runtime_probe.chmod(0o700)
    source = (ROOT / "spark" / wrapper_name).read_text()
    assert source.count("exec /opt/entrypoint.sh") == 1
    wrapper = tmp_path / wrapper_name
    wrapper.write_text(
        source.replace(
            "exec /opt/entrypoint.sh", "exec " + shlex.quote(str(image_entrypoint))
        )
    )
    log = tmp_path / "driver.log"
    env = {
        **os.environ,
        "TF_CLUSTER_LOG_PATH": str(log),
        "TF_CLUSTER_PYTHON": str(runtime_probe),
        "TF_CLUSTER_SOURCE_DIR": str(ROOT / "src"),
        "SPARK_DRIVER_BIND_ADDRESS": pod_ip,
    }
    result = subprocess.run(
        [
            "bash", str(wrapper), role,
            "--properties-file", "/opt/spark/conf/spark.properties",
            "local:///share/job.py", "--value", "中文 and spaces",
        ],
        env=env, capture_output=True, text=True, check=False,
    )
    return result, log.read_text() if log.exists() else result.stdout


@pytest.mark.parametrize(
    "wrapper_name", ["cluster_driver_entrypoint.sh", "scheduler_driver_entrypoint.sh"]
)
@pytest.mark.parametrize("pod_ip", ["10.112.1.23", "fd00::123"])
def test_driver_advertises_current_pod_ip_and_preserves_arguments(
    tmp_path, wrapper_name, pod_ip
):
    result, output = run_wrapper(tmp_path, wrapper_name, pod_ip)
    assert result.returncode == 37
    assert json.loads(output) == [
        "driver", "--conf", f"spark.driver.host={pod_ip}",
        "--properties-file", "/opt/spark/conf/spark.properties",
        "local:///share/job.py", "--value", "中文 and spaces",
    ]


@pytest.mark.parametrize(
    "wrapper_name", ["cluster_driver_entrypoint.sh", "scheduler_driver_entrypoint.sh"]
)
@pytest.mark.parametrize("pod_ip,role", [("", "driver"), ("10.112.1.23", "executor")])
def test_driver_wrapper_rejects_missing_pod_ip_or_wrong_role(
    tmp_path, wrapper_name, pod_ip, role
):
    result, output = run_wrapper(tmp_path, wrapper_name, pod_ip, role=role)
    assert result.returncode == 64
    assert not output
