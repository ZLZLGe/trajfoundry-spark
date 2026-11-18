#!/usr/bin/env bash
# Scheduler Driver evidence stays on Pod stdout; no Worker-private file writes.
set -euo pipefail
unset HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy
[[ "${TF_CLUSTER_PYTHON:-}" == /* ]] || exit 64
[[ "${TF_CLUSTER_SOURCE_DIR:-}" == /* ]] || exit 64
[[ "${1:-}" == driver && -n "${SPARK_DRIVER_BIND_ADDRESS:-}" ]] || exit 64
"$TF_CLUSTER_PYTHON" -c '
import ipaddress, json, os, platform, socket, sys
from pathlib import Path
try:
    import trajfoundry
    root = Path(os.environ["TF_CLUSTER_SOURCE_DIR"]).resolve(strict=True)
    origin = Path(trajfoundry.__file__).resolve(strict=True)
    origin.relative_to(root)
    report = {
        "hostname": socket.gethostname(), "uid": os.geteuid(),
        "architecture": platform.machine(), "python_major_minor": list(sys.version_info[:2]),
        "python_executable": sys.executable, "project_file": str(origin),
        "driver_ip": str(ipaddress.ip_address(os.environ["SPARK_DRIVER_BIND_ADDRESS"])),
    }
    report["passed"] = bool(
        report["architecture"].lower() in {"arm64", "aarch64"}
        and tuple(report["python_major_minor"]) >= (3, 11)
        and report["uid"] == int(os.environ["TF_CLUSTER_EXPECTED_UID"])
        and report["hostname"] == os.environ["TF_CLUSTER_DRIVER_POD"]
    )
except Exception as error:
    report = {"passed": False, "error_code": "driver_runtime_invalid", "error_type": type(error).__name__}
print("TF_CLUSTER_DRIVER_RUNTIME " + json.dumps(report, sort_keys=True), flush=True)
raise SystemExit(0 if report["passed"] else 2)
'
# Use the current Driver Pod IP for Executor RPC and block fetches. The
# scheduler can submit before the Pod IP exists, so set it inside this Pod.
shift
exec /opt/entrypoint.sh driver --conf "spark.driver.host=$SPARK_DRIVER_BIND_ADDRESS" "$@"
