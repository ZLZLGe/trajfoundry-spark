#!/usr/bin/env bash
# Run as a script, not with `source`; no parent-shell environment changes.
set -euo pipefail

usage() {
    printf '%s\n' \
        'Usage: submit_client_probe.sh [options]' \
        'Required flags (or matching TF_SPARK_* environment variables):' \
        '  --driver-python PATH          TF_SPARK_DRIVER_PYTHON (x86 Python >=3.11)' \
        '  --executor-python PATH        TF_SPARK_EXECUTOR_PYTHON (ARM Python >=3.11)' \
        '  --executor-pod-template PATH  TF_SPARK_EXECUTOR_POD_TEMPLATE' \
        '  --executor-queue QUEUE        TF_SPARK_EXECUTOR_QUEUE' \
        '  --master k8s://https://...     TF_SPARK_MASTER' \
        '  --namespace NAMESPACE         TF_SPARK_NAMESPACE' \
        '  --driver-host ADDRESS         TF_SPARK_DRIVER_HOST (Executor-reachable)' \
        '  --ca-cert PATH                TF_SPARK_CA_CERT (public API CA)' \
        'Optional: --spark-submit PATH, --project-root PATH, --executor-image IMAGE,' \
        '          --oauth-token-file PATH, --executor-service-account NAME,' \
        '          --driver-pod-name NAME, --driver-port PORT, --block-manager-port PORT, --help' \
        'Defaults: 2 Executors, 1 core each, 2g heap + 2g overhead; synthetic data only.'
}

fail() { printf 'client probe: %s\n' "$1" >&2; exit 2; }

probe_script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
probe_project_root="${TF_SPARK_PROJECT_ROOT:-$(cd -- "$probe_script_dir/.." && pwd)}"
probe_submit="${TF_SPARK_SUBMIT:-${SPARK_HOME:-/opt/spark}/bin/spark-submit}"
probe_driver_python="${TF_SPARK_DRIVER_PYTHON:-}"
probe_executor_python="${TF_SPARK_EXECUTOR_PYTHON:-}"
probe_template="${TF_SPARK_EXECUTOR_POD_TEMPLATE:-}"
probe_queue="${TF_SPARK_EXECUTOR_QUEUE:-}"
probe_master="${TF_SPARK_MASTER:-}"
probe_namespace="${TF_SPARK_NAMESPACE:-}"
probe_driver_host="${TF_SPARK_DRIVER_HOST:-}"
probe_ca="${TF_SPARK_CA_CERT:-}"
probe_image="${TF_SPARK_EXECUTOR_IMAGE:-}"
probe_token_file="${TF_SPARK_OAUTH_TOKEN_FILE:-}"
probe_service_account="${TF_SPARK_EXECUTOR_SERVICE_ACCOUNT:-}"
probe_driver_pod="${TF_SPARK_DRIVER_POD_NAME:-}"
probe_driver_port="${TF_SPARK_DRIVER_PORT:-}"
probe_block_manager_port="${TF_SPARK_BLOCK_MANAGER_PORT:-}"
probe_require_pyarrow=false

while (($#)); do
    case "$1" in
        --help|-h) usage; exit 0 ;;
        --require-pyarrow) probe_require_pyarrow=true; shift ;;
        --driver-python|--executor-python|--executor-pod-template|--executor-queue|--master|--namespace|--driver-host|--ca-cert|--spark-submit|--project-root|--executor-image|--oauth-token-file|--executor-service-account|--driver-pod-name|--driver-port|--block-manager-port)
            (($# >= 2)) || fail "missing value for $1"
            [[ -n "$2" ]] || fail "empty value for $1"
            case "$1" in
                --driver-python) probe_driver_python="$2" ;;
                --executor-python) probe_executor_python="$2" ;;
                --executor-pod-template) probe_template="$2" ;;
                --executor-queue) probe_queue="$2" ;;
                --master) probe_master="$2" ;;
                --namespace) probe_namespace="$2" ;;
                --driver-host) probe_driver_host="$2" ;;
                --ca-cert) probe_ca="$2" ;;
                --spark-submit) probe_submit="$2" ;;
                --project-root) probe_project_root="$2" ;;
                --executor-image) probe_image="$2" ;;
                --oauth-token-file) probe_token_file="$2" ;;
                --executor-service-account) probe_service_account="$2" ;;
                --driver-pod-name) probe_driver_pod="$2" ;;
                --driver-port) probe_driver_port="$2" ;;
                --block-manager-port) probe_block_manager_port="$2" ;;
            esac
            shift 2
            ;;
        *) fail "unknown option: $1" ;;
    esac
done

[[ "$probe_master" == k8s://https://* ]] || fail '--master must be k8s://https://...'
[[ -n "$probe_namespace" ]] || fail '--namespace is required'
[[ -n "$probe_queue" ]] || fail '--executor-queue is required'
[[ -n "$probe_driver_host" ]] || fail '--driver-host is required'
[[ "$probe_driver_python" == /* && -x "$probe_driver_python" ]] || fail '--driver-python must be an executable absolute path'
[[ "$probe_executor_python" == /* ]] || fail '--executor-python must be an absolute path in Executor pods'
[[ -r "$probe_template" ]] || fail '--executor-pod-template must be readable on the submit host'
[[ -r "$probe_ca" ]] || fail '--ca-cert must be a readable public CA file'
[[ -z "$probe_token_file" || -r "$probe_token_file" ]] || fail '--oauth-token-file must be readable; its contents are not inspected'
[[ -x "$probe_submit" ]] || fail 'spark-submit must be executable'
[[ "$probe_project_root" == /* ]] || fail '--project-root must be an absolute shared path'
[[ -d "$probe_project_root/src/trajfoundry/spark" ]] || fail '--project-root must be the Spark checkout'
[[ -r "$probe_project_root/spark/client_probe.py" ]] || fail 'client_probe.py not found'
for probe_port in "$probe_driver_port" "$probe_block_manager_port"; do
    if [[ -n "$probe_port" ]]; then
        [[ "$probe_port" =~ ^[0-9]{1,5}$ ]] || fail 'ports must be integers between 1 and 65535'
        ((10#$probe_port >= 1 && 10#$probe_port <= 65535)) || fail 'ports must be integers between 1 and 65535'
    fi
done

probe_command=(
    "$probe_submit"
    --master "$probe_master"
    --deploy-mode client
    --name trajfoundry-client-probe
    --driver-memory 2g
    --conf "spark.kubernetes.namespace=$probe_namespace"
    --conf "spark.kubernetes.authenticate.caCertFile=$probe_ca"
    --conf spark.kubernetes.trust.certificates=false
    --conf "spark.kubernetes.executor.podTemplateFile=$probe_template"
    --conf "spark.kubernetes.executor.label.queue=$probe_queue"
    --conf "spark.pyspark.driver.python=$probe_driver_python"
    --conf "spark.pyspark.python=$probe_executor_python"
    --conf "spark.driver.host=$probe_driver_host"
    --conf spark.driver.bindAddress=0.0.0.0
    --conf spark.driver.maxResultSize=16m
    --conf spark.executor.instances=2
    --conf spark.executor.cores=1
    --conf spark.kubernetes.executor.limit.cores=1
    --conf spark.executor.memory=2g
    --conf spark.executor.memoryOverhead=2g
    --conf spark.dynamicAllocation.enabled=false
    --conf spark.speculation=false
    --conf spark.default.parallelism=2
    --conf spark.task.cpus=1
    --conf spark.task.maxFailures=1
    --conf spark.stage.maxConsecutiveAttempts=1
    --conf spark.barrier.sync.timeout=120s
    --conf spark.scheduler.barrier.maxConcurrentTasksCheck.interval=15s
    --conf spark.scheduler.barrier.maxConcurrentTasksCheck.maxFailures=8
)
[[ -z "$probe_image" ]] || probe_command+=(--conf "spark.kubernetes.executor.container.image=$probe_image")
[[ -z "$probe_token_file" ]] || probe_command+=(--conf "spark.kubernetes.authenticate.oauthTokenFile=$probe_token_file")
[[ -z "$probe_service_account" ]] || probe_command+=(--conf "spark.kubernetes.authenticate.executor.serviceAccountName=$probe_service_account")
[[ -z "$probe_driver_pod" ]] || probe_command+=(--conf "spark.kubernetes.driver.pod.name=$probe_driver_pod")
[[ -z "$probe_driver_port" ]] || probe_command+=(--conf "spark.driver.port=$probe_driver_port")
[[ -z "$probe_block_manager_port" ]] || probe_command+=(--conf "spark.blockManager.port=$probe_block_manager_port")
probe_command+=(
    "$probe_project_root/spark/client_probe.py"
    --source-dir "$probe_project_root/src"
    --driver-python "$probe_driver_python"
    --executor-python "$probe_executor_python"
)

[[ "$probe_require_pyarrow" == false ]] || probe_command+=(--require-pyarrow)

# Spark/Fabric8 inherits platform authentication. Never inspect tokens or dump
# the complete configuration. These unsets affect only this child process.
exec env -u PYSPARK_DRIVER_PYTHON -u PYSPARK_PYTHON "${probe_command[@]}"
