#!/usr/bin/env bash
# Upload this standalone file as a DolphinScheduler resource and run it in a
# SHELL node first. Read-only: no Spark submission, S3 access, or secret contents.
set -uo pipefail

tf_project=${TF_SPARK_PROJECT:-/share/gezhilong/trajfoundry-spark}
tf_runtime=${TF_SPARK_RUNTIME:-/share/gezhilong/trajfoundry-spark-runtime}
tf_runs=${TF_SPARK_RUNS:-/share/gezhilong/trajfoundry-spark-runs}
tf_config=${TF_SPARK_CONFIG:-$tf_runtime/production/tokenplan.json}
tf_submit=${TF_SPARK_SUBMIT:-/opt/spark/bin/spark-submit}
tf_expected_uid=${TF_SPARK_POD_UID:-607031114}
tf_pod_template=${TF_SPARK_POD_TEMPLATE:-/share/gezhilong/pod-template-arm.yaml}
tf_failures=0

tf_check() {
    local name=$1 mode=$2 path=$3
    if test "$mode" "$path"; then
        printf 'TF_WORKER_CHECK name=%s passed=true path=%s\n' "$name" "$path"
    else
        printf 'TF_WORKER_CHECK name=%s passed=false path=%s\n' "$name" "$path"
        tf_failures=$((tf_failures + 1))
    fi
}

printf 'TF_WORKER_ID uid=%s gid=%s arch=%s expected_pod_uid=%s\n' \
    "$(id -u)" "$(id -g)" "$(uname -m)" "$tf_expected_uid"
if test "$(id -u)" != "$tf_expected_uid"; then
    printf '%s\n' 'TF_WORKER_CHECK name=submitter_uid_matches_pod_uid passed=false'
    tf_failures=$((tf_failures + 1))
else
    printf '%s\n' 'TF_WORKER_CHECK name=submitter_uid_matches_pod_uid passed=true'
fi

tf_check project -r "$tf_project/AGENTS.md"
tf_check date_runner -r "$tf_project/spark/run_scheduled_tokenplan.py"
tf_check submit_python -x "$tf_project/.venv/bin/python"
tf_check spark_submit -x "$tf_submit"
tf_check runs_parent -w "$tf_runs"
tf_check config -r "$tf_config"
tf_check pod_template -r "$tf_pod_template"
tf_check arm_python_visible -x "$tf_runtime/python/cpython-3.11.14-linux-aarch64-gnu/bin/python3.11"
if "$tf_project/.venv/bin/python" -c 'import yaml' >/dev/null 2>&1; then
    printf '%s\n' 'TF_WORKER_CHECK name=pyyaml_import passed=true'
else
    printf '%s\n' 'TF_WORKER_CHECK name=pyyaml_import passed=false'
    tf_failures=$((tf_failures + 1))
fi
tf_check credential_file_readable -r "$tf_runtime/production/s3-credentials.json"
tf_check kubernetes_ca -r /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
tf_check kubernetes_token_readable -r /var/run/secrets/kubernetes.io/serviceaccount/token
if test -n "${JAVA_HOME:-}"; then
    tf_check submit_java -x "$JAVA_HOME/bin/java"
elif command -v java >/dev/null 2>&1; then
    printf 'TF_WORKER_CHECK name=submit_java passed=true path=%s\n' "$(command -v java)"
else
    printf '%s\n' 'TF_WORKER_CHECK name=submit_java passed=false'
    tf_failures=$((tf_failures + 1))
fi

# Discovery hints only. Do not dump Spark defaults, kubeconfig, or credentials.
for tf_candidate in /share/test6/spark-3.5.7-bin-hadoop3/bin/spark-submit /opt/soft/spark/bin/spark-submit; do
    if test -x "$tf_candidate"; then
        printf 'TF_WORKER_HINT spark_submit_candidate=%s\n' "$tf_candidate"
    fi
done
if test -r /share/test6/.kube/config; then
    printf '%s\n' 'TF_WORKER_HINT platform_kubeconfig_readable=true (contents_not_read)'
fi

if test "$tf_failures" -eq 0; then
    # This command only validates a plan. It does not read S3 or submit a job,
    # and 915 is used solely to verify deterministic date expansion.
    env -u PYSPARK_DRIVER_PYTHON -u PYSPARK_PYTHON \
        "$tf_project/.venv/bin/python" -B \
        "$tf_project/spark/run_scheduled_tokenplan.py" \
        --config "$tf_config" --biz-date 2026-09-15 || tf_failures=$((tf_failures + 1))
fi
printf 'TF_WORKER_PREFLIGHT failures=%s submitted=false s3_accessed=false\n' "$tf_failures"
test "$tf_failures" -eq 0
