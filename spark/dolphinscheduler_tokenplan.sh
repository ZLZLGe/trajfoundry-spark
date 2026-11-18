#!/usr/bin/env bash
# Production: paste this one script into a DolphinScheduler SHELL node.
# All code, configuration, credentials (read only inside the Pods), and the
# base Pod template are taken from the shared /share directory. No
# DolphinScheduler resource-center attachment is required.
# biz_date is expanded in THIS NODE BODY.
set -euo pipefail
umask 077

tf_project=/share/gezhilong/trajfoundry-spark
tf_config=${TF_SPARK_CONFIG:-/share/gezhilong/trajfoundry-spark-runtime/production/tokenplan.json}
tf_python="$tf_project/.venv/bin/python"
tf_submit=${TF_SPARK_SUBMIT:-}
tf_biz_date='${biz_date}'

unset PYSPARK_DRIVER_PYTHON PYSPARK_PYTHON
export PYTHONUNBUFFERED=1

if [[ -z "$tf_submit" ]]; then
    for tf_candidate in \
        "${SPARK_HOME:-}/bin/spark-submit" \
        /opt/soft/spark/bin/spark-submit \
        /opt/spark/bin/spark-submit
    do
        if [[ -n "$tf_candidate" ]] && test -x "$tf_candidate"; then
            tf_submit="$tf_candidate"
            break
        fi
    done
fi

test -x "$tf_python"
test -x "$tf_submit"
test -r "$tf_config"
test -r "$tf_project/spark/submit_scheduler.py"
test -r "$tf_project/spark/run_scheduled_tokenplan.py"
test -r "$tf_project/spark/submit_cluster.py"
test -r "$tf_project/spark/tokenplan_normalize.py"

exec "$tf_python" -B "$tf_project/spark/submit_scheduler.py" \
    --config "$tf_config" \
    --spark-submit "$tf_submit" \
    --biz-date "$tf_biz_date" \
    --execute
