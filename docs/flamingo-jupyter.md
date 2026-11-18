# Flamingo Jupyter client-mode test

The current cluster submitter is intentionally left unchanged while the
Flamingo path is being verified.  In a Jupyter Pod, run the normalizer from
the shared checkout with a Spark session created by Flamingo:

```python
import sys
import os

PROJECT = "/share/gezhilong/trajfoundry-spark"
os.environ.setdefault(
    "FLAMINGO_CONF_DIR",
    "/share/gezhilong/trajfoundry-spark-runtime/production/flamingo",
)
sys.path.insert(0, f"{PROJECT}/src")
sys.path.insert(0, f"{PROJECT}/spark")

from flamingo_tokenplan import create_flamingo_session
from tokenplan_normalize import main as normalize

biz_date = "2026-09-16"
credentials = "/share/gezhilong/trajfoundry-spark-runtime/production/s3-credentials.json"
flamingo_queue = os.environ.get(
    "TF_FLAMINGO_QUEUE", "root.datacenter.pjcc.agent_trajectory"
)
executor_virtualenv = os.environ.get(
    "TF_SPARK_EXECUTOR_VENV",
    "/share/gezhilong/trajfoundry-spark-runtime/python/cpython-3.11.14-linux-aarch64-gnu",
)
executor_pod_template = os.environ.get(
    "TF_SPARK_POD_TEMPLATE",
    "/share/gezhilong/trajfoundry-spark-runtime/pod-template-arm.yaml",
)

normalizer_args = [
    "--input-uri",
    f"s3://agent-trajectory/lakehouse/token-plan/masked-raw/v002/dt={biz_date}/",
    "--output-uri",
    f"s3://agent-trajectory/lakehouse/token-plan/normalized/v006/test-flamingo-916/dt={biz_date}/",
    "--endpoint-url",
    "http://d-ceph-ssd-inside.pjlab.org.cn",
    "--region-name",
    "us-east-1",
    "--auto-prefix-shards",
    "--max-prefix-shards",
    "1000000",
    "--input-credentials-file",
    credentials,
    "--output-credentials-file",
    credentials,
    "--retain-s3-auxiliary",
    "--max-routing-mib",
    "4096",
    "--max-routing-snapshots",
    "2000000",
    "--max-root-mib",
    "2048",
    "--shuffle-partitions",
    "512",
    "--read-parallelism",
    "256",
]

spark = create_flamingo_session(
    app_name="trajfoundry-tokenplan-flamingo-916",
    # This must be a queue assigned to your own Flamingo account.  It is not
    # inferred from, or copied from, another user's Spark job.
    queue=flamingo_queue,
    # This must be your own Python 3.11 ARM executor environment.  The
    # Jupyter/Driver environment is selected by the active kernel.
    virtualenv=executor_virtualenv,
    pod_template_file=executor_pod_template,
)
try:
    result = normalize(normalizer_args, spark=spark)
    print(result)
finally:
    spark.stop()
```

The default queue is our own `root.datacenter.pjcc.agent_trajectory` queue.
`TF_FLAMINGO_QUEUE` may override it when the platform gives the project a
different queue; it is not an arbitrary label.  `TF_SPARK_EXECUTOR_VENV` must be
your own ARM Python 3.11 environment, visible in Executor Pods.  The active
Jupyter kernel must be your own Driver environment with `pyflamingo`, PySpark
3.5.7, and the TrajFoundry dependencies installed.  Do not point either
variable at another user's virtual environment.  The credential file is
passed as a path and is never put into Spark configuration or printed.

For the current checkout, the own-template values are normally:

```bash
export TF_SPARK_EXECUTOR_VENV=/share/gezhilong/trajfoundry-spark-runtime/python/cpython-3.11.14-linux-aarch64-gnu
export TF_SPARK_POD_TEMPLATE=/share/gezhilong/trajfoundry-spark-runtime/pod-template-arm.yaml
export FLAMINGO_CONF_DIR=/share/gezhilong/trajfoundry-spark-runtime/production/flamingo
```

Use these only if those paths are your deployment's own files; the template
does not create the Driver/Jupyter environment and does not register a queue.

This path uses `spark.submit.deployMode=client`: the Driver is the Jupyter
process and Executors are launched by Spark on Kubernetes.  The SDK supplies
the platform queue, image, service account, mounts, event-log settings and
Flamingo listener configuration.  The business source and credential file
are read from `/share`; no Python file or YAML needs to be uploaded to the
DolphinScheduler resource center for this Jupyter test.

Before running the full date, verify that the session is using the expected
platform listener and shared source path:

```python
print(spark.sparkContext.getConf().get("spark.submit.deployMode"))
print(spark.sparkContext.getConf().get("spark.eventLog.enabled"))
print(spark.sparkContext.getConf().get("spark.eventLog.dir"))
print(spark.sparkContext.getConf().get("spark.extraListeners", "<from Spark defaults>"))
print(spark.sparkContext.getConf().get("spark.executorEnv.PYTHONPATH"))
```

Do not print the complete Spark configuration: the platform may inject
authentication material into Spark defaults.

