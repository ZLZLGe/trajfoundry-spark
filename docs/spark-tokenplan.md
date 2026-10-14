# TokenPlan Spark 试运行说明

这个仓库的 Spark 入口只服务 TokenPlan，普通 Python 仓库、环境和现有调度节点不受影响。
遵循平台推荐：**Jupyter 上用 Client 开发，验证后再用 DolphinScheduler SHELL +
Cluster 模式上生产。** Client 调试不需要在调度平台手动建任务；Driver 在当前
Jupyter，Executor 在 Kubernetes，并不是 `local[*]` 单机运行。

## 先运行纯合成探针

Driver 与 Executor 的 Python major/minor 必须一致且至少为 3.11。x86 Driver 与
ARM Executor 使用独立解释器和对应的原生依赖，不能把 x86 venv 复制给 ARM。
Executor 模板中的 PATH、PYSPARK_PYTHON 要指向 ARM 环境，同时显式设置
`spark.pyspark.driver.python` 和 `spark.pyspark.python`，以免 Driver 下发错误的
worker 命令。修改专用模板副本，不影响共享默认环境。

探针固定两个 Executor，每个 1 核、2 GiB JVM + 2 GiB overhead，验证不同
Executor Pod、回连 Driver、架构、Python 路径/版本、依赖及 Spark checkout 导入
来源。不调用业务入口，不读取或写入业务 S3。

```bash
bash spark/submit_client_probe.sh \
  --spark-submit "$SPARK_HOME/bin/spark-submit" \
  --driver-python "$DRIVER_PYTHON" \
  --executor-python "$ARM_EXECUTOR_PYTHON" \
  --executor-pod-template "$ARM_POD_TEMPLATE" \
  --executor-queue "$SPARK_QUEUE" \
  --executor-image "$SPARK_IMAGE" \
  --master "$K8S_MASTER" \
  --namespace "$K8S_NAMESPACE" \
  --driver-host "$DRIVER_POD_IP" \
  --ca-cert "$K8S_CA_FILE" \
  --oauth-token-file "$K8S_TOKEN_FILE" \
  --executor-service-account "$SPARK_SERVICE_ACCOUNT"
```

可选 `--driver-pod-name`、`--driver-port` 和 `--block-manager-port`，端口应空闲且
可被 Executor 访问。最终 `TF_SPARK_CLIENT_PROBE` 结果中的 `"passed": true`
才代表成功，不能只看 Driver 已启动。脚本保留 TLS 校验，不输出凭据或完整
SparkConf。内部 API 被代理错误转发时，只调整提交子进程的代理，不要禁用 TLS。
平台挂载的 CA 可能是 intermediate CA，Python 和 Spark/Java 的信任链处理不同，
Python 证书验证失败本身不能证明 Spark 不可用。

依赖以 uv.lock 为准；可通过 [uv Python 版本选择](https://docs.astral.sh/uv/concepts/python-versions/)
准备独立 ARM Python，再使用 ARM 目标 wheels 并验证锁文件哈希。最终必须在真实
ARM Executor 上验证原生库加载。由平台 Spark 提供匹配的 PySpark/py4j，不要用
另一个 pip PySpark 覆盖平台版本。

## 小规模业务试跑及生产迁移

代码尊重 spark-submit 指定的模式与资源，不会把 Client 改成 Cluster，或把两个
小 Executor 覆盖成 24 个大 Executor。先选择已知小分片和全新、独占的测试输出
目录或前缀。小分片可以直接写共享文件系统，不需要写入 S3。

### 只读 S3 输入、共享目录输出

Spark 业务入口 `spark/tokenplan_normalize.py --output-uri` 支持绝对目录路径和
`file:///absolute/path`，例如 `/share/<user>/trajfoundry-spark-runs/<unique-run>/`。
不接受相对路径、文件系统根目录、带远端主机的 file URI、路径穿越或符号链接。
这里的“本地”必须是 Driver 与所有 Executor 都能访问的同一共享文件系统；
不同 Pod 的 `/tmp` 互不相通，只能用于各自的临时计算文件，不能作为共享输出。

每次使用全新或已有但为空的独占目录，不能复用上次失败的非空目录。Driver
先创建运行标记，Executor 只核对标记，不会自己创建一个同名私有输出目录。
写入先进入同目录临时文件，再用硬链接原子发布，绝不覆盖已有的不同内容；
Spark 重试仅接受相同字节。Driver 流式核对返回文件的 SHA-256 和大小，合并
lineage/诊断并清理本次分片临时文件，最后发布 `manifest.json`；之后只移除自身
运行标记，不再改动已发布的数据文件。
发布前失败会保留标记和未发布文件用于排查，没有成功清单不能当作完成；最终
仍应检查任务成功状态及输出验证结果，而不是仅看目录里出现了文件。
若 manifest 提交后运行标记清理失败，任务可能报错但 manifest 已经存在；先检查
现有产物并执行 `validate_output`，不要覆盖重跑或自动清空目录。

共享存储须支持同目录硬链接和 `fsync`。提交前在真实 Executor 验证读写权限与
Driver 回读；同组不等于有写权限，且文件/目录权限还会受到 umask 影响。建议
Driver、Executor 使用一致的工作身份，不要通过放宽已有生产目录权限解决问题。

真实业务验收脚本为 `spark/local_output_smoke.py`。它要求 Kubernetes 模式
（默认 Client，Cluster 提交传入 `--expected-deploy-mode cluster`）、两个独立
Executor，并检查 Python 版本、代码导入路径、运行 uid、输出
完整校验和 manifest 计数；显式禁止创建 S3 **输出**客户端。输入仍由 Executor
通过 S3 只读。此脚本使用 `--output-dir`，仅接受绝对路径；file URI 支持位于上述
通用业务入口的 `--output-uri`。

本轮指定读取 `dt=2026-09-01`。先只读盘点该日期下候选用户的对象数量和体积，
选择一个或少量完整 `<user_id>/` 分片。不要用前 N 个对象、`head` 或随机对象
截断用户分组，否则前缀聚合和 lineage 对照不完整。下面的变量应来自已经通过
合成探针的环境，模板也必须具备一致的运行身份和共享挂载：

```bash
env -u PYSPARK_DRIVER_PYTHON -u PYSPARK_PYTHON \
  "$SPARK_HOME/bin/spark-submit" \
  --master "$K8S_MASTER" \
  --deploy-mode client \
  --driver-memory 2g \
  --conf "spark.kubernetes.namespace=$K8S_NAMESPACE" \
  --conf "spark.kubernetes.authenticate.caCertFile=$K8S_CA_FILE" \
  --conf "spark.kubernetes.authenticate.oauthTokenFile=$K8S_TOKEN_FILE" \
  --conf spark.kubernetes.trust.certificates=false \
  --conf "spark.kubernetes.authenticate.executor.serviceAccountName=$SPARK_SERVICE_ACCOUNT" \
  --conf "spark.kubernetes.executor.container.image=$SPARK_IMAGE" \
  --conf "spark.kubernetes.executor.podTemplateFile=$ARM_POD_TEMPLATE" \
  --conf "spark.kubernetes.executor.label.queue=$SPARK_QUEUE" \
  --conf "spark.pyspark.driver.python=$DRIVER_PYTHON" \
  --conf "spark.pyspark.python=$ARM_EXECUTOR_PYTHON" \
  --conf "spark.executorEnv.PYTHONPATH=$SPARK_SOURCE_DIR" \
  --conf "spark.driver.host=$DRIVER_POD_IP" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.executor.instances=2 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=2g \
  --conf spark.executor.memoryOverhead=2g \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.speculation=false \
  --conf spark.default.parallelism=2 \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.driver.maxResultSize=64m \
  spark/local_output_smoke.py \
  --input-uri "s3://<bucket>/<tokenplan-input-prefix>/dt=2026-09-01/" \
  --prefix-shard "$COMPLETE_SMALL_USER_PREFIX" \
  --output-dir "$FRESH_SHARED_OUTPUT_DIR" \
  --source-dir "$SPARK_SOURCE_DIR" \
  --endpoint-url "$S3_ENDPOINT_URL" \
  --region-name us-east-1 \
  --input-credentials-file "$EXECUTOR_CREDENTIAL_FILE"
```

`SPARK_SOURCE_DIR` 必须是 Spark checkout 的共享 `src` 绝对路径，不是普通
Python checkout。`--prefix-shard` 可重复传入互不重叠的完整小用户前缀。
仅当最后的 `TF_SPARK_LOCAL_OUTPUT_SMOKE` JSON 中 `passed=true`，且校验通过，
才能认为这一小分片完成真实验收；它不代表整个日期处理完成。

`--input-credentials-file` 可选，传递的仅是绝对文件路径，真正的安全加载发生在
Executor 内，不在 Driver 读取后序列化密钥。文件必须归 Executor 的 effective uid
所有，权限仅允许 `0400` 或 `0600`，每层父目录也必须可遍历，实际 Pod 中必须
存在相同挂载。显式文件不可读或不安全时直接失败，不回退到其他身份。
没有传入此选项时保留 boto3 默认凭据链；Hadoop `fs.s3a.*` 不会自动变成 boto3
认证。不要把密钥放进命令行、SparkConf、代码、日志或测试报告，也不要修改
普通 Python 调度任务的密钥或目录权限；需要运维为本次 Spark 身份提供可用的
受保护输入凭据。采用本地输出时，不需要 S3 输出写权限，也不发起 S3 输出写入。

2026-10-09 验证记录：已使用 Kubernetes Client 模式和两个 ARM Executor，
只读处理 `dt=2026-09-01` 下两个完整用户分片，共 50 个对象、13,528,285 字节，
并成功写入本地共享目录，未写入 S3 输出。结果为 10 条轨迹：accepted=6、
quarantined_trajectories=4、parse_failures=0、quarantined_records=0。
`validate_output` 返回 valid=true、error_count=0，manifest 计数及文件哈希校验
通过；隔离原因计数为 missing_tool_result=4、no_final_assistant_turn=4。
本次应用关联的 Pods、Services 和 ConfigMaps 均已释放。
这是真实 S3 小分片验收，不是仅合成探针或本地集成测试，但不代表整个日期的
生产验收。已用同一 Spark checkout 的单机 `normalize_source` 独立构建基线，
发现并修复 Spark lineage 来源遗漏 `disposition`、`reason_codes` 的问题，再用
同一批真实输入重新提交两个 ARM Executor 验收。最终 10 个轨迹文件的文件名、
SHA-256 和大小完全一致，完整 lineage 记录（含 98 条来源审计，覆盖全部 50 个
输入）及 manifest 计数也一致；两份输出均通过校验。正常 Python 部署未改动，
测试临时凭据已删除，原账号文件保留。

### 从 Jupyter 提交 Cluster 验收

可以直接从 Jupyter 执行 `spark-submit --deploy-mode cluster`，无需先到
DolphinScheduler 手动创建节点。提交端只负责创建应用；Driver 和 Executor 均在
新建的 Kubernetes Pod 中运行。小分片验收通过后，再迁移到 DolphinScheduler
SHELL 节点及评估全量资源。下面是共享环境模式的提交骨架，不代表全日期已验收。

提交前准备并核对这些条件：

- `SPARK_CHECKOUT` 和 `SPARK_SOURCE_DIR=$SPARK_CHECKOUT/src` 是 Spark checkout
  的共享绝对路径，入口、源码和 ARM Python 在真实 Driver/Executor 中均可读。
  两端使用 ARM 镜像及专用 ARM 模板，不能把 Jupyter 的 x86 Python 当作 Cluster
  Driver 解释器。两端 Python major/minor 相同，模板的 PATH、PYSPARK_PYTHON
  与显式指定的 `ARM_PYTHON` 一致。
- 两个专用模板保持已验证的 nodeSelector、tolerations、共享卷、hostAliases
  等配置，并为 Driver/Executor 配置相同 uid/gid；该身份必须有共享输出写权限及
  输入凭据文件访问权限。不要修改公共模板、其他部署的目录权限或借用其他 uid。
- `SPARK_PROPERTIES_FILE` 是事先准备的、真实存在的普通 properties 文件，
  可以为空，用于隔离未经核对的默认镜像、队列及认证配置。**不能使用 `/dev/null`：
  当前 Spark 会拒绝它不是普通文件。** 必需配置在此骨架中显式给出；若平台要求
  额外 listener/scheduler，需单独核对后补入，不要复制整份含凭据的默认配置。
- namespace、两端队列、ServiceAccount、镜像与模板容器名使用平台已授权的值。
  Driver ServiceAccount 需要本应用创建、观察和清理 Executor 等资源的权限。
  下例两个模板的业务容器都名为 `spark-kubernetes`，不同名称须同步替换。
- `FRESH_SHARED_OUTPUT_DIR` 是全新或为空的独占共享目录。日志放到另外的本次
  专属目录，不能提前写入输出目录。两个 Executor、每个一核只是小分片验收配置，
  不是百万对象全量资源建议。

```bash
env -u PYSPARK_DRIVER_PYTHON -u PYSPARK_PYTHON \
  "$SPARK_HOME/bin/spark-submit" \
  --properties-file "$SPARK_PROPERTIES_FILE" \
  --name "$SPARK_RUN_NAME" \
  --master "$K8S_MASTER" \
  --deploy-mode cluster \
  --conf "spark.kubernetes.namespace=$K8S_NAMESPACE" \
  --conf "spark.kubernetes.authenticate.submission.caCertFile=$K8S_CA_FILE" \
  --conf "spark.kubernetes.authenticate.submission.oauthTokenFile=$K8S_TOKEN_FILE" \
  --conf spark.kubernetes.trust.certificates=false \
  --conf "spark.kubernetes.authenticate.driver.serviceAccountName=$SPARK_SERVICE_ACCOUNT" \
  --conf "spark.kubernetes.authenticate.executor.serviceAccountName=$SPARK_SERVICE_ACCOUNT" \
  --conf "spark.kubernetes.driver.container.image=$SPARK_IMAGE" \
  --conf "spark.kubernetes.executor.container.image=$SPARK_IMAGE" \
  --conf "spark.kubernetes.driver.podTemplateFile=$ARM_DRIVER_POD_TEMPLATE" \
  --conf "spark.kubernetes.executor.podTemplateFile=$ARM_EXECUTOR_POD_TEMPLATE" \
  --conf spark.kubernetes.driver.podTemplateContainerName=spark-kubernetes \
  --conf spark.kubernetes.executor.podTemplateContainerName=spark-kubernetes \
  --conf "spark.kubernetes.driver.label.queue=$SPARK_QUEUE" \
  --conf "spark.kubernetes.executor.label.queue=$SPARK_QUEUE" \
  --conf "spark.pyspark.driver.python=$ARM_PYTHON" \
  --conf "spark.pyspark.python=$ARM_PYTHON" \
  --conf "spark.kubernetes.driverEnv.PYTHONPATH=$SPARK_SOURCE_DIR" \
  --conf "spark.executorEnv.PYTHONPATH=$SPARK_SOURCE_DIR" \
  --conf spark.jars.ivy=/tmp/trajfoundry-ivy \
  --conf spark.driver.cores=1 \
  --conf spark.driver.memory=2g \
  --conf spark.driver.memoryOverhead=2g \
  --conf spark.driver.maxResultSize=64m \
  --conf spark.executor.instances=2 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=2g \
  --conf spark.executor.memoryOverhead=2g \
  --conf spark.default.parallelism=2 \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.speculation=false \
  --conf spark.kubernetes.submission.waitAppCompletion=true \
  --conf spark.kubernetes.executor.deleteOnTermination=true \
  --conf spark.kubernetes.driver.service.deleteOnTermination=true \
  "local://$SPARK_CHECKOUT/spark/local_output_smoke.py" \
  --expected-deploy-mode cluster \
  --input-uri "$SMALL_INPUT_URI" \
  --prefix-shard "$COMPLETE_SMALL_USER_PREFIX" \
  --output-dir "$FRESH_SHARED_OUTPUT_DIR" \
  --source-dir "$SPARK_SOURCE_DIR" \
  --endpoint-url "$S3_ENDPOINT_URL" \
  --region-name us-east-1 \
  --input-credentials-file "$EXECUTOR_CREDENTIAL_FILE"
```

`local://` 加上绝对路径构成 `local:///share/...`，表示入口已经位于 Pod 的共享
挂载，不通过 S3 上传；不需要为这个入口配置 `--py-files` 或资源上传目录。
若改用非共享入口或增加需上传的资源，必须重新验证分发机制。不要保留 Client
示例中指向 Jupyter 的 `spark.driver.host`、固定端口或 `spark.kubernetes.driver.pod.name`。
`spark.jars.ivy` 使用 Pod 内可写的绝对 `/tmp` 路径，避免匿名 uid 的默认用户 home
导致 Ivy 报“basedir must be absolute”；它不是共享输出目录。

提交端认证使用 `submission.*` 的 CA/token 文件路径；Cluster Driver 使用自身
ServiceAccount 的挂载身份，不应把 Jupyter 的 token 复制成 Driver 凭据。始终
保留 TLS 验证。输入 boto3 的认证与 Kubernetes 认证互不替代；可选的
`--input-credentials-file` 只在 Executor 安全加载输入凭据，不提供 S3 输出认证。
Hadoop S3A 连接池/上传参数不会调节这里的 boto3 业务读写；SQL AQE 参数也不能
解决当前 RDD scope 构建的内存边界，因此不在此验收骨架中添加这些参数。

**Cluster 的内层 `client` 不是退回 Jupyter。** Spark 3.5.7 官方容器 entrypoint
在新 Driver Pod 内调用 `spark-submit --deploy-mode client`，且 Kubernetes
Driver 构建器自动加入 `spark.kubernetes.submitInDriver=true`。因此 Driver 内
读到 `spark.submit.deployMode=client` 是正常的 Cluster 启动流程，不能只凭该值
拒绝运行；也不要把容器内层改成 `cluster`，否则会再次递归提交 Driver。
验收脚本用 `--expected-deploy-mode cluster` 表达预期，再检查实际内部标识、
Driver Pod 名与 hostname、ARM 架构及两个独立 Executor；不要手工伪造
`submitInDriver`，也不能仅凭用户传入的 expected 参数认定成功。

最终验收须联合外部 Kubernetes 记录与业务产物：记录本次应用的 Driver Pod UID、
`spark-role=driver` 及 application 标签，确认 Driver 实际成功退出；对应内部报告
必须 `passed=true`，manifest 计数/哈希和 `validate_output` 均通过。只看到
`spark-submit` 返回 0、Pod 被创建或目录已有文件，都不代表业务成功。

2026-10-09 Cluster 实际验收：由 Jupyter 提交，独立 ARM Driver 和两个 ARM
Executor 均使用 Python 3.11.14 及相同 uid。只读处理 `dt=2026-09-01` 下两个
完整用户分片，共 50 个对象，得到 10 条轨迹：accepted=6、
quarantined_trajectories=4、parse_failures=0。外部 Driver Pod 为 Succeeded、
容器 exitCode=0；内部报告确认有效 Cluster 模式及 `submitInDriver=true`。
共享目录输出通过 `validate_output`，所有轨迹路径、SHA-256、大小、完整 lineage
和 manifest 计数均与同一 Spark checkout 的串行基准完全一致。验收摘要和对照
结果分别保存在本次输出目录旁的 `-acceptance.json`、`-comparison.json` 中；
`-final.json` 联合两份结果判定最终通过，不能单独把前一份运行验收当作基准对比成功。
未写入 S3 输出，普通 Python 部署未改动。本轮成功及失败尝试关联的 Pods、
Services、ConfigMaps 均已核对归零；临时凭据副本已删除，原账号文件及诊断输出保留。

此成功仅覆盖上述小分片。`dt=2026-09-15` 的只读清单有 1,014,558 个候选对象、
225,695,782,263 字节；事故用户仍有 878,617 个请求、198,335,144,295 字节，
其中 `no_session_id/` 子目录就有 785,174 个请求、169,612,001,885 字节。
以上按 S3 目录统计，不等同于解析后的实际语义 scope；按用户拆分后仍存在巨大
分组风险，不能推断全量不会 OOM。该日期全量尚未运行，
单 scope 磁盘化及发布元数据的完整流式化也尚未实施。

平台账号可能可以观察 Pod 状态但没有 `pods/log` 权限。此时可以为**下一次新任务**
配置自有 Driver 启动包装器，将该任务 stdout/stderr 写入专属共享日志路径，
保留原启动参数和退出码；不能通过修改平台权限解决，也不要输出完整 SparkConf、
凭据或原始业务内容。停止本地提交进程不保证远端任务结束；先按本次 application
及准确 Pod UID 确认 Driver/Executor 状态，保存验收证据，再清理本次应用资源和
临时凭据。不要清空共享 PVC、父目录或失败输出以便重跑。

`--prefix-shard` 是输入前缀下真实存在的相对 S3 key 前缀，例如 `<user_id>/`，
不是任意编号或自动哈希桶。可重复传入互不重叠的完整分片；试跑统计只对应所选
分片，不能证明全日期覆盖。全量前需核对输入清单、单 scope 峰值与输出保护上限。
正式全量也可以改用 `--auto-prefix-shards`：Spark 会在一个 Executor 分区上用
`Delimiter="/"` 扫描日期目录的第一层用户前缀，只将有界的前缀名列表收集到
Driver，然后沿用 Executor 侧对象清单/读取流程；不会把日期下百万对象的清单
收集到 Driver。默认最多接受 100,000 个前缀，可用 `--max-prefix-shards`
调整；自动模式不得同时传 `--prefix-shard`。如果 S3 兼容端点忽略 `Delimiter`、
把嵌套对象返回到 `Contents`，或返回畸形清单，任务会直接失败而不发布部分结果，
避免把空的/不完整的前缀发现当作成功。
正式入口仍是 `spark/tokenplan_normalize.py`，使用 `--output-uri` 而非验收脚本的
`--output-dir`；选择 S3 输出时两端还需独立可用的输出 boto3 默认凭据链，
或显式传入 `--output-credentials-file` 指向安全共享凭据文件。
DolphinScheduler Worker 的 `/share` 视图可能不同于 Jupyter/Kubernetes，迁移时
另行核对提交端可读的 properties/template 路径和 Pod 内的共享入口路径；不要
修改普通 Python 调度节点。

## 数据路径和运行模型

输入仍是原始 TokenPlan 对象：

```text
s3://<bucket>/<input-prefix>/dt=YYYY-MM-DD/<user>/<session-or-placeholder>/req_*.json
```

Executor 分片读取前缀，过滤 `req_*.json`，逐对象调用现有 TokenPlan 适配器和 provider parser。解析记录在 Spark Shuffle 中按 `(scope_kind, scope_id)` 分区，随后在分区内排序并逐个 scope 构建轨迹。最终直接写入现有 flat v4 JSONL 输出；以下 S3 前缀也可替换为独占共享目录，文件布局不变：

```text
s3://agent-trajectory/lakehouse/token-plan/normalized/v005/dt=YYYY-MM-DD/
├── <session_id>_sub_<n>.jsonl
├── no_session_id_<trajectory_hash>_sub_0.jsonl
├── lineage.jsonl
├── manifest.json
└── diagnostics/
    ├── input_diagnostics.ndjson
    ├── ._objects_part_<run_id>_<partition>.ndjson
    ├── ._lineage_part_<run_id>_<partition>.ndjson
    └── ._diagnostic_part_<run_id>_<partition>.ndjson
```

轨迹正文、`lineage.jsonl` 和最终 `manifest.json` 直接位于日期目录下，便于下游
按既有路径读取。运行中的分片回执与输入诊断统一位于 `diagnostics/` 子目录，
不会与正文文件混在一起；诊断分片合并为 `diagnostics/input_diagnostics.ndjson`。
失败运行的未发布文件和运行标记按原约定保留以便排查，不能将其当作成功结果；
已成功发布的 manifest 只登记正文和 lineage，不把诊断辅助文件加入业务文件计数。

缺失 session（包括字面值 `no_session_id`）先按既有用户身份归集，再按
**解析后、未压缩的 Snapshot JSON 字节数**分块。这里不是 S3 原始文件大小、
最终输出大小或内存额度，也不另加消息数、请求数分块阈值。
每个用户按其占全部缺失-session 正文字节的比例获得目标块数，至少一块、
不超过其请求数；`shuffle_partitions` 是这一比例分配的全局目标，不是所有
用户最终块数之和的硬上限。小用户保留一块，巨型用户自动获得更多块。

用户内部按 `captured_at` 的 UTC 时刻、来源路径和来源哈希稳定排序，时间只
决定顺序，不再按固定几小时切片。时间缺失、无效或没有时区时排在已知时间
之后，并按来源身份稳定排序。缺失用户仍使用既有空用户身份，与字面值
`no_user_id` 保持区分。按累计字节切块，一条请求完整留在其所在块中，
不截断历史或回复；单条请求大于目标块大小时独立成块。因此块大小、块数
是近似目标，不保证严格相等。真实 session 和普通 Python 部署的分组不变。

规划仅排序轻量的时间、标识、字节数与前缀组件摘要，不为分块先做前缀去重，
也不先把大用户全部正文汇集到一个任务。Driver 接收块级边界和摘要，
按块字节从大到小分配给当前负载最小的物理 Spark 分区；真实 session 整组
也参与任务负载分配，避免单纯哈希碰撞把几个大块再次堆到同一任务。

块内继续使用精确前缀聚合与结构化子代理挂载，跨块不合并、不挂载、不补接
边界。确定性的块标记参与缺失-session 轨迹的语义身份与输出文件名，
输出 metadata 仍为 `no_session_id`，不生成伪造的真实 session。
相同输入和配置的分块不依赖输入枚举或 Spark 任务执行顺序，但改变分区目标
或输入可能改变分块与轨迹身份。批次反馈计数与 `total_tokens` 的完整口径见
[生产反馈说明](production-feedback.md)。派生字段校验仍严格执行。

该模式**不保存外部 Snapshot Parquet，也不创建长期 `spark-work` 目录**。Spark
仍会使用 Executor 本地 Shuffle/spill 和发布元数据临时 SQLite。manifest 最后
发布；已知输入失败及不可构建输入写安全 `diagnostics/input_diagnostics.ndjson` 侧车，摘要返回路径/哈希，它不
是轨迹也不加入 v4 files。内部程序异常必须导致失败，不能伪装成坏输入跳过。

输出前缀或目录非空时在任何数据写入前拒绝运行，不覆盖旧 manifest 引用的数据。
同一输出目的地必须独占；S3 空前缀检查不是分布式锁，共享目录还使用本次运行的
独占标记防止误写到非共享挂载。失败的新目的地可能留下未发布对象/文件，不会
自动删除，也不能将它们视作成功输出。计数由成功 Spark task 返回的数据流汇总，
不使用重试时可能重复累加的 accumulators。

跨块的同一对话可能保留多个片段或重复历史，这是显式选择的截断聚合边界。
跨 scope 的意外文件冲突仍应失败，不得通过全局去重丢弃来源或恢复跨块合并。
历史产物的身份与校验兼容性需按具体版本核验。

## 内存和失败边界

读取先分布式列举轻量对象引用，再重新分区进行 GET/解析，避免一个巨大用户
前缀的所有下载工作固定在一个 task。解析结果只缓存在 Executor 本地磁盘。
完整 scope 的常量大小摘要通过预算检查后广播；缺失 session 的构建 scope
由用户和字节块确定。只有整个块没有子代理路由证据时，才在块内按可证明
独立的消息前缀组件继续处理。真实 session 保留整体分支编号；含路由证据的
块保留块内整体规划，不按协议或随机桶再拆，也不回退成跨块整用户规划。

默认每组件探测上限为 `--scope-memory-mib 64` / `--max-scope-snapshots 1000`；
达到任一上限即使用磁盘快照、精确前缀索引、磁盘去重和逐 root 物化。
即使没达到上述上限，只要有子代理关联、spawn 或 agent-message 证据，也使用
磁盘路径：诊断共享目标集合并落盘，单根诊断先检查预算再解码，避免诊断数与
叶子数相乘形成内存列表。规划器各阶段在保留每条诊断前共享累计字节预算，
避免长来源路径随异常调用数量重复展开；单条诊断构造仍有暂态分配，不是硬 RSS 限制。
父叶查找有有界反向索引，装不下时回退精确扫描；
重复事件复用相同候选集合，不改变缺失父节点或歧义诊断的语义。
超大的单对象、单 root 或 compact 路由规划仍受显式预算限制，超限直接失败，
不会截断或跳过后发布成功。预算是序列化数据量 admission，不是 Python RSS
硬限制，也不能证明任意数据都不会 OOM；复杂子代理规划仍是重点验收边界。

Driver 不收集 Snapshot/Trajectory 正文，输出描述符和 manifest 已改为 SQLite
落盘及流式生成；仅保留每分区的小回执。默认 `--max-output-objects 2000000`、
`--max-partition-objects 2000000` 为数量保护，不再对应同等规模的内存列表。
构建/发布先通过并行动作执行，再读取磁盘缓存的小回执，避免逐分区拉取时
将业务阶段意外串行化。磁盘兜底主要降低峰值，提速来自下载/组件/分区并行，
不能把磁盘路径本身描述为必然更快。

不保存外部 Snapshot 的代价是失败后可能重新读取/解析输入。字节分块和任务
负载分配降低大用户造成的长尾，但正文字节数不是精确耗时或峰值内存预测。
单条超大请求、真实超大 session、子代理关系复杂度和存储吞吐仍会影响运行；
不能将块数等同加速倍数，也不能保证不会 OOM。Client/Cluster 模式本身不能
替代内存边界设计。

## 可复用 Cluster 资源档（默认只生成计划）

`spark/submit_cluster.py` 提供独立的 Cluster 提交器，不修改普通 Python 调度节点，
也不修改上面已经验证的私有小样本 wrapper。默认 **dry-run**：只检查非敏感模板、
路径和参数并打印资源摘要，不创建 Pod、不读取认证文件、不创建输出或控制目录。
只有显式添加 `--execute` 才会真实提交。`starter` 已完成真实 50 条小分片验收，
详见下方记录；`scale` 和事故日期全量尚未通过实测。

| 档位 | Executor 数 × 核数 | 单 Executor JVM + overhead | Executor 总资源 |
| --- | --- | --- | --- |
| `starter`（默认） | 16 × 8 | 16 GiB + 48 GiB = 64 GiB | 128 核、1 TiB |
| `scale`（只配置，不自动扩容） | 48 × 8 | 16 GiB + 48 GiB = 64 GiB | 384 核、3 TiB |

两档均不设置 `spark.executor.pyspark.memory`，并关闭额外 off-heap 内存，避免在
64 GiB 之外另加 Python 内存请求。**64 GiB 是整个 Executor Pod 的额度，不是
每个 Python worker 的额度。** 每个 Executor 最多同时运行 8 个单核 task，Python
进程、native buffers 等共享 48 GiB overhead，不能据此保证巨型单 scope 不会 OOM。
Driver 默认另计 4 核、8 GiB JVM + 4 GiB overhead，可通过对应参数显式调整。
默认路由保护为 `--max-routing-mib 1024 --max-routing-snapshots 1000000`，
单根保护仍为 `--max-root-mib 64`；放宽路由上限不代表可以放宽根的内存边界。

提交器将 `spark.kubernetes.driver.master` 显式设为与提交端相同的 HTTPS API
地址，避免某些节点解析默认 `kubernetes.default.svc` 失败。TLS、CA、主机名
校验均保持启用；这是 Spark 支持的 [Driver API 地址配置](https://spark.apache.org/docs/3.5.7/running-on-kubernetes.html#configuration)，不是忽略证书错误。

准备两个**不含密钥字面值**的 JSON Pod 模板（JSON 本身就是合法 YAML）。可以将
已验证的专用模板转换为 JSON；保留平台要求的 PVC/subPath、nodeSelector、
tolerations、hostAliases 等，不使用或修改共享默认模板。业务容器名默认
`spark-kubernetes`。提交器生成副本，强制 ARM nodeSelector、Driver/Executor
相同的非 root uid/gid、ARM Python/PATH/PYTHONPATH，并要求 PVC/NFS/CSI 挂载
覆盖源码、ARM 环境、控制目录和输出目录。它不会读取或修改原模板。

生成模板会添加磁盘 `emptyDir` 到 `/tmp/trajfoundry-spark-local`，同时配置
Spark local dir、Python `TMPDIR` 和 Ivy 路径，不使用 tmpfs，不把 SQLite、spill
或 shuffle 放到共享 `/share`。原模板不得预先声明 `spark-local-dir-*` 卷或占用
这个临时路径。输入凭据仍仅传文件路径，真正加载在 Executor；源码与凭据父目录
的实际遍历权限、PVC 在不同环境中的视图仍须在实际 Pod 核对。

```bash
# 首先只生成计划；下面不包含 --execute，不会启动集群任务。
.venv/bin/python spark/submit_cluster.py \
  --profile starter \
  --project-root "$SPARK_CHECKOUT" \
  --arm-python "$ARM_PYTHON" \
  --driver-pod-template "$ARM_DRIVER_TEMPLATE_JSON" \
  --executor-pod-template "$ARM_EXECUTOR_TEMPLATE_JSON" \
  --image "$SPARK_IMAGE" --queue "$SPARK_QUEUE" \
  --master "$K8S_MASTER" --namespace "$K8S_NAMESPACE" \
  --service-account "$SPARK_SERVICE_ACCOUNT" \
  --ca-cert "$K8S_CA_FILE" --oauth-token-file "$K8S_TOKEN_FILE" \
  --run-as-user "$SPARK_UID" --run-as-group "$SPARK_GID" \
  --control-dir "$NEW_SHARED_CONTROL_DIR" \
  --output-dir "$FRESH_SHARED_OUTPUT_DIR" \
  --input-uri "$INPUT_URI" --prefix-shard "$COMPLETE_USER_PREFIX" \
  --endpoint-url "$S3_ENDPOINT_URL" \
  --input-credentials-file "$EXECUTOR_CREDENTIAL_FILE"
```

正式运行需要再次审核参数、队列余量并**显式增加 `--execute`**。`--profile scale`
只改变静态请求，不会依据队列余量自动扩容；本脚本不启动任何额外预检任务。
`--prefix-shard` 可重复，必须覆盖完整用户分片；`--job-arg=--参数名 --job-arg=值`
可传递新的有界构建参数，但不能覆盖输入/输出/认证路由（也不接受这些参数的缩写）。
默认示例使用共享目录；正式任务也可将 `--output-dir` 替换为
`--output-uri s3://bucket/prefix/dt=YYYY-MM-DD/`。两项互斥，S3 输出只支持
`--validation-mode manifest`，不允许覆盖非空前缀或与输入前缀重叠。
`--output-credentials-file` 显式指定 Driver/Executor 可读的安全凭据文件；
未指定时使用 boto3 默认凭据链，输入凭据不会自动作为输出凭据。
两个凭据参数只传路径，文件必须匹配运行 UID、权限为 0400/0600，且共享挂载可达。
S3 轨迹由各 Executor 上传后流式回读验证 SHA256/字节数，再返回小型成功回执；
Driver 校验合并后的 lineage/diagnostics 后最后发布 manifest。既有同名不同内容
拒绝覆盖。空前缀检查和 HEAD 冲突检查不是分布式锁，目标日期仍须由本次运行独占。

默认模式下，输出凭据除 LIST/PUT/GET 外还需要目标前缀下的 DeleteObject 权限：
Driver 合并并校验临时清单、lineage 与诊断分片后，删除本次临时分片，再发布
成功 manifest。能上传和读回不代表能完成清理，默认模式的删除拒绝仍会失败。

若已明确允许保留辅助文件，可显式添加 `--retain-s3-auxiliary`，仅支持 S3 输出
及 manifest 校验模式。此模式不尝试删除临时分片，按运行 UUID 隔离分片名，
仍核对所有轨迹、分片以及合并后 lineage/diagnostics 的实际 SHA-256/字节数。
manifest 只登记最终业务文件，完成摘要单独记录本次保留的辅助对象数。
初始化只放行严格命名的历史辅助分片与权限探针，仍拒绝既有业务 JSONL、
manifest 或未知文件；这不是任意非空目录续跑或覆盖开关。读取结果以 manifest
为准，不能将以点开头的内部辅助文件当作训练数据。

控制目录必须不存在，父目录已存在且可写；输出目录必须全新或为空，二者必须
分离且不能嵌套。控制目录按 `0700` 创建，因此提交者 uid 必须与 Pod uid 一致。
其中保存生成的模板、资源计划、submit/Driver 日志和最终判定，不污染业务输出。
提交使用独立的空 `spark/cluster-isolated.properties`，不会加载未经审核的默认
资源或认证配置；该文件应保持仅注释。只向 Spark 传认证文件路径，TLS 与主机名
校验保持开启。不要手工注入 `spark.kubernetes.submitInDriver`。

成功必须同时满足：提交进程没有报错，实际 Driver Pod 的 name/namespace/UID、
本次随机 run-id 与 application 标签一致，Pod 为 `Succeeded`、所有容器 exitCode
为 0，Driver ARM/Python/运行身份探针通过，唯一业务完成摘要与输出路径一致，
manifest 的流式 SHA-256 与业务摘要相同。Spark watcher 单独返回 0 不算成功。
Driver 日志通过 `cluster_driver_entrypoint.sh` 保留，不要求 `pods/log` 权限。
`final.json` 是联合判定结果，不在基准对比之前提前发布“最终成功”。

任务和连续 stage 尝试上限均为 4，开启失败 Executor 排除，推测执行仍关闭。
这样允许广播/网络的瞬时故障有限重试，但不会无限重试或跳过坏数据。
重试期间输入日期和输出前缀须保持独占、稳定：这是确定性幂等输出，不是跨应用
事务或 Driver 断点续跑；整次应用失败后仍须检查非空输出保护，不得强制覆盖。

Spark 解析缓存及诊断/输出 JSON 统一复用 `json_codec` 的大整数兼容编码，
超过 64 位的 JSON 整数保留原始精度，不转浮点、不截断，也不按解析失败跳过。
普通整数仍走快速编码路径；模型的 Pydantic JSON 读写保留并由正负大整数、
三种协议及内存/磁盘构建回归覆盖。`Integer exceeds 64-bit range` 属于代码兼容
问题，增加 Executor 内存或反复重试不能解决。

默认 `--validation-mode manifest` 仅额外流式核对 manifest 哈希，不把整个 manifest
或全量 lineage 装入内存，也**不声称完成独立全量语义复验**。小样本回归显式使用
`--validation-mode small`，会额外执行 `validate_output` 与 manifest 计数核对；可加
`--baseline-dir "$SERIAL_BASELINE_DIR"`，联合比较完整 lineage、轨迹文件路径/
SHA-256/大小、manifest 计数及 input_root。小样本模式限制 10,000 个输入/轨迹、
16 MiB manifest、单文件 32 MiB、所列文件总计 256 MiB，超限直接失败而不进行
无界校验；不适用于事故日期的百万对象规模。

在真正申请 16 或 48 个 Pod 前，使用授权的只读 Kubernetes/调度平台工具核对：

- 节点必须满足 ARM、队列、affinity/tolerations/PVC 调度约束；不能仅看集群总内存。
- 每个可用槽位需要**同一节点**同时容纳 8 CPU 与 64 GiB。按节点的 allocatable
  减去已调度 Pod 的 **requests**（不是当前 RSS），取 CPU/内存槽位较小值再求和。
  可用 `kubectl get nodes -l kubernetes.io/arch=arm64 -o json` 和授权范围内的
  `kubectl get pods -A -o json` 只读盘点；命令本身不保证拥有跨 namespace 权限。
- 另计 Driver 的 4 CPU/12 GiB、DaemonSet/系统开销、队列配额、ResourceQuota、
  LimitRange、Pod 数量/调度策略，以及磁盘 emptyDir 的剩余容量和 ephemeral-storage
  请求。磁盘溢写并不等于磁盘无限；应按最大完整用户与并发任务的实测放大估算。
- `starter` 的 1 TiB、`scale` 的 3 TiB 是请求总量，不是当前队列已确认的可用量。

默认 `--timeout-seconds 0`，不设置 Pod 总运行 deadline，提交端也不限时等待；
生成模板会移除继承的 `activeDeadlineSeconds`。仅显式传正数时启用总时限，
提交端额外等待最多 60 秒。网络请求等单次操作超时仍保留。
Driver 每 60 秒及阶段切换时写入 `TF_SPARK_PROGRESS`，包含当前阶段、累计耗时、
活动 stage 的任务总数/已完成/运行/失败数；这些是遥测，不是输入输出精确计数。
任务数不变化可能是在处理一个大型分组，不代表成功或必然挂死。
超时停止本地 watcher 不保证远端任务已经停止，必须保存
本次 plan/日志并按精确 Pod UID、run-id/application 标签核对清理。脚本不自动
删除 Pod、共享 PVC、控制目录或失败输出；下次运行始终使用新的目录。

### 128 核档实测（2026-10-09）

从 Jupyter 直接提交 Cluster，均读取真实完整用户前缀，输出到全新的共享目录，
不写生产 S3。已观察到 16 个 Executor Pod 同时按每个 8 核/64 GiB 运行。

| 输入 | 条数 | 轨迹数 | 业务阶段耗时 | 观测 Python worker RSS 峰值 |
| --- | --- | --- | --- | --- |
| 2026-09-01 两个完整小用户，最终补丁后强制磁盘复测 | 50 | 10 | 53.342 秒 | 102.0 MiB |
| 2026-09-15 用户 `499100836/`，输入约 2.21 GB | 6,241 | 1,164 | 136.309 秒 | 478.1 MiB |

小样本逐文件哈希/字节、lineage、manifest 计数与串行基线完全一致。
中分片解析失败为 0，938 条 accepted、226 条轨迹隔离、85 条记录隔离；
13 个组件走磁盘、40 个走内存，最大原 scope 为 784 条、最大组件 749 条。
完成后在有界小样本校验范围内独立执行 `validate_output`，通过文件/语义与计数
校验；这一中分片没有串行基线对照。控制目录分别为
`/share/gezhilong/trajfoundry-spark-runs/bounded-50-20261009-v2-control` 和
`/share/gezhilong/trajfoundry-spark-runs/bounded-6241-20261009-v2-control`；
中分片额外产物校验见 `artifact-validation.json`。

上述 RSS 是单个 Python worker 观测峰值，不是整个 Executor/任务的内存峰值。
时间不含 Pod 提交/启动与事后独立校验；缺少同资源旧实现基准，不能据此宣称
固定加速倍数。启动期间出现过 Executor 替换，原因未确认，不能归因为 OOM。
两个应用最终 Driver Succeeded/exit 0，终态资源已按 UID 清理，日志和输出保留。
事故日期的百万对象全量、最大热点用户以及 384 核档仍未验收；不能用这些分片
结论承诺任意全量输入不会 OOM。

最后的诊断文本累计预算补丁在中分片完成后加入，最终 50 条复测覆盖该版本。
最终默认测试套件 1,179 passed / 15 skipped；另显式加载本地 Spark 的并行执行、
混合组件、作业和本地输出回归 33 passed，不能用默认跳过的集成项冒充已运行。

## 本地契约测试

测试不需要连接真实 Spark 集群，覆盖配置安全边界、TokenPlan 单对象适配和 Shuffle 记录序列化：

```bash
uv sync --python 3.11 --dev --locked
PYTHONPATH=src .venv/bin/python -m pytest -q
bash -n spark/submit_client_probe.sh
```

完整契约测试验证确定性文件名、JSON roundtrip、派生字段、失败诊断、lineage
和 manifest。本地存储测试还覆盖非空拒绝、同内容重试、冲突不覆盖、原子发布、
流式哈希、运行标记、符号链接及路径保护，以及本地输出绝不创建 S3 客户端。
这些测试可以使用合成输入，不能作为读取真实 S3 的证据。
真实集群探针只证明调度、回连和环境；业务正确性仍需完整小用户分片对照
普通 Python 版的解析/失败数、轨迹、lineage 与 manifest。完整日期运行不会在
探针通过后自动执行。
