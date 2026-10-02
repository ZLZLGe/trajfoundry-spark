# TokenPlan Spark 试运行说明

这个仓库的 Spark 入口只服务 TokenPlan，原仓库中的 Python 任务不受影响。生产环境应由 DolphinScheduler 的 SHELL 节点提交 Spark Kubernetes **Cluster 模式**任务：

```bash
spark-submit \
  --master <平台提供的-kubernetes-master> \
  --deploy-mode cluster \
  --conf spark.submit.deployMode=cluster \
  --conf spark.executor.instances=24 \
  --conf spark.executor.cores=4 \
  --conf spark.executor.memory=32g \
  --conf spark.executor.memoryOverhead=8g \
  --conf spark.driver.cores=4 \
  --conf spark.driver.memory=16g \
  --conf spark.driver.memoryOverhead=4g \
  --conf spark.driver.maxResultSize=1g \
  --conf spark.sql.shuffle.partitions=2048 \
  --conf spark.default.parallelism=2048 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.skewJoin.enabled=true \
  --conf spark.python.worker.reuse=true \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.hadoop.fs.s3a.connection.maximum=32 \
  --conf spark.hadoop.fs.s3a.threads.max=16 \
  --conf spark.hadoop.fs.s3a.fast.upload=true \
  --conf spark.hadoop.fs.s3a.fast.upload.buffer=disk \
  --py-files trajfoundry_spark.zip \
  spark/tokenplan_normalize.py \
  --input-uri s3://agent-trajectory/lakehouse/token-plan/masked-raw/v002/dt=2026-09-16/ \
  --output-uri s3://agent-trajectory/lakehouse/token-plan/normalized/v005/dt=2026-09-16/ \
  --input-format tokenplan \
  --endpoint-url http://d-ceph-ssd-inside.pjlab.org.cn
```

平台的 Kubernetes namespace、ServiceAccount、队列、Spark 镜像、pod template 和上传路径应从公司调度平台配置注入，不要把凭据或 kubeconfig 写进仓库。Cluster 模式是必要条件：Client 模式会把 Driver 留在 DolphinScheduler Worker 上，重新受到 Worker 内存限制。

## 数据路径和运行模型

输入仍是原始 TokenPlan 对象：

```text
s3://agent-trajectory/lakehouse/token-plan/masked-raw/v002/dt=YYYY-MM-DD/req_*.json
```

Executor 分片读取前缀，过滤 `req_*.json`，逐对象调用现有 TokenPlan 适配器和 provider parser。解析记录在 Spark Shuffle 中按 `(scope_kind, scope_id)` 分区，随后在分区内排序并逐个 scope 构建轨迹。最终直接写入现有 flat v4 JSONL 输出：

```text
s3://agent-trajectory/lakehouse/token-plan/normalized/v005/dt=YYYY-MM-DD/
├── <session_id>_sub_<n>.jsonl
├── no_session_id_<trajectory_hash>_sub_0.jsonl
├── lineage.jsonl
└── manifest.json
```

该模式**不保存外部 Snapshot Parquet，也不创建长期 `spark-work` 目录**。Spark 仍会在 Executor 本地磁盘使用 Shuffle/spill，这是运行时临时数据，不是可复用的中间数据集。`manifest.json` 最后发布，未列入 manifest 的重试残留不属于正式输出。

## 内存和失败边界

Driver 只保存小型输出描述和统计信息，禁止 `collect()`、`toPandas()` 或缓存整日原始数据。`mapPartitions` 一次只保留当前 scope，scope 完成后立即释放。极大的单一 session/user scope 仍可能导致单个 task OOM，需要在作业中监控和隔离。

不保存 Snapshot 的代价是：构建或发布阶段失败后必须从原始 S3 重新读取和解析；它减少了中间 I/O，但不是 OOM 的根本修复。真正的内存边界来自 Cluster 模式、scope 分区、无全量收集和足够的 `memoryOverhead`。

## 本地契约测试

测试不需要连接真实 Spark 集群，覆盖配置安全边界、TokenPlan 单对象适配和 Shuffle 记录序列化：

```bash
uv sync --python 3.11 --dev --locked
uv run pytest -q tests/test_spark_config.py tests/test_spark_records.py tests/test_spark_adapters.py
```

完整输出契约测试还会验证确定性文件名、轨迹 JSONL、lineage 和 manifest。真实集群试跑前，先用小前缀或小日期对比旧 Python 版本的解析数、失败数、轨迹数、lineage 和 manifest。
