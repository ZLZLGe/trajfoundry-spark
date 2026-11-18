# Boyue 与 DeepInfra Spark 标准化

复用 Spark 的按会话聚合、无会话字节分块、前缀去重、质量隔离和 v4 输出。
此次只做标准化，不调用分类模型。

| 来源 | 输入选择 | 正式输出 |
|---|---|---|
| Boyue | `boyue-rich/masked-raw/v002/YYYY-MM-DD/` 下最终 `raw.json` | `boyue-rich/normalized/v001/dt=YYYY-MM-DD/` |
| DeepInfra | `deep-infra/masked-raw/v002/dt=YYYY-MM-DD/` 下 JSON 和 Parquet | `deep-infra/normalized/v002/dt=YYYY-MM-DD/` |

这些路径位于同一 lakehouse 前缀。DeepInfra 的 `normalized/v001` 保留。
Boyue 的 `request.json`、`response.json`、临时文件和同步标记不是独立请求。
DeepInfra 的 `manifest.json` 不是请求。Parquet 必须含字符串 `raw_json`；逐行
来源是 `object.parquet#L00000000`，物理文件损坏使任务失败，单行非法 JSON
进入解析失败诊断。读取时检查对象大小、ETag，数据盘点有独立的物理文件计数。

Boyue 优先使用明确的请求路径，仅在响应协议明确时推断旧数据路径。没有记录
HTTP 状态码时会留下审计标记，根据响应结构判断完整性，不补造 HTTP 200。
仅剩 `response_text`、非终态 Responses 事件、流中断都不能作为完整轨迹。
只保留允许的身份请求头；请求 ID 不充当会话 ID。一天内的真实会话保持完整，
不同日期不合并；没有会话的请求按用户与标准化字节量分块。

`total_tokens` 保持现有 `unicode-word-v1` 估算口径，统计标准化轨迹内容，
不是上游 API 的计费 token 数。manifest 保留 `record_count`、
`accepted_trajectory_count`、`quarantined_trajectory_count`、`parse_failure_count`、
`started_at_ms`、`completed_at_ms` 和 `counts`。其中 `counts.input_files`
在 Parquet 输入下计逻辑请求行。识别出的非轨迹端点计入
`counts.skip_reason_counts.non_trajectory_endpoint`，不算解析失败。

## 执行

`spark/submit_cluster.py --input-format boyue|deepinfra` 可以提交一个完整日期。
已有 TokenPlan 入口仍默认 `tokenplan`。生产资源建议 starter：16 个 Executor，
每个 8 核、16 GiB JVM + 48 GiB overhead；读取并行 128，shuffle 256；
Driver 为 4 核、8 GiB + 4 GiB。先执行两个单核 Executor 的合成环境探针，
然后用完整小日期与新输出前缀做业务验收。Parquet 需要 PyArrow 21；x86/ARM
依赖分别安装，使用同版本的 Python 3.11。

批量入口接受包含 `submit_args` 的非敏感 JSON 配置（与现有提交器参数一致）：

```bash
python spark/backfill_lakehouse.py \
  --config /absolute/private/backfill.json \
  --state-root /absolute/shared/backfill-state \
  --execute
```

默认扫描两路来源，截至启动时上海时间的前一天。`--source`、`--date` 可选择
来源和日期；省略 `--execute` 只盘点并计划。`--observe-only` 只记录首次盘点。
同一生产部署所有调用必须使用同一个共享 `state-root`，全局锁确保两路来源
总共只运行一个业务任务，另有日期锁。每个日期有独立状态、计数、日志和结果；
进程退出不代表 Spark Driver 已退出，遗留 `running` 状态须先核对真实 Driver。
提交器失败时会检查实际 Driver 的身份和终态；检查失败则保留 `running`
并停止整个批次，防止下一日期与尚在运行的任务重叠。
可在 DolphinScheduler 调用此入口，日期显式传入，不需改现有 TokenPlan 调度。

有上游 manifest 的日期必须明确成功；没有 manifest 的历史日期必须超过
72 小时，并有间隔至少 30 分钟、完全相同的两次对象盘点。观察记录不能用
等待的时间代替第二次盘点。每个任务在读取前和发布 manifest 前再次核对
所选输入的文件名、大小及 ETag 指纹。日常重扫只回看最近 7 天，已成功的旧
日期封存；显式 `--recheck` 才重新盘点旧日期。

## 固定目录与重跑

首次执行必须是空的日期输出目录。控制状态将输出归属绑定到输入指纹、代码
哈希和参数；每个输出对象携带 `tf-owner` 元数据。失败后、输入和代码均未
变化时可复用相同内容的文件；不同内容、未知归属或已有成功 manifest 都会
拒绝覆盖。manifest 最后发布。诊断辅助文件按运行保留，便于核查失败现场。

当前实测对象存储没有 DeleteObject 权限，而且忽略条件 PUT；因此互斥使用
共享文件系统锁，不依赖条件 PUT。输入或代码发生变化时，本入口会保留旧
目录并阻止重建，不会递归删除或绕过权限。需重建的日期须在取得删除能力后，
先核对归属和旧 manifest，再按明确清单清理；已有成功结果不自动失效。
本轮首次写入新版本目录不依赖删除权限。

判断运行是否完成应看每日期的 `state.json` 与 Spark 的 `final.json`。
`summary.json` 汇总成功、未就绪、受阻日期；仅提交成功不表示标准化完成。

## 仅执行 Boyue 的历史批次

`spark/backfill_boyue.py` 是固定来源的专用监督入口，没有来源切换参数。
它复用现有共享 `state-root` 的日期状态和锁，但将冻结日期清单、独立校验结果、
状态与每 30 秒更新的 `progress.md` 写入专用 `batch-root`。不恢复两路来源的
旧监督进程，也不修改旧批次的暂停记录。

```bash
python spark/backfill_boyue.py \
  --config /absolute/private/submit-config.json \
  --state-root /absolute/shared/existing-state \
  --batch-root /absolute/shared/existing-state/boyue-only \
  --cutoff 2026-10-31 \
  --execute
```

省略 `--execute` 时只读盘点日期，不创建批次文件或提交任务。执行时固定使用
已验收版本、starter 资源、读取并行 128、shuffle 256；已有成功日期先核验
产物后跳过，新日期按时间顺序串行执行。清单冻结后重启不会自动扩展日期。
每个成功日期独立核对输出归属、完整 manifest 哈希、轨迹和输入计数，并抽查
最多 8 个轨迹的哈希、结构、来源字段及 `total_tokens`。任务成功和数据被接受
分别计数；失败或受阻日期不计入完成。业务尝试最多两次；归属冲突立即受阻。

发送 SIGTERM/SIGINT 后停止提交后续日期；当前仍在只读盘点时终止盘点，
已经提交的 Spark 日期完成后退出。若远端状态无法确认，保留运行状态并受阻，
必须核对 Driver 后再恢复。此入口仅处理标准化，不调用分类模型。
