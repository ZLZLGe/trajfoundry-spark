# 请求日志式轨迹去重（traj_dedup）

请求日志式的捕获数据里，agent 每调一次模型就留下一条带完整对话的快照，规范化之后同一段对话常常还是好几条记录：同一请求被重发后没被执行的回答、上下文压缩后重建的历史、前后脱敏不一致的副本等，详见关联的 issue。这个脚本在规范化输出之后单独运行，只根据消息序列的结构判断哪些记录多余，不读语义、不用随机数，也不改动现有管线。

## 规则

比较时每条消息取规范化 JSON 的 sha256，去掉工具调用 id、`tool_call_id` 和所有 reasoning 字段。有会话 id 的记录按会话分桶（跨模型目录合并），没有会话 id 的按第一条用户消息分桶。

第一级只删能结构上证明多余的记录：完全相同的只留一条；是另一条严格前缀的删掉；一条记录去掉结尾的模型回答后，剩下的历史被另一条记录继续往下走了（有工具结果或新的用户消息），说明这个回答被弃用，删掉；同一历史下几个回答都没人继续时，只留最晚的一条。两条消息如果只差脱敏占位符（默认识别 `<PHONE>` 这类尖括号大写占位符、`[USER]`、`[REDACTED]`、`****` 等，可用 `--placeholder-regex` 改），也当作同一条消息，`--no-desens` 可以关掉。

第二级在第一级的结果上，每个对话只留最后一版：有会话 id 时主线程一条、每个子代理（按第一条用户消息区分）一条；没有会话 id 时，开头第一轮问答完全相同的记录算同一个对话。主要用来合并上下文压缩前后的版本。

完整规则、确定性保证和已知局限写在 `traj_dedup.py` 文件头。

## 用法

```bash
# S3（老版本 ceph 也可以：只用 list_objects v1、path-style，并绕开环境里的 http_proxy）
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...
python3 tools/traj_dedup/traj_dedup.py --src s3://<bucket>/<prefix>/ \
    --endpoint-url http://<s3-endpoint> --out runs/<name> --procs 6

# 本地目录
python3 tools/traj_dedup/traj_dedup.py --src /path/to/normalized --out runs/<name>
```

只处理 key 形如 `.../dt=<日期>/<session>_sub_<n>.jsonl` 的文件。`--level snapshot` 只输出第一级结果，默认是第二级；`--copy-to s3://...` 可以把保留的记录在服务端复制到新前缀。对象只在内存里解析，不落盘。

## 额外工具分流

有些记录声明或调用了数据集标准工具之外的工具（比如 MCP 工具）。去重结束后，脚本默认把这类保留记录分到 `<out>/extra_tools/kept.jsonl`，其余的留在 `<out>/kept.jsonl`，两个文件的每一行都带 `extra_tool_tag` 和 `split`（`main` 或 `extra_tools`）。分流只作用在去重之后的保留记录上，按每条保留记录自己的字段判断，去重的规则和结果完全不受影响。

判断依据是记录里的 `extra_tool_tag`（`none`、`declared_only`、`called`）和 `extra_tools`（每个条目有 `name`、`server`、`kind`、`call_count`、`declaration`，`kind` 为 `mcp` 或 `unknown`）。`extra_tool_tag` 缺失时退回看 `extra_tools` 是否非空，两个字段都没有的记为 `absent`，不分出去，在 summary 里单独计数。`--extra-split` 选口径：`tag`（默认）是 `extra_tool_tag` 不为 `none` 就分出去，声明和调用、`mcp` 和 `unknown` 都算；`mcp` 是 `extra_tools` 里有 `kind` 为 `mcp` 的条目；`mcp-called` 是有 `kind` 为 `mcp` 且 `call_count` 大于 0 的条目；`off` 是不分，也不建子目录。

`--copy-to` 会把分出去的记录复制到 `<dest>/extra_tools/<相对路径>`。想把保留的原始对象落到本地目录时用 `--export-dir DIR`：保留的对象原样字节写到 `DIR/<相对路径>`，分出去的写到 `DIR/extra_tools/<相对路径>`。这个选项会重新读取保留的对象，逐个写完即丢弃，只建议在自己的机器上用；`DIR` 和本地源目录互相包含时，或者相对路径里有 `..` 时，脚本会拒绝执行。

## 输出

`--out` 目录里只有元数据（key、会话 id、时间、计数、哈希），没有消息正文，所有文件按 key 排序。同一输入两次运行的输出逐字节相同。

- `kept.jsonl`：保留的记录（开了分流时只含留在主目录的那部分），含所属对话、消息数、`normalization_audit.tag`、`extra_tool_tag`、`split` 和它代表了多少条输入。
- `extra_tools/kept.jsonl`：分流出去的保留记录，字段相同；`--extra-split off` 时不生成。
- `dropped.jsonl`：删除的记录，含规则、所在级别、直接参照（witness）、最终代表它的记录，以及这次删除是否依赖脱敏等价。
- `buckets.jsonl`：每个会话或分桶的输入数和两级剩余数。
- `summary.json`：各规则删除数、两级剩余数、按 audit tag 的分布、脱敏合并统计、分流统计（两级各自两侧的条数、`extra_tool_tag` 分布、`absent` 数）和输入 key 列表的哈希。

## 测试

```bash
python -m pytest tools/traj_dedup -q
```

## 实测

SXF 分类子集 6,981 条：第一级剩 4,151 条（被弃用的回答 2,641、兄弟回答 139、前缀 50，其中依赖脱敏等价的 50），第二级剩 2,512 条。`--procs 6` 约 110 秒，两次运行输出完全一致。按默认口径，第二级的 2,512 条里 2,069 条留在主目录，443 条分到 `extra_tools`（`called` 167、`declared_only` 276）；其中含 MCP 额外工具的 279 条，含被调用过的 MCP 工具的 97 条，所以 `--extra-split mcp` 分出 279 条。
