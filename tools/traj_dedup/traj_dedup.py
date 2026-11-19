#!/usr/bin/env python3
"""traj_dedup: 对“请求日志式”规范化轨迹集合做确定性去重/合并。

【适用数据】
  每条记录是一次模型调用时的完整对话快照（一个 ``<session>_sub_<n>.jsonl`` 文件，一行 JSON）。
  同一段对话在日志里会留下很多快照：同一历史被重复请求、被弃用的模型回答、
  上下文压缩后重建的历史等。本脚本只依据消息序列的结构判断哪些记录多余，不看语义，
  不用随机数，不依赖遍历/完成顺序。

【输入】
  ``--src s3://<bucket>/<prefix>/`` 或本地目录。只处理 key 匹配
  ``(^|/)dt=[^/]+/[^/]+_sub_\\d+\\.jsonl$`` 的对象（本地目录用相对路径当 key，规则相同），
  其他对象（``_pipeline/``、``manifest.json`` 等）一律跳过。记录字段：``messages``、
  ``metadata.session_id``、``metadata.created_at``、``completeness.is_subagent``、
  ``normalization_audit.tag``，以及分流用的 ``extra_tool_tag``、``extra_tools``（可缺省）。S3 对象一律 get_object 读进内存、处理后丢弃，
  不向本机或服务器磁盘写任何轨迹内容。

【消息 token】
  每条消息做规范化 JSON（``json.dumps(ensure_ascii=False, sort_keys=True,
  separators=(",", ":"))``）后取 sha256：
    system/developer/user : {role, content}
    assistant             : {role, content, calls:[{name, arguments}...]}（去掉调用 id 和全部 reasoning 字段）
    tool                  : {role, name, content}（不含 tool_call_id）
  content 是 list/dict 时同样规范化；arguments 是 dict 时规范化，是字符串时原样。
  因此两条只差工具调用 id 的历史得到完全相同的 token 序列。

【脱敏等价（默认开启，--no-desens 关闭）】
  同一段内容在不同快照里可能被脱敏成不同占位符（如 <PHONE>、<IP_ADDRESS>、[REDACTED]、****），
  导致 token 不同。开启后先把“只差脱敏占位符”的 token 合并成同一个代表 token，再跑下面的级别 1、级别 2，
  规则不变（D1 保留最晚的一条，正好等于“以最后一版为准”）。
  * 占位符：默认正则 ``<[A-Z][A-Z0-9_]{1,40}>|\\[(?:USER|REDACTED|TMP|[A-Z0-9_]*_REDACTED)\\]|\\*{4,}``，
    可用 ``--placeholder-regex`` 覆盖；``[INFO]``、``[ERROR]`` 这类日志标记不算占位符。
  * 等价判定 desens_equiv(x, y)，作用在两条消息的规范化 JSON 文本上：x==y 为真；否则至少一边含占位符，
    且存在一种对齐：每个占位符替换另一边 1-256 个字符（相邻占位符合并成一段，下限、上限按个数放大），
    被替换片段里不含换行（原始换行和 JSON 转义的反斜杠加 n 都不行，片段与相邻字符拼出的转义也不行），
    其余字面文本逐字相同；两边都有占位符（位置不同）也支持，相同位置的同一个占位符按字面文本相同处理，
    不同的占位符互相不对齐。含占位符的一边，字面字符必须 >=8 且 >= 其长度的 50%。
    实现是双指针贪心：两边先逐段比较到下一个占位符，遇到占位符就在另一边最靠左的位置找
    占位符后面那段字面文本（最多试 256 个候选位置），找到就跳过去，找不到判不等价（宁可漏，不可错）。
    每个占位符只做有界搜索，总比较量有预算上限，所以是线性/近线性，不会回溯爆炸。
  * 候选：在每个桶内，同一位置上出现的不同 token 两两配对，要求同 role、至少一边含占位符、
    规范化文本的前 64 字符哈希相同或后 64 字符哈希相同、长度比在 [0.5, 2] 内。无会话记录的分桶键
    （第一条 user 消息）先做同样的全局候选（不限位置），这样只差脱敏的第一条 user 能进同一桶。
  * 核验：extract 时只为每条消息记下 role/是否含占位符/长度/头尾哈希，不保留正文。候选需要正文时，
    重新读取“含该 token 的 key 最小的记录”，只取需要的位置，核验完丢弃（分批，限制内存）。
  * 合并：核验为等价的 token 用并查集合并，代表取 sha256 摘要最小的那个，与顺序无关。
    合并不要求传递性成立，并查集会把链上的 token 全并进同一组。

【分桶】
  session_id 非空且不以 ``no_`` 开头（排除 ``no_session_id`` 等哨兵）→ 桶 ``s:<session_id>``
  （同一会话跨目录、跨模型的记录进同一桶）；否则 → ``n:<第一条 user 消息的 token(hex)>``
  （脱敏合并后用代表 token）；没有 user 消息的记录自成一桶 ``k:<key>``。去重只在桶内进行。

【级别 1 snapshot：只删能结构上证明多余的记录】
  D1 duplicate        token 序列完全相同 → 保留 created_at 最晚的（相同则 key 字典序最小），其余删，witness=保留的那条。
  D2 prefix           A 的序列是另一条 B 的严格前缀 → 删 A，witness 取 B 中序列最长的
                      （再 created_at 最晚、再 key 最小）。
  D3 abandoned_answer / sibling_answer
                      A 以 >=1 条 assistant 结尾；h = A 去掉结尾连续 assistant 后的前缀（len(h)>=1）。
                      在 D1、D2 的幸存者里找其他记录 B，满足 B[:len(h)]==h（A 此时必然不是 B 的前缀，否则已被 D2 删）：
                      * 某个 B 在位置 len(h) 之后含有非 assistant 消息（继续走了：有工具结果或新 user 消息）
                        → A 是被弃用的回答，删，规则 abandoned_answer，witness 取这类 B 里最长的
                        （再 created_at 最晚、再 key 最小）。
                      * 否则所有 B 在 h 之后都只有 assistant → A 与它们互为兄弟回答，整组只保留
                        created_at 最晚（再序列更长、再 key 最小）的一条，其余删，规则 sibling_answer，
                        witness=保留的那条。
                      D3 对 D2 的幸存者整体同时判定，结果与遍历顺序无关。
                      不会把一整棵树删空：abandoned_answer 的 witness 含的非 assistant 消息数严格多于被删记录
                      （被删记录的非 assistant 消息都在 h 内，B 在 h 之后还有至少一条），所以沿 witness 走必然
                      严格上升、不成环，终点不会再被 abandoned_answer 删除；sibling_answer 组内保留的那条
                      因为组里没有“继续走”的记录，同样不会被 D3 删除。D1 的代表、D2 的 witness（最长者
                      不可能是别人的前缀）同理。脚本在输出前会校验每条被删记录的 witness 链都终止在保留记录上。

【级别 2 conversation（默认输出级别）：在级别 1 保留结果上，同一对话只留“最后一版”】
  真实会话桶：对话键 = (session_id, "main")（is_subagent 为假）或
  (session_id, "sub", 第一条 user token)（is_subagent 为真）。
  无会话桶（n:/k:）：两条保留记录共享“第一次交换”——从开头到第一条 user 之后的第一条 assistant
  为止的 token 序列完全相同——就连起来，每个连通分量是一个对话（“相同”是等价关系，
  所以连通分量就是按该序列分组）；没有第一次交换的记录自成一个对话。
  每个对话保留 created_at 最晚的一条（再消息数多、再 key 小），其余删，规则 same_conversation，
  witness=保留的那条。

【额外工具分流（--extra-split，默认 tag）】
  有些记录带“额外工具”（声明或调用了数据集标准工具之外的工具，例如 MCP 工具）。分流只作用于去重之后的
  保留记录，按每条保留记录自身的字段判断，去重逻辑完全不变。字段：``extra_tool_tag``（none / declared_only /
  called）和 ``extra_tools``（元素键 name/server/kind/call_count/declaration，kind 为 mcp 或 unknown）。
  extra_tool_tag 缺失时退回看 extra_tools 是否非空（非空记为 called 或 declared_only，空列表记为 none）；
  两个字段都没有记为 absent（不分出去，summary 里单独计数）。
  --extra-split 取值：tag（默认：extra_tool_tag 不是 none 就分出去，声明或调用、mcp 或 unknown 都算）、
  mcp（extra_tools 里有 kind==mcp 的条目）、mcp-called（有 kind==mcp 且 call_count>0 的条目）、
  off（不分，也不建子目录）。
  输出：<out>/kept.jsonl 只放不分出去的保留记录，<out>/extra_tools/kept.jsonl 放分出去的（非 off 时即使为空也会写），
  两份的每行都有 extra_tool_tag 和 split（main/extra_tools）。--copy-to 把分出去的复制到
  <dest>/extra_tools/<相对路径>。--export-dir DIR（默认不开）把保留对象原样字节写到 DIR/<相对路径>，
  分出去的写到 DIR/extra_tools/<相对路径>：需要重新读取保留对象，逐个写完即丢弃；DIR 与本地源目录互相包含时
  拒绝；相对路径含空段、. 或 .. 时拒绝。这个选项是给使用者在自己机器上用的，脚本的测试只用内存和临时目录验证。

【用法】
  python3 traj_dedup.py --src s3://<bucket>/<prefix>/ --endpoint-url http://<s3-endpoint> --out runs/xxx
  python3 traj_dedup.py --src /path/to/local/dir --out runs/yyy --level snapshot --no-desens
  参数：--src、--endpoint-url、--profile、--workers（读对象的线程数，默认 16）、
        --procs（解析用的进程数，默认 0=线程内解析）、--level {snapshot,conversation}（决定 kept.jsonl
        和 dropped.jsonl 用哪一级，summary 两级都给）、--out、--no-desens、--placeholder-regex、
        --copy-to s3://...（可选，服务端 copy_object 复制保留记录并保持相对路径；默认不做，
        也绝不写 S3 的其他内容）、--skip-bad（读不了/解析不了的对象记入 summary.skipped 后继续，
        默认遇到就失败退出）、--mem-budget-gb，以及分流参数 --extra-split {tag,mcp,mcp-called,off}、--export-dir。
  S3 用 list_objects（v1，Marker 分页）、path-style 寻址、绕过环境里的 http_proxy；
  凭据按 boto3 默认凭据链取（环境变量或 --profile），脚本里不含任何凭据。依赖：标准库，
  读 S3 时才导入 boto3。Python >= 3.10。
  --placeholder-regex 由使用者负责不要写会灾难性回溯的表达式。

【输出（--out DIR，只含元数据：key、会话 id、时间、计数、哈希，没有消息正文；所有文件按 key 排序）】
  kept.jsonl     key、bucket、conversation、created_at、n_messages、is_subagent、audit_tag、
                 extra_tool_tag、split（main/extra_tools）、
                 merged（被它代表的输入条数，含自身，所以全部保留记录的 merged 之和等于输入条数）。
                 分流开启（--extra-split 不是 off）时只放 split 为 main 的保留记录。
  extra_tools/kept.jsonl
                 split 为 extra_tools 的保留记录，字段同 kept.jsonl（--extra-split off 时不生成）。
  dropped.jsonl  key、bucket、rule（duplicate/prefix/abandoned_answer/sibling_answer/same_conversation）、
                 level（该记录是在哪一级被删的）、witness（直接 witness）、root（最终代表它的保留记录）、
                 desens（布尔：这条删除只有在脱敏等价下才成立，即这条记录和它的直接 witness 在未合并脱敏
                 之前的原始 token 上并不满足该规则的结构关系）。
                 --level snapshot 时只含级别 1 的删除；--level conversation 时含两级，
                 kept 与 dropped 合起来恰好覆盖全部输入。
  buckets.jsonl  每个桶的输入条数、级别 1 剩余、级别 2 剩余（附加文件，便于核对大会话）。
  summary.json   输入条数、桶数、各规则删除数、各规则里经由脱敏的删除数、级别 1/2 剩余数、
                 按 normalization_audit.tag 分的剩余数、最大的 10 个桶、参数（含 desens 开关和占位符正则）、
                 脱敏统计（候选 token 对数、核验为等价的对数、被并入代表的 token 数、涉及记录数=含有任一等价组 token 的记录、
                 被改写记录数=token 序列被换成代表的记录）、
                 输入 key 列表的 sha256、脚本版本、跳过的对象；extra_split：分流口径，以及两级各自
                 main/extra_tools 条数、extra_tool_tag 分布（总体和两侧各自）、absent 数、含 mcp 额外工具的条数、
                 含被调用 mcp 额外工具的条数。

【确定性】
  所有选择都走全序比较（created_at 解析为纳秒整数后比较，缺失/无法解析视为最早；最后一级始终是 key），
  不依赖字典/集合遍历顺序、线程完成顺序和内部 token 编号；脱敏的候选集合、核验顺序（按摘要排序）、
  代表选择（最小摘要）、取正文的记录（含该 token 的 key 最小者）都是确定的。输出不含运行时间戳，
  同一输入两次运行输出逐字节相同（test_traj_dedup.py 里有打乱输入顺序的回归测试）。

【已知局限】
  * 脱敏等价只在“同一位置的 token”（以及无会话记录的第一条 user）之间找候选；位置错开的、
    占位符同时落在头 64 和尾 64 字符里的、规范化文本短于约 64 字符的消息不会成为候选。
    占位符要替换 1-256 个字符且不含换行，多行的被脱敏片段不会合并。字面字符太少（不足 8 个或不到一半）的消息不合并。
  * 客户端截断了早先工具输出的记录，token 不同，级别 1 不会合并。
  * 无会话记录的压缩链（压缩后历史重建，第一次交换不同）在级别 2 不会合并；
    真实会话桶里，同一 main/sub 键下的所有保留记录（包括压缩前后的版本）会被合成一条。
  * 级别 2 在真实会话桶里把同一 session 的全部 main 记录当成一个对话；sub 记录按第一条 user 消息分组，
    不同子代理若恰好收到相同的第一条 user 消息会被合并。
  * assistant 的 content 为 None 与 "" 视为不同（不做猜测性规范化）；arguments 字符串只原样比较。
  * created_at 缺失或无法解析的记录视为最早，不会因此出错但会优先被替换。
  * 分流按保留下来的那一条记录自身的字段判断：同一对话更早的快照里有过额外工具、但保留的最后一版没有，
    这个对话仍留在 main；反过来亦然。
"""

from __future__ import annotations

import argparse
import bisect
import concurrent.futures as cf
import functools
import hashlib
import itertools
import json
import os
import re
import sys
import threading
import time
from datetime import date

VERSION = "1.1.0"
TRAJ_RE = re.compile(r"(?:^|/)dt=[^/]+/[^/]+_sub_\d+\.jsonl$")
RULES = (
    "duplicate",
    "prefix",
    "abandoned_answer",
    "sibling_answer",
    "same_conversation",
)
NEG_INF = -(1 << 62)
_EPOCH_ORD = date(1970, 1, 1).toordinal()
_TS_RE = re.compile(
    r"^\s*(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:[.,](\d+))?\s*(Z|z|[+-]\d{2}:?\d{2})?\s*$"
)

DEFAULT_PLACEHOLDER_REGEX = (
    r"<[A-Z][A-Z0-9_]{1,40}>|\[(?:USER|REDACTED|TMP|[A-Z0-9_]*_REDACTED)\]|\*{4,}"
)
PLACEHOLDER_RE = re.compile(DEFAULT_PLACEHOLDER_REGEX)
EXTRA_SPLITS = ("tag", "mcp", "mcp-called", "off")
EXTRA_DIR = "extra_tools"  # 分流出去的保留记录所在的子目录名
MAX_FRAGMENT = 256  # 一个占位符最多替换另一边的字符数
TEXT_CHUNK_CHARS = 64_000_000  # 核验时同时放在内存里的消息文本字符数上限


# ----------------------------------------------------------------------------- 记录与 token


def parse_ts(v):
    """created_at -> 纳秒整数；缺失或无法解析返回 NEG_INF（视为最早）。"""
    if isinstance(v, bool):
        return NEG_INF
    if isinstance(v, (int, float)):
        return int(v * 1_000_000_000)
    if not isinstance(v, str):
        return NEG_INF
    m = _TS_RE.match(v)
    if not m:
        return NEG_INF
    y, mo, d, hh, mi, ss = (int(x) for x in m.groups()[:6])
    try:
        days = date(y, mo, d).toordinal() - _EPOCH_ORD
    except ValueError:
        return NEG_INF
    secs = days * 86400 + hh * 3600 + mi * 60 + ss
    tz = m.group(8)
    if tz and tz not in ("Z", "z"):
        digits = tz[1:].replace(":", "")
        off = int(digits[:2]) * 3600 + int(digits[2:]) * 60
        secs -= off if tz[0] == "+" else -off
    return secs * 1_000_000_000 + int((m.group(7) or "")[:9].ljust(9, "0"))


def _canon(v):
    return json.dumps(v, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def message_view(m):
    """进入 token 的那部分消息内容（去掉调用 id、tool_call_id、reasoning 字段）。"""
    if not isinstance(m, dict):
        return {"role": None, "content": m}
    role = m.get("role")
    if role == "assistant":
        calls = []
        for c in m.get("tool_calls") or []:
            f = (
                c.get("function")
                if isinstance(c, dict) and isinstance(c.get("function"), dict)
                else c
            )
            if isinstance(f, dict):
                calls.append({"name": f.get("name"), "arguments": f.get("arguments")})
            else:
                calls.append({"name": None, "arguments": f})
        return {"role": role, "content": m.get("content"), "calls": calls}
    if role == "tool":
        return {"role": role, "name": m.get("name"), "content": m.get("content")}
    return {"role": role, "content": m.get("content")}


def message_text(m):
    """消息的规范化 JSON 文本（token 就是它的 sha256）。"""
    return _canon(message_view(m))


def _sha(text):
    return hashlib.sha256(text.encode("utf-8", "surrogatepass")).digest()


def _h64(text):
    return int.from_bytes(
        hashlib.blake2b(text.encode("utf-8", "surrogatepass"), digest_size=8).digest(),
        "big",
    )


def _called(entry):
    n = entry.get("call_count")
    return isinstance(n, (int, float)) and not isinstance(n, bool) and n > 0


def _extra_tool_info(obj):
    """(extra_tool_tag, 是否含 mcp 额外工具, 是否含被调用过的 mcp 额外工具)。

    extra_tool_tag 缺失（或为空）时退回看 extra_tools：非空记为 called（有条目被调用过）或 declared_only，
    空列表记为 none；两个字段都没有记为 absent。"""
    tag = obj.get("extra_tool_tag")
    tools = obj.get("extra_tools")
    entries = (
        [t for t in tools if isinstance(t, dict)] if isinstance(tools, list) else []
    )
    mcp = [t for t in entries if t.get("kind") == "mcp"]
    if isinstance(tag, str) and tag:
        etag = tag
    elif isinstance(tools, list):
        if not tools:
            etag = "none"
        else:
            etag = "called" if any(_called(t) for t in entries) else "declared_only"
    else:
        etag = "absent"
    return etag, bool(mcp), any(_called(t) for t in mcp)


def split_of(r, mode):
    """保留记录归哪边：main 或 extra_tools（见文件头【额外工具分流】）。"""
    if mode == "tag":
        extra = r.extra_tag not in ("none", "absent")
    elif mode == "mcp":
        extra = r.extra_mcp
    elif mode == "mcp-called":
        extra = r.extra_mcp_called
    elif mode == "off":
        extra = False
    else:
        raise ValueError(f"unknown extra split mode: {mode!r}")
    return EXTRA_DIR if extra else "main"


def split_keys(res, level, mode):
    """级别 level 的保留记录 -> (main 的 key 列表, extra_tools 的 key 列表)，各自按 key 排序。"""
    keep = res.keep1 if level == "snapshot" else res.keep2
    main_keys, extra_keys = [], []
    for k in sorted(keep):
        (extra_keys if split_of(res.recs[k], mode) == EXTRA_DIR else main_keys).append(
            k
        )
    return main_keys, extra_keys


def extract(obj, rx=None):
    """解析后的 JSON 对象 -> 纯元数据 dict（含每条消息的 sha256 摘要列表）。可在子进程里运行。
    rx 不为 None 时，另为每条消息记下 (role, 是否含占位符, 规范化文本长度, 前 64 字符哈希, 后 64 字符哈希)，不保留正文。"""
    if not isinstance(obj, dict) or not isinstance(obj.get("messages"), list):
        raise TypeError("no messages list")
    msgs = obj["messages"]
    digests, infos = [], []
    for m in msgs:
        text = message_text(m)
        digests.append(_sha(text))
        if rx is not None:
            role = m.get("role") if isinstance(m, dict) else None
            infos.append(
                (
                    role if isinstance(role, str) else None,
                    rx.search(text) is not None,
                    len(text),
                    _h64(text[:64]),
                    _h64(text[-64:]),
                )
            )
    roles = [m.get("role") if isinstance(m, dict) else None for m in msgs]
    n = len(roles)
    tail = 0
    while tail < n and roles[n - 1 - tail] == "assistant":
        tail += 1
    u = next((i for i, r in enumerate(roles) if r == "user"), -1)
    fx_end = -1
    if u >= 0:
        fx_end = next((j for j in range(u + 1, n) if roles[j] == "assistant"), -1)
    md = obj.get("metadata") if isinstance(obj.get("metadata"), dict) else {}
    comp = obj.get("completeness") if isinstance(obj.get("completeness"), dict) else {}
    audit = (
        obj.get("normalization_audit")
        if isinstance(obj.get("normalization_audit"), dict)
        else {}
    )
    sid = md.get("session_id")
    created = md.get("created_at")
    tag = audit.get("tag")
    extra_tag, extra_mcp, extra_mcp_called = _extra_tool_info(obj)
    return {
        "extra_tag": extra_tag,
        "extra_mcp": extra_mcp,
        "extra_mcp_called": extra_mcp_called,
        "digests": digests,
        "infos": infos if rx is not None else None,
        "tail": tail,
        "last_nonasst": n - tail - 1,
        "u": u,
        "fx_end": fx_end,
        "sid": sid if isinstance(sid, str) else None,
        "created_raw": created if isinstance(created, str) else None,
        "created": parse_ts(created),
        "is_sub": bool(comp.get("is_subagent")),
        "tag": tag if isinstance(tag, str) else None,
    }


class Interner:
    """把 32 字节摘要映射成小整数（只在进程内部用，编号顺序不影响任何输出）。
    digests[i] 是编号 i 的摘要，info[i] 是它的脱敏候选信息（未开启脱敏时为 None）。"""

    def __init__(self):
        self._ids = {}
        self.digests = []
        self.info = []
        self._lock = threading.Lock()

    def ids(self, digests, infos=None):
        with self._lock:
            table = self._ids
            out = []
            for k, d in enumerate(digests):
                i = table.get(d)
                if i is None:
                    i = table[d] = len(self.digests)
                    self.digests.append(d)
                    self.info.append(infos[k] if infos else None)
                out.append(i)
        return tuple(out)


class Rec:
    __slots__ = (
        "bucket",
        "created",
        "created_raw",
        "extra_mcp",
        "extra_mcp_called",
        "extra_tag",
        "first_user",
        "fx_end",
        "fx_hash",
        "is_sub",
        "key",
        "last_nonasst",
        "n",
        "raw",
        "raw_bucket",
        "raw_first_user",
        "sid",
        "tag",
        "tail",
        "tokens",
        "u",
    )

    def __repr__(self):
        return f"Rec({self.key})"


def _derive(r, interner):
    """由当前 tokens（可能已换成脱敏代表）推出第一条 user 的 hex、第一次交换哈希和桶。"""
    d = interner.digests
    r.first_user = d[r.tokens[r.u]].hex() if r.u >= 0 else None
    r.fx_hash = (
        hashlib.sha256(b"".join(d[t] for t in r.tokens[: r.fx_end + 1])).hexdigest()[
            :16
        ]
        if r.fx_end >= 0
        else None
    )
    if r.sid and not r.sid.startswith("no_"):
        r.bucket = "s:" + r.sid
    elif r.first_user:
        r.bucket = "n:" + r.first_user
    else:
        r.bucket = "k:" + r.key


def make_record(key, ex, interner):
    r = Rec()
    r.key = key
    r.raw = r.tokens = interner.ids(ex["digests"], ex["infos"])
    r.n = len(r.tokens)
    r.sid = ex["sid"]
    r.created = ex["created"]
    r.created_raw = ex["created_raw"]
    r.is_sub = ex["is_sub"]
    r.tag = ex["tag"]
    r.extra_tag = ex["extra_tag"]
    r.extra_mcp = ex["extra_mcp"]
    r.extra_mcp_called = ex["extra_mcp_called"]
    r.tail = ex["tail"]
    r.last_nonasst = ex["last_nonasst"]
    r.u = ex["u"]
    r.fx_end = ex["fx_end"]
    _derive(r, interner)
    r.raw_bucket, r.raw_first_user = r.bucket, r.first_user
    return r


def record_from_obj(key, obj, interner, rx=None):
    return make_record(key, extract(obj, rx), interner)


def apply_rep(recs, rep, interner):
    """把所有记录的 token 序列换成脱敏代表（rep: 编号 -> 代表编号），并重新推出桶等派生量。raw 保持不变。"""
    if not rep:
        return
    for r in recs:
        new = tuple(rep.get(t, t) for t in r.raw)
        r.tokens = r.raw if new == r.raw else new
        _derive(r, interner)


# ----------------------------------------------------------------------------- 脱敏等价


def _consume(P, L, max_frag, budget):
    """P 在占位符起点，L 在字面文本上：让 P 的（一组相邻）占位符替换 L 的一段字面文本。成功则推进两边的位置。"""
    ps, pspans, _ppos, pidx = P
    ls, lspans, lpos, lidx = L
    end = pspans[pidx][1]
    m, k = 1, pidx + 1
    while k < len(pspans) and pspans[k][0] == end:  # 相邻占位符合并成一段
        end = pspans[k][1]
        m += 1
        k += 1
    lo, hi = m, max_frag * m
    nl = (
        lspans[lidx][0] if lidx < len(lspans) else len(ls)
    )  # L 的下一个占位符起点（片段不能含占位符）
    top = min(lpos + hi, nl)
    if top < lpos + lo:
        return False
    if end == len(ps):  # P 的占位符在末尾：必须吃掉 L 的全部剩余
        if nl != len(ls) or len(ls) - lpos > hi:
            return False
        e = len(ls)
    else:
        pn = pspans[k][0] if k < len(pspans) else len(ps)
        lit = ps[end:pn]  # 占位符后面的字面文本，非空
        e = None
        c = lpos + lo
        last = min(top, nl - 1)
        while c <= last:  # 最靠左的候选片段终点，最多 256*m 个位置
            c = ls.find(lit[0], c, last + 1)
            if c < 0:
                break
            n = min(len(lit), nl - c)
            budget[0] -= n + 1
            if budget[0] < 0:
                return False
            if ls.startswith(lit if n == len(lit) else lit[:n], c):
                e = c
                break
            c += 1
        if e is None:
            if top == nl and nl < len(ls):  # 片段一直延伸到 L 的下一个占位符前
                e = nl
            else:
                return False
    seg = ls[max(lpos - 1, 0) : e + 1]  # 片段加两侧各一个字符，防止拼出换行转义
    if "\n" in seg or "\\n" in seg:
        return False
    P[2], P[3] = end, k
    L[2] = e
    return True


def desens_equiv(x, y, rx=None, max_frag=MAX_FRAGMENT):
    """两条消息的规范化 JSON 文本是否只差脱敏占位符。规则和算法见文件头；找不到对齐就判不等价。"""
    if x == y:
        return True
    rx = rx or PLACEHOLDER_RE
    sides = []
    for s in (x, y):
        spans = [m.span() for m in rx.finditer(s) if m.end() > m.start()]
        if spans:
            lit = len(s) - sum(e - b for b, e in spans)
            if lit < 8 or lit * 2 < len(s):
                return False
        sides.append([s, spans, 0, 0])  # [文本, 占位符区间, 当前位置, 下一个占位符序号]
    X, Y = sides
    if not X[1] and not Y[1]:
        return False
    budget = [64 * (len(x) + len(y)) + 100000]
    while True:
        dx = (X[1][X[3]][0] if X[3] < len(X[1]) else len(X[0])) - X[2]
        dy = (Y[1][Y[3]][0] if Y[3] < len(Y[1]) else len(Y[0])) - Y[2]
        n = min(dx, dy)
        if n > 0:  # 两边都是字面文本：逐字相同
            if X[0][X[2] : X[2] + n] != Y[0][Y[2] : Y[2] + n]:
                return False
            X[2] += n
            Y[2] += n
            continue
        xend, yend = X[2] == len(X[0]), Y[2] == len(Y[0])
        if xend and yend:
            return True
        if xend or yend:
            return False
        if dx == 0 and dy == 0:  # 两边同时是占位符：必须是同一个占位符
            bx, ex = X[1][X[3]]
            by, ey = Y[1][Y[3]]
            if X[0][bx:ex] != Y[0][by:ey]:
                return False
            X[2], X[3], Y[2], Y[3] = ex, X[3] + 1, ey, Y[3] + 1
            continue
        P, L = (X, Y) if dx == 0 else (Y, X)
        if not _consume(P, L, max_frag, budget):
            return False


def candidate_pairs(tokens, info):
    """tokens: 一组不同的 token 编号 -> {(a, b)} 候选对（无序，a<b）：同 role、至少一边含占位符、
    头哈希或尾哈希相同、长度比在 [0.5, 2] 内。"""
    tokens = list(tokens)
    ph = [t for t in tokens if info[t][1]]
    if not ph:
        return set()
    by_head, by_tail = {}, {}
    for t in tokens:
        by_head.setdefault(info[t][3], []).append(t)
        by_tail.setdefault(info[t][4], []).append(t)
    out = set()
    for t in ph:
        role, _, ln, hh, th = info[t]
        for u in itertools.chain(by_head[hh], by_tail[th]):
            if u == t or info[u][0] != role:
                continue
            lu = info[u][2]
            if ln <= 2 * lu and lu <= 2 * ln:
                out.add((t, u) if t < u else (u, t))
    return out


class _Equiv:
    """候选 token 对的核验与并查集（代表 = 组内摘要最小的 token）。"""

    def __init__(self, recs, interner, fetcher, sizes, rx, workers, text_chars, log):
        self.by_key = sorted(recs, key=lambda r: r.key)
        self.interner, self.fetcher, self.sizes, self.rx = interner, fetcher, sizes, rx
        self.workers, self.text_chars, self.log = workers, text_chars, log
        self.parent, self.verified, self.candidates = {}, {}, set()

    def find(self, t):
        root = t
        while root in self.parent:
            root = self.parent[root]
        while t in self.parent and self.parent[t] != root:
            nxt = self.parent[t]
            self.parent[t] = root
            t = nxt
        return root

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        dg = self.interner.digests
        keep, other = (ra, rb) if dg[ra] < dg[rb] else (rb, ra)
        self.parent[other] = keep

    def _texts(self, toks, loc):
        wanted = {}
        for t in toks:
            k, p = loc[t]
            wanted.setdefault(k, {})[p] = t

        def work(k):
            return k, self.fetcher.run(
                k, self.sizes.get(k, 0), _proc_texts, sorted(wanted[k])
            )

        out = {}
        with cf.ThreadPoolExecutor(self.workers) as tp:
            for k, texts in tp.map(work, sorted(wanted)):
                for p, text in texts.items():
                    t = wanted[k][p]
                    if _sha(text) != self.interner.digests[t]:
                        raise RuntimeError("source object changed while running: " + k)
                    out[t] = text
        return out

    def process(self, pairs):
        self.candidates |= pairs
        new = [p for p in pairs if p not in self.verified]
        if not new:
            return
        dg, info = self.interner.digests, self.interner.info
        new.sort(key=lambda p: tuple(sorted((dg[p[0]], dg[p[1]]))))
        need = {t for p in new for t in p}
        loc = {}  # token -> (含它的 key 最小的记录, 位置)
        for r in self.by_key:
            for pos, t in enumerate(r.raw):
                if t in need and t not in loc:
                    loc[t] = (r.key, pos)
            if len(loc) == len(need):
                break
        chunk, toks, size = [], set(), 0

        def flush():
            texts = self._texts(toks, loc)
            for a, b in chunk:
                x, y = (a, b) if dg[a] < dg[b] else (b, a)
                ok = desens_equiv(texts[x], texts[y], self.rx)
                self.verified[(a, b)] = ok
                if ok:
                    self.union(a, b)

        for p in new:
            add = sum(info[t][2] for t in p if t not in toks)
            if chunk and size + add > self.text_chars:
                flush()
                chunk, toks, size = [], set(), 0
                add = sum(info[t][2] for t in p)
            chunk.append(p)
            toks.update(p)
            size += add
        flush()


def resolve_desens(
    recs,
    interner,
    fetcher,
    sizes,
    rx,
    workers=16,
    text_chars=TEXT_CHUNK_CHARS,
    log=None,
):
    """返回 (rep, stats)。rep: 被并入代表的 token 编号 -> 代表编号。不修改 recs。"""
    eq = _Equiv(recs, interner, fetcher, sizes, rx, workers, text_chars, log)
    info = interner.info
    # 第一步：无会话记录的第一条 user token（全局、不限位置）
    firsts = {r.raw[r.u] for r in recs if r.raw_bucket.startswith("n:")}
    eq.process(candidate_pairs(firsts, info))
    if log:
        log(
            f"  desens stage 1 (first user): {len(eq.candidates)} candidates, "
            f"{sum(eq.verified.values())} equivalent"
        )
    # 第二步：按（合并第一条 user 之后的）桶，同一位置上不同的 token
    groups = {}
    for r in recs:
        if r.raw_bucket.startswith("n:"):
            groups.setdefault(("n", eq.find(r.raw[r.u])), []).append(r)
        elif r.raw_bucket.startswith("s:"):
            groups.setdefault(r.raw_bucket, []).append(r)
    pairs = set()
    for g in groups.values():
        if len(g) < 2:
            continue
        cols = [set() for _ in range(max(r.n for r in g))]
        for r in g:
            for p, t in enumerate(r.raw):
                cols[p].add(t)
        for col in cols:
            if len(col) > 1:
                pairs |= candidate_pairs(col, info)
    eq.process(pairs)
    rep = {}
    for t in list(eq.parent):
        rep[t] = eq.find(t)
    if log:
        log(
            f"  desens stage 2 (same position): {len(eq.candidates)} candidates, "
            f"{sum(eq.verified.values())} equivalent, {len(rep)} tokens merged"
        )
    return rep, {
        "candidate_pairs": len(eq.candidates),
        "equivalent_pairs": sum(eq.verified.values()),
        "tokens_merged": len(rep),
    }


# ----------------------------------------------------------------------------- 去重核心


def _k_dup(r):  # D1 代表：created_at 最晚，再 key 最小
    return (-r.created, r.key)


def _k_longest(r):  # D2 / abandoned witness：序列最长，再 created_at 最晚，再 key 最小
    return (-r.n, -r.created, r.key)


def _k_latest(
    r,
):  # sibling 保留 / 级别 2 保留：created_at 最晚，再序列更长，再 key 最小
    return (-r.created, -r.n, r.key)


def level1_bucket(recs):
    """返回 (幸存记录列表, [(key, rule, witness_key)...])。"""
    drops = []
    # D1
    groups = {}
    for r in recs:
        groups.setdefault(r.tokens, []).append(r)
    uniq = []
    for g in groups.values():
        rep = min(g, key=_k_dup)
        drops.extend((r.key, "duplicate", rep.key) for r in g if r is not rep)
        uniq.append(rep)
    # 按 token 序列字典序排序后，以某序列为前缀的所有序列在排序结果里是紧跟其后的一段连续区间。
    uniq.sort(key=lambda r: r.tokens)
    toks = [r.tokens for r in uniq]
    # D2
    s2 = []
    for i, a in enumerate(uniq):
        la = len(a.tokens)
        if i + 1 < len(uniq) and toks[i + 1][:la] == a.tokens:
            hi = bisect.bisect_right(toks, a.tokens, lo=i + 1, key=lambda t: t[:la])
            w = min(uniq[i + 1 : hi], key=_k_longest)
            drops.append((a.key, "prefix", w.key))
        else:
            s2.append(a)
    # D3：在 D2 幸存者 s2 上整体判定（不用顺序结果）
    toks2 = [r.tokens for r in s2]
    out = []
    for a in s2:
        lh = a.n - a.tail
        if a.tail < 1 or lh < 1:
            out.append(a)
            continue
        h = a.tokens[:lh]
        lo = bisect.bisect_left(toks2, h)
        hi = bisect.bisect_right(toks2, h, lo=lo, key=lambda t: t[:lh])
        block = s2[lo:hi]  # 含 a 自己；h 本身是 a 的严格前缀，已被 D2 删，不在其中
        cont = [b for b in block if b.last_nonasst >= lh]  # h 之后含非 assistant 消息
        if cont:
            drops.append((a.key, "abandoned_answer", min(cont, key=_k_longest).key))
            continue
        best = min(block, key=_k_latest)  # 此时整个 block 都是 h+纯 assistant，互为兄弟
        if best is a:
            out.append(a)
        else:
            drops.append((a.key, "sibling_answer", best.key))
    return out, drops


def conv_groups(bucket, recs):
    """级别 2：返回 [(conversation 标签, [记录...])]。"""
    groups = {}
    if bucket.startswith("s:"):
        for r in recs:
            k = ("sub", r.first_user or "none") if r.is_sub else ("main",)
            groups.setdefault(k, []).append(r)
        return [(bucket + "/" + "/".join(k), g) for k, g in groups.items()]
    if bucket.startswith("n:"):
        solo = []
        for r in recs:
            if r.fx_end < 0:
                solo.append((bucket + "/solo:" + r.key, [r]))
            else:
                groups.setdefault(r.tokens[: r.fx_end + 1], []).append(r)
        return [(bucket + "/" + g[0].fx_hash, g) for g in groups.values()] + solo
    return [(bucket + "/solo", [r]) for r in recs]


def _resolve(parent, k, memo):
    path = []
    while k in parent and k not in memo:
        path.append(k)
        k = parent[k][1]
        if len(path) > len(parent) + 1:
            raise RuntimeError("witness cycle")
    root = memo.get(k, k)
    for p in path:
        memo[p] = root
    return root


def _via_desens(rule, a, w):
    """这条删除是否只有在脱敏合并之后才成立：在原始（合并前）token 上检查 a 与直接 witness w 的结构关系。"""
    if a.raw_bucket != w.raw_bucket:
        return True
    if rule == "duplicate":
        return a.raw != w.raw
    if rule == "prefix":
        return w.raw[: a.n] != a.raw
    if rule in ("abandoned_answer", "sibling_answer"):
        lh = a.n - a.tail
        return a.raw[:lh] != w.raw[:lh]
    if rule == "same_conversation":
        if a.bucket.startswith("s:"):
            return a.is_sub and a.raw_first_user != w.raw_first_user
        if a.bucket.startswith("n:"):
            return a.raw[: a.fx_end + 1] != w.raw[: w.fx_end + 1]
    return False


class Result:
    pass


def dedup(recs):
    keys = [r.key for r in recs]
    if len(set(keys)) != len(keys):
        raise ValueError("duplicate keys in input")
    buckets = {}
    for r in recs:
        buckets.setdefault(r.bucket, []).append(r)
    res = Result()
    res.recs = {r.key: r for r in recs}
    res.drops1 = {}  # key -> (rule, witness)
    res.drops2 = {}
    res.conv = {}  # L1 幸存记录 key -> conversation 标签
    res.keep1, res.keep2 = set(), set()
    res.bucket_counts = {}  # bucket -> [n_input, n_l1, n_l2]
    for b in sorted(buckets):
        surv, drops = level1_bucket(buckets[b])
        for k, rule, w in drops:
            res.drops1[k] = (rule, w)
        res.keep1.update(r.key for r in surv)
        n2 = 0
        for label, g in conv_groups(b, surv):
            keep = min(g, key=_k_latest)
            res.keep2.add(keep.key)
            n2 += 1
            for r in g:
                res.conv[r.key] = label
                if r is not keep:
                    res.drops2[r.key] = ("same_conversation", keep.key)
        res.bucket_counts[b] = [len(buckets[b]), len(surv), n2]
    # 校验 + 根节点解析 + merged 计数
    memo1, memo2 = {}, {}
    res.root1, res.root2 = {}, {}
    res.merged1, res.merged2 = {}, {}
    for k in keys:
        r1 = _resolve(res.drops1, k, memo1)
        if r1 not in res.keep1:
            raise RuntimeError("level-1 witness chain does not end in a kept record")
        r2 = _resolve(res.drops2, r1, memo2)
        if r2 not in res.keep2:
            raise RuntimeError("level-2 witness chain does not end in a kept record")
        res.root1[k], res.root2[k] = r1, r2
        res.merged1[r1] = res.merged1.get(r1, 0) + 1
        res.merged2[r2] = res.merged2.get(r2, 0) + 1
    if sum(res.merged1.values()) != len(keys) or sum(res.merged2.values()) != len(keys):
        raise RuntimeError("merged counts do not add up")
    res.via = {}  # 被删记录 key -> 是否只有脱敏等价下才成立
    for drops in (res.drops1, res.drops2):
        for k, (rule, w) in drops.items():
            res.via[k] = _via_desens(rule, res.recs[k], res.recs[w])
    return res


# ----------------------------------------------------------------------------- 输出


def _line(row):
    return json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n"


def _tag_name(t):
    return t if t is not None else "(none)"


def _split_stats(res, keys, mode):
    """一级的保留记录按分流口径统计：两侧条数、extra_tool_tag 分布（总体和两侧各自）、absent 数、含 mcp 的条数。"""
    st = {
        "main": 0,
        EXTRA_DIR: 0,
        "absent": 0,
        "with_mcp": 0,
        "with_mcp_called": 0,
        "tags": {},
        "main_tags": {},
        f"{EXTRA_DIR}_tags": {},
    }
    for k in keys:
        r = res.recs[k]
        side = split_of(r, mode)
        st[side] += 1
        st["absent"] += r.extra_tag == "absent"
        st["with_mcp"] += r.extra_mcp
        st["with_mcp_called"] += r.extra_mcp_called
        for d in (st["tags"], st[f"{side}_tags"]):
            d[r.extra_tag] = d.get(r.extra_tag, 0) + 1
    return st


def render(
    res,
    level,
    input_keys,
    params=None,
    skipped=None,
    desens_stats=None,
    extra_split="tag",
):
    """返回 {文件名: 文本}，全部按 key 排序，不含时间戳。分流开启时多一个 extra_tools/kept.jsonl。"""
    if extra_split not in EXTRA_SPLITS:
        raise ValueError(f"unknown extra split mode: {extra_split!r}")
    snap = level == "snapshot"
    keep = res.keep1 if snap else res.keep2
    merged = res.merged1 if snap else res.merged2
    rows = {"main": [], EXTRA_DIR: []}
    for k in sorted(keep):
        r = res.recs[k]
        side = split_of(r, extra_split)
        rows[side].append(
            _line(
                {
                    "key": k,
                    "bucket": r.bucket,
                    "conversation": res.conv[k],
                    "created_at": r.created_raw,
                    "n_messages": r.n,
                    "is_subagent": r.is_sub,
                    "audit_tag": r.tag,
                    "extra_tool_tag": r.extra_tag,
                    "split": side,
                    "merged": merged[k],
                }
            )
        )
    kept_txt = "".join(rows["main"])
    drows = []
    for k in sorted(res.drops1):
        rule, w = res.drops1[k]
        drows.append(
            (
                k,
                {
                    "key": k,
                    "bucket": res.recs[k].bucket,
                    "rule": rule,
                    "level": 1,
                    "witness": w,
                    "root": res.root1[k] if snap else res.root2[k],
                    "desens": res.via[k],
                },
            )
        )
    if not snap:
        for k in res.drops2:
            rule, w = res.drops2[k]
            drows.append(
                (
                    k,
                    {
                        "key": k,
                        "bucket": res.recs[k].bucket,
                        "rule": rule,
                        "level": 2,
                        "witness": w,
                        "root": res.root2[k],
                        "desens": res.via[k],
                    },
                )
            )
    drows.sort(key=lambda t: t[0])
    dropped_txt = "".join(_line(row) for _, row in drows)
    bucket_txt = "".join(
        _line(
            {"bucket": b, "n_input": c[0], "n_snapshot": c[1], "n_conversation": c[2]}
        )
        for b, c in sorted(res.bucket_counts.items())
    )
    by_rule = {r: 0 for r in RULES}
    via_rule = {r: 0 for r in RULES}
    for drops in (res.drops1, res.drops2):
        for k, (rule, _) in drops.items():
            by_rule[rule] += 1
            if res.via[k]:
                via_rule[rule] += 1

    def tags(keys):
        d = {}
        for k in keys:
            t = _tag_name(res.recs[k].tag)
            d[t] = d.get(t, 0) + 1
        return d

    ds = dict(
        {
            "enabled": False,
            "candidate_pairs": 0,
            "equivalent_pairs": 0,
            "tokens_merged": 0,
            "records_affected": 0,
            "records_rewritten": 0,
        },
        **(desens_stats or {}),
    )
    largest = sorted(res.bucket_counts.items(), key=lambda kv: (-kv[1][0], kv[0]))[:10]
    summary = {
        "script_version": VERSION,
        "params": dict(params or {}, level=level, extra_split=extra_split),
        "extra_split": {
            "mode": extra_split,
            "snapshot": _split_stats(res, res.keep1, extra_split),
            "conversation": _split_stats(res, res.keep2, extra_split),
        },
        "input_keys_sha256": hashlib.sha256(
            "\n".join(sorted(input_keys)).encode("utf-8")
        ).hexdigest(),
        "n_input": len(res.recs),
        "n_skipped": len(skipped or []),
        "skipped": sorted(skipped or [], key=lambda x: x["key"]),
        "n_buckets": len(res.bucket_counts),
        "n_buckets_session": sum(1 for b in res.bucket_counts if b.startswith("s:")),
        "dropped_by_rule": by_rule,
        "dropped_via_desens_by_rule": via_rule,
        "dropped_via_desens_total": sum(via_rule.values()),
        "desens": ds,
        "level1_snapshot_remaining": len(res.keep1),
        "level2_conversation_remaining": len(res.keep2),
        "remaining_by_audit_tag": {
            "snapshot": tags(res.keep1),
            "conversation": tags(res.keep2),
        },
        "largest_buckets": [
            {"bucket": b, "n_input": c[0], "n_snapshot": c[1], "n_conversation": c[2]}
            for b, c in largest
        ],
        "witness_chains_ok": True,
    }
    summary_txt = (
        json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    )
    files = {
        "kept.jsonl": kept_txt,
        "dropped.jsonl": dropped_txt,
        "buckets.jsonl": bucket_txt,
        "summary.json": summary_txt,
    }
    if extra_split != "off":
        files[f"{EXTRA_DIR}/kept.jsonl"] = "".join(rows[EXTRA_DIR])
    return files


def write_outputs(out_dir, files):
    os.makedirs(out_dir, exist_ok=True)
    for name, txt in files.items():
        path = os.path.join(out_dir, *name.split("/"))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            f.write(txt)


# ----------------------------------------------------------------------------- 输入源


class LocalSource:
    def __init__(self, root):
        self.root = root

    def list(self):
        out = []
        for dp, dns, fns in os.walk(self.root):
            dns.sort()
            for fn in sorted(fns):
                p = os.path.join(dp, fn)
                rel = os.path.relpath(p, self.root).replace(os.sep, "/")
                if TRAJ_RE.search(rel):
                    out.append((rel, os.path.getsize(p)))
        return sorted(out)

    def read(self, key):
        with open(os.path.join(self.root, key), "rb") as f:
            return f.read()

    def rel(self, key):
        return key


class MemorySource:
    """内存里的 {key: JSON 对象}（测试用）。"""

    def __init__(self, objs):
        self.blobs = {k: json.dumps(o).encode("utf-8") for k, o in objs.items()}

    def list(self):
        return sorted((k, len(b)) for k, b in self.blobs.items() if TRAJ_RE.search(k))

    def read(self, key):
        return self.blobs[key]

    def rel(self, key):
        return key


class S3Source:
    def __init__(self, client, bucket, prefix):
        self.client, self.bucket, self.prefix = client, bucket, prefix

    def list(self):
        out, marker = [], ""
        while True:  # 老版本 S3 网关：list_objects v1 + Marker 分页
            r = self.client.list_objects(
                Bucket=self.bucket, Prefix=self.prefix, Marker=marker, MaxKeys=1000
            )
            objs = r.get("Contents", [])
            out += [(o["Key"], o["Size"]) for o in objs if TRAJ_RE.search(o["Key"])]
            if not r.get("IsTruncated") or not objs:
                break
            marker = r.get("NextMarker") or objs[-1]["Key"]
        return sorted(out)

    def read(self, key):
        last = None
        for attempt in range(4):
            try:
                return self.client.get_object(Bucket=self.bucket, Key=key)[
                    "Body"
                ].read()
            # 网络/SDK 抛出的异常种类很多，任何失败都重试；重试用完后原样向上抛
            except Exception as e:  # noqa: BLE001
                last = e
                time.sleep(1 + 2 * attempt)
        raise last

    def rel(self, key):
        return key.removeprefix(self.prefix)


def make_s3_client(endpoint_url, profile, workers):
    import boto3
    from botocore.config import Config

    session = (
        boto3.session.Session(profile_name=profile)
        if profile
        else boto3.session.Session()
    )
    cfg = Config(
        proxies={},
        s3={"addressing_style": "path"},
        retries={"max_attempts": 5},
        connect_timeout=10,
        read_timeout=120,
        max_pool_connections=max(workers, 10),
    )
    return session.client("s3", endpoint_url=endpoint_url, config=cfg)


def parse_s3_url(url):
    m = re.match(r"^s3://([^/]+)/?(.*)$", url)
    if not m:
        raise ValueError(f"bad s3 url: {url}")
    return m.group(1), m.group(2)


class ByteBudget:
    """限制同时在内存里处理的对象总量（估算值），避免几个超大对象同时解析撑爆内存。"""

    def __init__(self, total):
        self.total, self.used, self.cv = total, 0, threading.Condition()

    def acquire(self, n):
        n = min(n, self.total)
        with self.cv:
            while self.used > 0 and self.used + n > self.total:
                self.cv.wait()
            self.used += n
        return n

    def release(self, n):
        with self.cv:
            self.used -= n
            self.cv.notify_all()


@functools.lru_cache(maxsize=8)
def _compiled(pattern):
    return re.compile(pattern)


def _proc_extract(blob, pattern):
    return extract(json.loads(blob), _compiled(pattern) if pattern else None)


def _proc_texts(blob, positions):
    """重新读取记录，只取指定位置的消息规范化文本（核验脱敏候选用）。"""
    msgs = json.loads(blob)["messages"]
    return {p: message_text(msgs[p]) for p in positions}


class Fetcher:
    """读对象并解析；可选地把解析放到进程池（读取和内存预算仍在线程里）。"""

    def __init__(self, src, procs=0, budget_bytes=6 << 30):
        self.src, self.budget, self.pool = src, ByteBudget(budget_bytes), None
        if procs:
            import multiprocessing

            self.pool = cf.ProcessPoolExecutor(
                procs, mp_context=multiprocessing.get_context("forkserver")
            )

    def run(self, key, size, fn, *args):
        w = self.budget.acquire(size * 8 + (1 << 20))
        try:
            blob = self.src.read(key)
            return (
                self.pool.submit(fn, blob, *args).result()
                if self.pool
                else fn(blob, *args)
            )
        finally:
            self.budget.release(w)

    def close(self):
        if self.pool:
            self.pool.shutdown()


def load_all(fetcher, items, interner, pattern=None, workers=16, log=None):
    """items=[(key, size)] -> (recs, errors{key: 错误类名})。读完即丢弃对象正文。"""

    def work(item):
        key, size = item
        try:
            return (
                key,
                make_record(
                    key, fetcher.run(key, size, _proc_extract, pattern), interner
                ),
                None,
            )
        # 单个对象读不了/解析不了不能中断整批：只记录异常类名（避免把内容带进日志），由调用方决定是否失败
        except Exception as e:  # noqa: BLE001
            return key, None, type(e).__name__

    recs, errors, done, t0 = [], {}, 0, time.time()
    order = sorted(
        items, key=lambda kv: (-kv[1], kv[0])
    )  # 大对象先读，尾部更均匀（不影响结果）
    with cf.ThreadPoolExecutor(workers) as tp:
        for key, rec, err in tp.map(work, order):
            done += 1
            if err:
                errors[key] = err
            else:
                recs.append(rec)
            if log and done % 500 == 0:
                log(f"  loaded {done}/{len(order)} ({time.time() - t0:.0f}s)")
    return recs, errors


def run_pipeline(
    src,
    items=None,
    desens=True,
    placeholder_regex=DEFAULT_PLACEHOLDER_REGEX,
    workers=16,
    procs=0,
    budget_bytes=6 << 30,
    text_chars=TEXT_CHUNK_CHARS,
    skip_bad=False,
    log=None,
):
    """读入 -> （脱敏合并）-> 去重。返回 (res, errors, desens_stats)；有读取错误且不允许跳过时 res 为 None。"""
    items = src.list() if items is None else items
    rx = re.compile(placeholder_regex) if desens else None
    interner = Interner()
    fetcher = Fetcher(src, procs, budget_bytes)
    try:
        recs, errors = load_all(
            fetcher,
            items,
            interner,
            placeholder_regex if desens else None,
            workers,
            log,
        )
        if errors and not skip_bad:
            return None, errors, None
        stats = {
            "enabled": bool(desens),
            "candidate_pairs": 0,
            "equivalent_pairs": 0,
            "tokens_merged": 0,
            "records_affected": 0,
            "records_rewritten": 0,
        }
        if desens and recs:
            rep, st = resolve_desens(
                recs, interner, fetcher, dict(items), rx, workers, text_chars, log
            )
            apply_rep(recs, rep, interner)
            stats.update(st)
            involved = set(rep) | set(
                rep.values()
            )  # 属于某个多 token 等价组的 token（含代表）
            stats["records_affected"] = sum(
                1 for r in recs if any(t in involved for t in r.raw)
            )
            stats["records_rewritten"] = sum(1 for r in recs if r.tokens is not r.raw)
    finally:
        fetcher.close()
    return dedup(recs), errors, stats


def copy_kept(client, src_bucket, src_prefix, dest_url, keys, workers, extra_keys=()):
    """服务端复制保留对象：keys -> <dest>/<相对路径>，extra_keys（分流出去的）-> <dest>/extra_tools/<相对路径>。"""
    dbucket, dprefix = parse_s3_url(dest_url)
    if dprefix and not dprefix.endswith("/"):
        dprefix += "/"
    if dbucket == src_bucket and (
        dprefix.startswith(src_prefix) or src_prefix.startswith(dprefix)
    ):
        raise SystemExit("--copy-to overlaps the source prefix; refusing")
    jobs = [(k, dprefix + k[len(src_prefix) :]) for k in keys]
    jobs += [(k, f"{dprefix}{EXTRA_DIR}/{k[len(src_prefix) :]}") for k in extra_keys]

    def one(job):
        k, dest_key = job
        client.copy_object(
            Bucket=dbucket,
            Key=dest_key,
            CopySource={"Bucket": src_bucket, "Key": k},
        )

    with cf.ThreadPoolExecutor(workers) as tp:
        list(tp.map(one, jobs))


def _rel_parts(rel):
    parts = rel.split("/")
    if not rel or any(p in ("", ".", "..") for p in parts):
        raise ValueError(f"unsafe relative path: {rel!r}")
    return parts


def check_export_dir(src, export_dir):
    """本地源目录与导出目录互相包含时拒绝，免得写坏源数据。"""
    src_root = getattr(src, "root", None)
    if src_root is None:
        return
    dst, srcr = os.path.realpath(export_dir), os.path.realpath(src_root)
    if dst == srcr or dst.startswith(srcr + os.sep) or srcr.startswith(dst + os.sep):
        raise SystemExit("--export-dir overlaps the source directory; refusing")


def export_kept(
    src,
    export_dir,
    main_keys,
    extra_keys=(),
    sizes=None,
    workers=16,
    budget_bytes=2 << 30,
):
    """把保留对象原样字节写到 export_dir/<相对路径>，分流出去的写到 export_dir/extra_tools/<相对路径>。
    重新读取对象，逐个写完即丢弃；先写 .part 再改名，不留半个文件。返回 (写出的文件数, 字节数)。"""
    check_export_dir(src, export_dir)
    base = os.path.realpath(export_dir)
    jobs = []
    for sub, keys in (((), main_keys), ((EXTRA_DIR,), extra_keys)):
        for k in keys:  # 先把所有路径校验完，再开始写
            jobs.append((k, os.path.join(base, *sub, *_rel_parts(src.rel(k)))))
    budget, sizes = ByteBudget(budget_bytes), sizes or {}

    def one(job):
        key, path = job
        w = budget.acquire(sizes.get(key, 0) + (1 << 20))
        try:
            blob = src.read(key)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path + ".part", "wb") as f:
                f.write(blob)
            os.replace(path + ".part", path)
            return len(blob)
        finally:
            budget.release(w)

    with cf.ThreadPoolExecutor(workers) as tp:
        total = sum(tp.map(one, jobs))
    return len(jobs), total


# ----------------------------------------------------------------------------- 命令行


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="请求日志式轨迹去重/合并（详见文件头说明）"
    )
    ap.add_argument("--src", required=True, help="s3://<bucket>/<prefix>/ 或本地目录")
    ap.add_argument(
        "--endpoint-url", default=None, help="S3 端点，例如 http://<s3-endpoint>"
    )
    ap.add_argument("--profile", default=None)
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--procs", type=int, default=0, help="解析用进程数，0=线程内解析")
    ap.add_argument(
        "--level", choices=("snapshot", "conversation"), default="conversation"
    )
    ap.add_argument("--out", required=True)
    ap.add_argument(
        "--no-desens", action="store_true", help="关闭脱敏占位符等价合并（默认开启）"
    )
    ap.add_argument(
        "--placeholder-regex",
        default=DEFAULT_PLACEHOLDER_REGEX,
        help="占位符正则（覆盖默认值）",
    )
    ap.add_argument(
        "--copy-to",
        default=None,
        help="s3://<bucket>/<prefix>/ ：服务端复制保留记录（默认不做）",
    )
    ap.add_argument(
        "--extra-split",
        choices=EXTRA_SPLITS,
        default="tag",
        help="带额外工具的保留记录分到 <out>/extra_tools/kept.jsonl："
        "tag=extra_tool_tag 不是 none（默认）；mcp=含 mcp 额外工具；"
        "mcp-called=含被调用过的 mcp 额外工具；off=不分",
    )
    ap.add_argument(
        "--export-dir",
        default=None,
        help="把保留对象原样字节写到 DIR/<相对路径>，分流出去的写到 DIR/extra_tools/<相对路径>（默认不做）",
    )
    ap.add_argument("--skip-bad", action="store_true")
    ap.add_argument("--mem-budget-gb", type=float, default=6.0)
    args = ap.parse_args(argv)
    log = lambda s: print(s, file=sys.stderr, flush=True)
    t0 = time.time()

    client = None
    if args.src.startswith("s3://"):
        bucket, prefix = parse_s3_url(args.src)
        client = make_s3_client(args.endpoint_url, args.profile, args.workers)
        src = S3Source(client, bucket, prefix)
    else:
        if args.copy_to:
            raise SystemExit("--copy-to needs an s3:// source")
        src = LocalSource(args.src)
    if args.export_dir:
        check_export_dir(src, args.export_dir)
    items = src.list()
    log(f"listed {len(items)} trajectory objects ({time.time() - t0:.0f}s)")
    desens = not args.no_desens
    res, errors, stats = run_pipeline(
        src,
        items,
        desens,
        args.placeholder_regex,
        args.workers,
        args.procs,
        int(args.mem_budget_gb * (1 << 30)),
        skip_bad=args.skip_bad,
        log=log,
    )
    if res is None:
        log(
            f"FAILED to read/parse {len(errors)} objects; "
            f"first: {sorted(errors.items())[:3]}"
        )
        return 2
    log(f"pipeline finished ({time.time() - t0:.0f}s)")
    skipped = [{"key": k, "error": e} for k, e in sorted(errors.items())]
    params = {
        "src": args.src,
        "desens": desens,
        "placeholder_regex": args.placeholder_regex if desens else None,
    }
    files = render(
        res,
        args.level,
        [k for k, _ in items],
        params,
        skipped,
        stats,
        args.extra_split,
    )
    write_outputs(args.out, files)
    s = json.loads(files["summary.json"])
    log(
        f"done in {time.time() - t0:.0f}s: input {s['n_input']}, buckets {s['n_buckets']}, "
        f"dropped {s['dropped_by_rule']}, via desens {s['dropped_via_desens_total']}, "
        f"level1 {s['level1_snapshot_remaining']}, level2 {s['level2_conversation_remaining']}"
    )
    main_keys, extra_keys = split_keys(res, args.level, args.extra_split)
    log(
        f"extra split ({args.extra_split}): main {len(main_keys)}, "
        f"{EXTRA_DIR} {len(extra_keys)}"
    )
    if args.copy_to:
        copy_kept(
            client,
            src.bucket,
            src.prefix,
            args.copy_to,
            main_keys,
            args.workers,
            extra_keys,
        )
        log(f"copied {len(main_keys) + len(extra_keys)} kept objects")
    if args.export_dir:
        n_files, n_bytes = export_kept(
            src,
            args.export_dir,
            main_keys,
            extra_keys,
            dict(items),
            args.workers,
            int(args.mem_budget_gb * (1 << 30)),
        )
        log(f"exported {n_files} kept objects ({n_bytes} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
