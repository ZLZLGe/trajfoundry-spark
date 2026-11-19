"""traj_dedup 的单元测试（合成记录，不含任何真实轨迹内容）。运行：python3 -m unittest -v test_traj_dedup"""

import json
import os
import random
import tempfile
import unittest

import traj_dedup as td


def U(t):
    return {"role": "user", "content": t}


def SYS(t="sys"):
    return {"role": "system", "content": t}


def T(t, cid="c1", name="bash"):
    return {"role": "tool", "tool_call_id": cid, "name": name, "content": t}


def A(t, calls=None, rc=None):
    m = {"role": "assistant", "content": t}
    if rc is not None:
        m["reasoning_content"] = rc
    if calls:
        m["tool_calls"] = [
            {"id": cid, "type": "function", "function": {"name": n, "arguments": a}}
            for cid, n, a in calls
        ]
    return m


def rec(sid, ts, msgs, sub=False, tag="clean"):
    return {
        "messages": msgs,
        "metadata": {"session_id": sid, "created_at": ts},
        "completeness": {"is_subagent": sub},
        "normalization_audit": {"tag": tag},
    }


def build(specs, interner=None):
    """specs: [(key, obj)] -> Rec 列表。"""
    it = interner or td.Interner()
    return [td.record_from_obj(k, o, it) for k, o in specs]


def run(specs, level="conversation"):
    recs = build(specs)
    res = td.dedup(recs)
    return res


def rules(res):
    d = {k: (v[0], v[1]) for k, v in res.drops1.items()}
    d.update({k: v for k, v in res.drops2.items()})
    return d


T0 = "2026-08-21T10:00:00.000Z"
T1 = "2026-08-21T10:00:01.000Z"
T2 = "2026-08-21T10:00:02.000Z"
H = [SYS(), U("q1"), A("", [("call_a", "bash", '{"cmd":"ls"}')]), T("out1", "call_a")]


class TokenTests(unittest.TestCase):
    def test_ids_and_reasoning_ignored(self):
        a = [
            SYS(),
            U("q"),
            A("x", [("call_1", "bash", {"b": 1, "a": 2})], rc="think1"),
            T("r", "call_1"),
        ]
        b = [
            SYS(),
            U("q"),
            A("x", [("call_ZZ", "bash", {"a": 2, "b": 1})], rc="other"),
            T("r", "call_QQ"),
        ]
        it = td.Interner()
        ra, rb = build([("a", rec("s1", T0, a)), ("b", rec("s1", T1, b))], it)
        self.assertEqual(ra.tokens, rb.tokens)

    def test_string_arguments_verbatim(self):
        it = td.Interner()
        ra, rb = build(
            [
                ("a", rec("s", T0, [U("q"), A("", [("1", "f", '{"a":1}')])])),
                ("b", rec("s", T0, [U("q"), A("", [("1", "f", '{"a": 1}')])])),
            ],
            it,
        )
        self.assertNotEqual(ra.tokens, rb.tokens)

    def test_content_list_and_role_matter(self):
        it = td.Interner()
        r1, r2, r3 = build(
            [
                (
                    "a",
                    rec(
                        "s",
                        T0,
                        [{"role": "user", "content": [{"type": "text", "text": "x"}]}],
                    ),
                ),
                (
                    "b",
                    rec(
                        "s",
                        T0,
                        [{"role": "user", "content": [{"type": "text", "text": "x"}]}],
                    ),
                ),
                (
                    "c",
                    rec(
                        "s",
                        T0,
                        [
                            {
                                "role": "developer",
                                "content": [{"type": "text", "text": "x"}],
                            }
                        ],
                    ),
                ),
            ],
            it,
        )
        self.assertEqual(r1.tokens, r2.tokens)
        self.assertNotEqual(r1.tokens, r3.tokens)

    def test_timestamp_parse(self):
        self.assertEqual(td.parse_ts("1970-01-01T00:00:01Z"), 10**9)
        self.assertEqual(td.parse_ts("1970-01-01T08:00:00.5+08:00"), 5 * 10**8)
        self.assertLess(
            td.parse_ts("2026-09-14T11:39:29.25Z"),
            td.parse_ts("2026-09-14T11:39:29.256Z"),
        )
        self.assertEqual(td.parse_ts(None), td.NEG_INF)
        self.assertEqual(td.parse_ts("garbage"), td.NEG_INF)

    def test_buckets(self):
        recs = build(
            [
                ("a", rec("sess1", T0, [U("q")])),
                ("b", rec("no_session_id", T0, [SYS(), U("q")])),
                ("c", rec("no_session_id", T0, [SYS(), U("q"), A("z")])),
                ("d", rec(None, T0, [SYS()])),
                ("e", rec("", T0, [SYS(), U("q")])),
            ]
        )
        by = {r.key: r.bucket for r in recs}
        self.assertEqual(by["a"], "s:sess1")
        self.assertTrue(by["b"].startswith("n:") and by["b"] == by["c"] == by["e"])
        self.assertEqual(by["d"], "k:d")


class Level1Tests(unittest.TestCase):
    def test_d1_duplicates_keep_latest_then_smallest_key(self):
        res = run(
            [("k1", rec("s", T0, H)), ("k2", rec("s", T1, H)), ("k3", rec("s", T1, H))]
        )
        self.assertEqual(res.keep1, {"k2"})
        self.assertEqual(res.drops1["k1"], ("duplicate", "k2"))
        self.assertEqual(res.drops1["k3"], ("duplicate", "k2"))

    def test_d1_ignores_call_id_differences(self):
        h2 = [
            SYS(),
            U("q1"),
            A("", [("call_other", "bash", '{"cmd":"ls"}')]),
            T("out1", "call_other"),
        ]
        res = run([("k1", rec("s", T0, H)), ("k2", rec("s", T1, h2))])
        self.assertEqual(res.keep1, {"k2"})
        self.assertEqual(res.drops1["k1"][0], "duplicate")

    def test_d2_prefix_chain_witness_is_longest(self):
        a = H[:2]
        b = H
        c = H + [A("done")]
        res = run(
            [("a", rec("s", T2, a)), ("b", rec("s", T2, b)), ("c", rec("s", T0, c))]
        )
        self.assertEqual(res.keep1, {"c"})
        self.assertEqual(res.drops1["a"], ("prefix", "c"))
        self.assertEqual(res.drops1["b"], ("prefix", "c"))

    def test_d2_prefix_that_differs_only_in_call_id(self):
        longer = [
            SYS(),
            U("q1"),
            A("", [("call_X", "bash", '{"cmd":"ls"}')]),
            T("out1", "call_X"),
            A("next", [("call_Y", "bash", '{"cmd":"pwd"}')]),
            T("out2", "call_Y"),
        ]
        res = run([("short", rec("s", T0, H)), ("long", rec("s", T1, longer))])
        self.assertEqual(res.keep1, {"long"})
        self.assertEqual(res.drops1["short"], ("prefix", "long"))

    def test_d3_abandoned_answer_when_other_branch_continues_with_tool(self):
        h = H
        a = h + [A("answer X")]  # 被弃用的回答
        b = h + [
            A("", [("c2", "bash", '{"cmd":"cat"}')]),
            T("o2", "c2"),
        ]  # 继续走了：有工具结果
        res = run([("a", rec("s", T2, a)), ("b", rec("s", T0, b))])
        self.assertEqual(res.keep1, {"b"})
        self.assertEqual(res.drops1["a"], ("abandoned_answer", "b"))

    def test_d3_abandoned_answer_when_other_branch_continues_with_user(self):
        a = H + [A("answer X")]
        b = H + [A("answer Y"), U("follow up")]
        res = run([("a", rec("s", T2, a)), ("b", rec("s", T0, b))])
        self.assertEqual(res.keep1, {"b"})
        self.assertEqual(res.drops1["a"][0], "abandoned_answer")

    def test_d3_siblings_keep_latest(self):
        specs = [
            ("a", rec("s", T0, H + [A("ans 1")])),
            ("b", rec("s", T2, H + [A("ans 2")])),
            ("c", rec("s", T1, H + [A("ans 3")])),
        ]
        res = run(specs)
        self.assertEqual(res.keep1, {"b"})
        self.assertEqual(res.drops1["a"], ("sibling_answer", "b"))
        self.assertEqual(res.drops1["c"], ("sibling_answer", "b"))

    def test_d3_sibling_tie_prefers_longer_then_smallest_key(self):
        # created_at 全相同：先比序列长度，再比 key
        specs = [
            ("a", rec("s", T1, H + [A("x1")])),
            ("b", rec("s", T1, H + [A("x2"), A("x2b")])),
            ("c", rec("s", T1, H + [A("x3"), A("x3b")])),
        ]
        res = run(specs)
        self.assertEqual(res.keep1, {"b"})  # b、c 同长，key 小的 b 胜
        self.assertEqual(res.drops1["a"], ("sibling_answer", "b"))
        self.assertEqual(res.drops1["c"], ("sibling_answer", "b"))
        res2 = run(list(reversed(specs)))
        self.assertEqual(res2.keep1, {"b"})

    def test_d3_mixed_siblings_and_continuation_never_empties_tree(self):
        specs = [
            ("s1", rec("s", T2, H + [A("ans 1")])),
            ("s2", rec("s", T1, H + [A("ans 2")])),
            (
                "go",
                rec(
                    "s",
                    T0,
                    H + [A("", [("c9", "bash", "{}")]), T("o9", "c9"), A("final")],
                ),
            ),
        ]
        res = run(specs)
        self.assertEqual(res.keep1, {"go"})
        self.assertEqual(res.drops1["s1"][0], "abandoned_answer")
        self.assertEqual(res.drops1["s2"][0], "abandoned_answer")

    def test_d3_not_triggered_without_shared_history_and_in_other_buckets(self):
        other = [
            SYS(),
            U("different"),
            A("", [("c", "bash", "{}")]),
            T("o", "c"),
            A("z"),
        ]
        res = run([("a", rec("s", T0, H + [A("ans")])), ("b", rec("s", T1, other))])
        self.assertEqual(res.keep1, {"a", "b"})
        res = run(
            [
                ("a", rec("s1", T0, H + [A("ans")])),
                ("b", rec("s2", T1, H + [A("", [("x", "bash", "{}")]), T("o", "x")])),
            ]
        )
        self.assertEqual(res.keep1, {"a", "b"})

    def test_all_assistant_record_has_no_d3(self):
        res = run([("a", rec("s", T0, [A("only")])), ("b", rec("s", T1, H))])
        self.assertEqual(res.keep1, {"a", "b"})


class Level2Tests(unittest.TestCase):
    def test_session_main_vs_sub(self):
        m1 = [SYS(), U("main q"), A("", [("c", "bash", "{}")]), T("o", "c")]
        m2 = [SYS(), U("main q"), A("a1"), U("another turn")]  # 与 m1 不互为前缀
        s1 = [SYS(), U("sub task A"), A("", [("c", "bash", "{}")]), T("o", "c")]
        s2 = [
            SYS(),
            U("sub task A"),
            A("", [("c", "bash", "{}")]),
            T("o", "c"),
            A("", [("d", "bash", "{}")]),
            T("p", "d"),
        ]
        s3 = [SYS(), U("sub task B"), A("", [("c", "bash", "{}")]), T("o", "c")]
        res = run(
            [
                ("m1", rec("S", T0, m1)),
                ("m2", rec("S", T2, m2)),
                ("s1", rec("S", T0, s1, sub=True)),
                ("s2", rec("S", T1, s2, sub=True)),
                ("s3", rec("S", T0, s3, sub=True)),
            ]
        )
        self.assertEqual(
            res.keep1, {"m1", "m2", "s2", "s3"}
        )  # s1 是 s2 的前缀（级别 1）
        self.assertEqual(res.keep2, {"m2", "s2", "s3"})
        self.assertEqual(res.drops2["m1"], ("same_conversation", "m2"))
        self.assertEqual(res.conv["m2"], "s:S/main")
        self.assertTrue(
            res.conv["s2"].startswith("s:S/sub/") and res.conv["s2"] != res.conv["s3"]
        )

    def test_conversation_ties_prefer_more_messages_then_key(self):
        a = [SYS(), U("q"), A("a1"), U("t2")]
        b = [SYS(), U("q"), A("a2"), U("t2"), A("a3"), U("t3")]
        c = [SYS(), U("q"), A("a4"), U("t2"), A("a5"), U("t3")]
        res = run(
            [("a", rec("S", T1, a)), ("b", rec("S", T1, b)), ("c", rec("S", T1, c))]
        )
        self.assertEqual(res.keep2, {"b"})

    def test_nosession_first_exchange_connectivity(self):
        first = [SYS(), U("q"), A("first answer")]
        # 同一第一次交换，之后分叉（互不为前缀，且都不以 assistant 结尾，D3 不触发）
        a = first + [U("turn 2a"), A("a2a"), U("turn 3a")]
        b = first + [U("turn 2b"), A("a2b"), U("turn 3b")]
        # 第一次交换不同（压缩重建 / 另一次采样）
        c = [SYS(), U("q"), A("other first answer"), U("turn 2c")]
        res = run(
            [
                ("a", rec("no_session_id", T0, a)),
                ("b", rec("no_session_id", T2, b)),
                ("c", rec("no_session_id", T1, c)),
            ]
        )
        self.assertEqual(res.keep1, {"a", "b", "c"})
        self.assertEqual(res.keep2, {"b", "c"})
        self.assertEqual(res.drops2["a"], ("same_conversation", "b"))
        self.assertEqual(len({res.conv[k] for k in ("a", "b", "c")}), 2)

    def test_nosession_without_first_exchange_stays_alone(self):
        a = [SYS(), U("q")]
        b = [SYS("different system"), U("q")]
        res = run(
            [("a", rec("no_session_id", T0, a)), ("b", rec("no_session_id", T1, b))]
        )
        self.assertEqual(res.keep2, {"a", "b"})


class DeterminismTests(unittest.TestCase):
    def _specs(self):
        main = [
            SYS(),
            U("q"),
            A("", [("c1", "bash", "{}")]),
            T("o1", "c1"),
            A("", [("c2", "bash", "{}")]),
            T("o2", "c2"),
        ]
        specs = []
        for i in range(1, 6):  # 同一历史的不同长度前缀
            specs.append(
                (f"pre{i}", rec("S", f"2026-08-21T10:00:0{i}.000Z", main[: i + 1]))
            )
        specs += [(f"dup{i}", rec("S", T1, main)) for i in range(3)]
        specs += [
            (
                f"ab{i}",
                rec("S", f"2026-08-21T10:01:0{i}.000Z", main[:4] + [A(f"ans {i}")]),
            )
            for i in range(4)
        ]
        specs += [
            (f"sib{i}", rec("S", T1, [SYS(), U("other"), A(f"ans {i}")]))
            for i in range(3)
        ]
        specs += [
            (
                f"n{i}",
                rec(
                    "no_session_id",
                    f"2026-08-21T10:02:0{i}.000Z",
                    [SYS(), U("nq"), A("fx"), U(f"t{i}"), A("z"), U(f"u{i}")],
                ),
            )
            for i in range(3)
        ]
        specs += [
            (
                f"sub{i}",
                rec(
                    "S",
                    f"2026-08-21T10:03:0{i}.000Z",
                    [SYS(), U("sub"), A("a"), U(f"v{i}")],
                    sub=True,
                ),
            )
            for i in range(3)
        ]
        specs += [("e1", rec("no_session_id", T0, [SYS()]))]
        return specs

    def _render(self, specs, level):
        it = td.Interner()  # 每次新建，编号随输入顺序变化
        res = td.dedup(build(specs, it))
        return td.render(res, level, [k for k, _ in specs], {"src": "mem"})

    def test_shuffled_inputs_give_identical_output(self):
        specs = self._specs()
        for level in ("snapshot", "conversation"):
            base = self._render(specs, level)
            for seed in range(25):
                sh = list(specs)
                random.Random(seed).shuffle(sh)
                self.assertEqual(self._render(sh, level), base)

    def test_sanity_of_synthetic_mix(self):
        res = td.dedup(build(self._specs()))
        self.assertEqual(sum(res.merged1.values()), len(res.recs))
        self.assertEqual(sum(res.merged2.values()), len(res.recs))
        self.assertTrue(res.keep2 <= res.keep1)
        for k in res.drops1:
            self.assertNotIn(k, res.keep1)

    def test_output_has_no_message_text(self):
        out = self._render(self._specs(), "conversation")
        blob = "".join(out.values())
        for s in ("ans 1", "turn", "sub task", "different"):
            self.assertNotIn(s, blob)
        for line in out["kept.jsonl"].splitlines():
            self.assertEqual(
                set(json.loads(line)),
                {
                    "key",
                    "bucket",
                    "conversation",
                    "created_at",
                    "n_messages",
                    "is_subagent",
                    "audit_tag",
                    "merged",
                },
            )


class BruteForceTests(unittest.TestCase):
    """用按规则原文写的 O(n^2) 参考实现，对随机小树逐条比对级别 1 的结果。"""

    @staticmethod
    def _ref_level1(items):
        # items: [(key, created_ns, [message_json_str...], [role...])]
        drops, alive = {}, []
        groups = {}
        for it in items:
            groups.setdefault(tuple(it[2]), []).append(it)
        for g in groups.values():
            rep = min(g, key=lambda x: (-x[1], x[0]))
            for it in g:
                if it is not rep:
                    drops[it[0]] = ("duplicate", rep[0])
            alive.append(rep)
        s2 = []
        for a in alive:
            la = len(a[2])
            bs = [
                b
                for b in alive
                if b is not a and len(b[2]) > la and tuple(b[2][:la]) == tuple(a[2])
            ]
            if bs:
                w = min(bs, key=lambda x: (-len(x[2]), -x[1], x[0]))
                drops[a[0]] = ("prefix", w[0])
            else:
                s2.append(a)
        for a in s2:
            roles = a[3]
            tail = 0
            while tail < len(roles) and roles[len(roles) - 1 - tail] == "assistant":
                tail += 1
            lh = len(roles) - tail
            if tail < 1 or lh < 1:
                continue
            h = tuple(a[2][:lh])
            bs = [
                b
                for b in s2
                if b is not a
                and tuple(b[2][:lh]) == h
                and len(b[2]) >= lh
                and not (
                    len(b[2]) > len(a[2]) and tuple(b[2][: len(a[2])]) == tuple(a[2])
                )
            ]
            cont = [b for b in bs if any(r != "assistant" for r in b[3][lh:])]
            if cont:
                w = min(cont, key=lambda x: (-len(x[2]), -x[1], x[0]))
                drops[a[0]] = ("abandoned_answer", w[0])
            elif bs:
                grp = bs + [a]
                best = min(grp, key=lambda x: (-x[1], -len(x[2]), x[0]))
                if best is not a:
                    drops[a[0]] = ("sibling_answer", best[0])
        return drops

    def test_random_trees_match_reference(self):
        rnd = random.Random(7)
        for trial in range(300):
            # 几条主线（assistant 调用 / tool 结果交替的长历史），每条记录取某处截断，再随机接 0-2 条 assistant 回答
            trunks = []
            for t in range(rnd.randint(1, 3)):
                m = [SYS(), U("q")]
                for i in range(rnd.randint(0, 5)):
                    m += [
                        A(
                            f"a{t}_{rnd.randint(0, 1)}",
                            [(f"c{rnd.randint(0, 9)}", "bash", "{}")],
                        ),
                        T(f"r{t}_{rnd.randint(0, 1)}", "x"),
                    ]
                trunks.append(m)
            specs = []
            for i in range(rnd.randint(1, 14)):
                m = list(rnd.choice(trunks))
                m = m[: rnd.randint(2, len(m))]
                for _ in range(rnd.choice([0, 0, 1, 1, 2])):
                    m.append(A(f"ans{rnd.randint(0, 2)}"))
                ts = f"2026-08-21T10:00:0{rnd.randint(0, 3)}.000Z"
                specs.append((f"k{i:02d}", rec("S", ts, m)))
            res = td.dedup(build(specs))
            items = []
            for k, o in specs:
                msgs = o["messages"]
                items.append(
                    (
                        k,
                        td.parse_ts(o["metadata"]["created_at"]),
                        [td._canon(td.message_view(m)) for m in msgs],
                        [m["role"] for m in msgs],
                    )
                )
            ref = self._ref_level1(items)
            self.assertEqual(dict(res.drops1), ref, f"trial {trial}")
            self.assertTrue(res.keep1)  # 树不会被删空


class CoverageTests(unittest.TestCase):
    def test_synthetic_mix_exercises_every_rule(self):
        res = td.dedup(build(DeterminismTests()._specs()))
        seen = {v[0] for v in res.drops1.values()} | {v[0] for v in res.drops2.values()}
        self.assertEqual(seen, set(td.RULES))


class SourceTests(unittest.TestCase):
    def test_traj_regex(self):
        ok = [
            "prefix/x/model-a/V001/dt=2026-08-21/abc_sub_0.jsonl",
            "A/dt=2026/ab_cd_sub_12.jsonl",
            "dt=1/x_sub_3.jsonl",
        ]
        bad = [
            "prefix/x/_pipeline/batches/000001.json",
            "manifest.json",
            "A/dt=1/x.jsonl",
            "A/dt=1/sub/x_sub_1.jsonl",
            "A/dt=1/x_sub_1.jsonl.bak",
        ]
        for k in ok:
            self.assertTrue(td.TRAJ_RE.search(k), k)
        for k in bad:
            self.assertFalse(td.TRAJ_RE.search(k), k)

    def test_local_directory_end_to_end(self):
        with tempfile.TemporaryDirectory() as root:
            os.makedirs(os.path.join(root, "m1", "dt=2026-01-01"))
            os.makedirs(os.path.join(root, "_pipeline"))
            for name, msgs in (
                ("S_sub_0.jsonl", H),
                ("S_sub_1.jsonl", H + [A("final")]),
            ):
                with open(os.path.join(root, "m1", "dt=2026-01-01", name), "w") as f:
                    f.write(json.dumps(rec("S", T0, msgs)) + "\n")
            with open(os.path.join(root, "_pipeline", "x.json"), "w") as f:
                f.write("{}")
            with open(os.path.join(root, "manifest.json"), "w") as f:
                f.write("{}")
            out = os.path.join(root, "out")
            self.assertEqual(td.main(["--src", root, "--out", out]), 0)
            with open(os.path.join(out, "kept.jsonl")) as f:
                kept = [json.loads(l) for l in f]
            self.assertEqual(
                [r["key"] for r in kept], ["m1/dt=2026-01-01/S_sub_1.jsonl"]
            )
            self.assertEqual(kept[0]["merged"], 2)
            with open(os.path.join(out, "summary.json")) as f:
                summ = json.load(f)
            self.assertEqual(
                (summ["n_input"], summ["dropped_by_rule"]["prefix"]), (2, 1)
            )

    def test_s3_v1_listing_with_marker_pagination(self):
        pages = {
            "": ["p/a/dt=1/x_sub_0.jsonl", "p/a/_pipeline/b.json"],
            "p/a/_pipeline/b.json": ["p/a/dt=1/y_sub_1.jsonl", "p/manifest.json"],
        }

        class Fake:
            def list_objects(self, Bucket, Prefix, Marker, MaxKeys):
                keys = pages[Marker]
                return {
                    "Contents": [{"Key": k, "Size": 1} for k in keys],
                    "IsTruncated": Marker == "",
                }

        got = td.S3Source(Fake(), "b", "p/").list()
        self.assertEqual(
            [k for k, _ in got], ["p/a/dt=1/x_sub_0.jsonl", "p/a/dt=1/y_sub_1.jsonl"]
        )

    def test_copy_kept_maps_relative_paths_and_refuses_overlap(self):
        calls = []

        class Fake:
            def copy_object(self, Bucket, Key, CopySource):
                calls.append((Bucket, Key, CopySource["Bucket"], CopySource["Key"]))

        td.copy_kept(
            Fake(), "src-b", "a/b/", "s3://dst-b/out", ["a/b/m/dt=1/x_sub_0.jsonl"], 2
        )
        self.assertEqual(
            calls,
            [
                (
                    "dst-b",
                    "out/m/dt=1/x_sub_0.jsonl",
                    "src-b",
                    "a/b/m/dt=1/x_sub_0.jsonl",
                )
            ],
        )
        with self.assertRaises(SystemExit):
            td.copy_kept(
                Fake(),
                "src-b",
                "a/b/",
                "s3://src-b/a/b/kept/",
                ["a/b/m/dt=1/x_sub_0.jsonl"],
                2,
            )
        self.assertEqual(len(calls), 1)


# ----------------------------------------------------------------------------- 脱敏等价

BASE = "Please contact the customer at {} regarding the delayed shipment of order 1234 today."


class DesensEquivTests(unittest.TestCase):
    def eq(self, x, y):
        a, b = td.desens_equiv(x, y), td.desens_equiv(y, x)
        self.assertEqual(a, b, "asymmetric result")
        return a

    def test_single_placeholder(self):
        self.assertTrue(self.eq(BASE.format("<PHONE>"), BASE.format("13800138000")))
        self.assertTrue(self.eq(BASE.format("<IP_ADDRESS>"), BASE.format("10.0.0.1")))
        self.assertTrue(
            self.eq(BASE.format("[REDACTED]"), BASE.format("Mr. John Smith"))
        )
        self.assertTrue(self.eq(BASE.format("[USER]"), BASE.format("alice")))
        self.assertTrue(self.eq(BASE.format("[AWS_REDACTED]"), BASE.format("AKIAXXXX")))
        self.assertTrue(self.eq(BASE.format("********"), BASE.format("hunter2hunter2")))

    def test_identical_and_no_placeholder(self):
        self.assertTrue(td.desens_equiv(BASE.format("x"), BASE.format("x")))
        self.assertFalse(
            self.eq(BASE.format("abc"), BASE.format("abd"))
        )  # 两边都没有占位符

    def test_fragment_length_bounds(self):
        self.assertTrue(self.eq(BASE.format("<PHONE>"), BASE.format("1")))
        self.assertTrue(self.eq(BASE.format("<PHONE>"), BASE.format("9" * 256)))
        self.assertFalse(self.eq(BASE.format("<PHONE>"), BASE.format("9" * 257)))
        self.assertFalse(
            self.eq(BASE.format("<PHONE>"), BASE.format(""))
        )  # 替换 0 个字符不行

    def test_multiple_placeholders(self):
        t = "Call {} or mail {} before the deadline of the quarterly project review, token {} included."
        self.assertTrue(
            self.eq(
                t.format("<PHONE>", "<EMAIL>", "<TOKEN>"),
                t.format("13800138000", "a@b.com", "xyz"),
            )
        )
        self.assertTrue(
            self.eq(
                t.format("<PHONE>", "a@b.com", "xyz"),
                t.format("13800138000", "<EMAIL>", "<TOKEN>"),
            )
        )
        self.assertFalse(
            self.eq(
                t.format("<PHONE>", "<EMAIL>", "<TOKEN>"),
                t.format("13800138000", "a@b.com", "xyz") + " extra",
            )
        )

    def test_adjacent_placeholders_scale_the_bounds(self):
        t = "The secret pair for the staging environment is {} and nothing else was recorded here."
        self.assertTrue(self.eq(t.format("<USER_A><USER_B>"), t.format("abcd")))
        self.assertFalse(
            self.eq(t.format("<USER_A><USER_B>"), t.format("a"))
        )  # 两个占位符至少替换 2 个字符

    def test_placeholders_on_both_sides_at_different_positions(self):
        t = "Send the file to {} and copy {} on the thread about the contract renewal for next year."
        self.assertTrue(
            self.eq(t.format("<EMAIL>", "bob"), t.format("alice@x.org", "<NAME>"))
        )
        self.assertTrue(
            self.eq(t.format("<EMAIL>", "<NAME>"), t.format("<EMAIL>", "bob"))
        )  # 同位置同一个占位符按字面相同
        self.assertFalse(
            self.eq(t.format("<EMAIL>", "bob"), t.format("<PHONE>", "bob"))
        )  # 同位置不同占位符不对齐

    def test_newline_in_fragment_rejected(self):
        self.assertFalse(
            self.eq(BASE.format("<PHONE>"), BASE.format("12\\n34"))
        )  # JSON 转义的 \n
        self.assertFalse(
            self.eq(BASE.format("<PHONE>"), BASE.format("12\n34"))
        )  # 原始换行
        # 片段以反斜杠结尾，与后面的字面 n 拼成换行转义
        self.assertFalse(
            self.eq(
                "starting text before the mask <PHONE>nine other literal text after.",
                "starting text before the mask 12\\nine other literal text after.",
            )
        )
        self.assertTrue(
            self.eq(BASE.format("<PHONE>"), BASE.format("12\\\\34"))
        )  # 转义的反斜杠不算换行

    def test_literal_mismatch_and_info_log_markers(self):
        self.assertFalse(
            self.eq(BASE.format("<PHONE>"), BASE.format("123").replace("order", "item"))
        )
        a = "[INFO] starting the server on port 8080 now and waiting for connections"
        b = "[INFO] starting the server on port 9090 now and waiting for connections"
        self.assertFalse(self.eq(a, b))
        self.assertIsNone(td.PLACEHOLDER_RE.search("[INFO] [ERROR] [WARNING] [HTTP]"))

    def test_mostly_placeholder_messages_rejected(self):
        few = "abc" + "<NAME>"
        self.assertFalse(self.eq(few, "abc" + "x" * 20))  # 字面不足 8 个字符
        mostly = "xxxxxxxxxxxx" + "<NAME>" * 13  # 字面 12 个，占比远低于 50%
        self.assertFalse(self.eq(mostly, "xxxxxxxxxxxx" + "q" * 40))
        half = "xxxxxxxxxxxx" + "<NAME>" * 2  # 字面 12/24 = 50%
        self.assertTrue(self.eq(half, "xxxxxxxxxxxx" + "q" * 4))

    def test_custom_regex(self):
        import re as _re

        rx = _re.compile(r"\{\{[a-z]+\}\}")
        self.assertTrue(
            td.desens_equiv(BASE.format("{{phone}}"), BASE.format("555"), rx)
        )
        self.assertFalse(
            td.desens_equiv(BASE.format("<PHONE>"), BASE.format("555"), rx)
        )

    def test_100k_characters_is_fast(self):
        import time

        words = "lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor "
        parts_x, parts_y = [], []
        for i in range(300):
            chunk = words * 4
            parts_x.append(chunk + "<PHONE>")
            parts_y.append(chunk + f"1380013{i:04d}")
        x, y = "".join(parts_x), "".join(parts_y)
        self.assertGreater(len(x), 90000)
        t0 = time.time()
        self.assertTrue(self.eq(x, y))
        self.assertLess(time.time() - t0, 1.0)
        # 对抗输入：大量重复字符，贪心对齐的搜索量有预算上限
        bad_x = "a" * 50000 + "<PHONE>" + "a" * 49999 + "b"
        bad_y = "a" * 50000 + "1" + "a" * 99999 + "c"
        t0 = time.time()
        td.desens_equiv(bad_x, bad_y)
        self.assertLess(time.time() - t0, 2.0)
        many = ("ab<PHONE>" + "cd" * 20) * 3000
        t0 = time.time()
        td.desens_equiv(many, many.replace("<PHONE>", "1"))
        self.assertLess(time.time() - t0, 2.0)


class CandidateTests(unittest.TestCase):
    def test_candidate_filters(self):
        info = {
            1: ("user", True, 100, 5, 6),
            2: ("user", False, 100, 5, 9),
            3: ("assistant", False, 100, 5, 6),
            4: ("user", False, 300, 5, 6),
            5: ("user", False, 100, 7, 8),
            6: ("user", False, 100, 8, 6),
            7: ("user", False, 60, 1, 6),
            8: ("user", False, 200, 1, 6),
        }
        got = td.candidate_pairs([1, 2, 3, 4, 5, 6, 7, 8], info)
        # 1-2 头哈希相同；1-6 尾哈希相同；1-3 role 不同；1-4 长度比 3；1-5 无哈希相同；1-7、1-8 长度比 100/60≈1.67、2 都在 [0.5,2] 内
        self.assertEqual(got, {(1, 2), (1, 6), (1, 7), (1, 8)})
        self.assertEqual(
            td.candidate_pairs([2, 5, 6], info), set()
        )  # 没有含占位符的 token


# ----------------------------------------------------------------------------- 去重层面的脱敏

LONG = (
    "Please contact the customer at {} regarding the delayed shipment of order 1234 that was placed last week, "
    "and make sure to confirm the new delivery window with them before the end of today if at all possible."
)


def key_of(k):
    return f"m/dt=1/{k}_sub_0.jsonl"


def pipeline(specs, **kw):
    src = td.MemorySource({key_of(k): o for k, o in specs})
    res, errors, stats = td.run_pipeline(src, **kw)
    assert not errors
    return res, stats


def hist(ph, tool_ph=None):
    """一段带占位符/真实值的历史（用户消息和工具输出各有可脱敏处）。"""
    msgs = [
        SYS(),
        U(LONG.format(ph)),
        A("", [("c1", "bash", '{"cmd":"ls"}')]),
        T(
            "directory listing for the user %s: a.txt b.txt c.txt, plus some other files that were found."
            % (tool_ph or ph),
            "c1",
        ),
    ]
    return msgs


class DesensPipelineTests(unittest.TestCase):
    def test_desens_duplicate(self):
        specs = [
            ("a", rec("S", T0, hist("<PHONE>"))),
            ("b", rec("S", T1, hist("13800138000"))),
        ]
        res, st = pipeline(specs)
        self.assertEqual(res.keep1, {key_of("b")})
        self.assertEqual(res.drops1[key_of("a")], ("duplicate", key_of("b")))
        self.assertTrue(res.via[key_of("a")])
        self.assertGreaterEqual(st["equivalent_pairs"], 1)
        self.assertEqual(
            (st["records_affected"], st["records_rewritten"]), (2, 1)
        )  # 代表所在的记录不用改写
        res2, st2 = pipeline(specs, desens=False)
        self.assertEqual(res2.keep1, {key_of("a"), key_of("b")})
        self.assertFalse(any(res2.via.values()))
        self.assertEqual(st2["enabled"], False)

    def test_desens_prefix_and_flag_false_for_plain_prefix(self):
        longer = hist("13800138000") + [
            A("next", [("c2", "bash", "{}")]),
            T("more output", "c2"),
        ]
        plain = [SYS(), U("short"), A("x")]
        specs = [
            ("a", rec("S", T2, hist("<PHONE>"))),
            ("b", rec("S", T0, longer)),
            ("p1", rec("S", T0, plain)),
            ("p2", rec("S", T0, plain + [U("again")])),
        ]
        res, _ = pipeline(specs)
        self.assertEqual(res.drops1[key_of("a")], ("prefix", key_of("b")))
        self.assertTrue(res.via[key_of("a")])
        self.assertEqual(res.drops1[key_of("p1")][0], "prefix")
        self.assertFalse(res.via[key_of("p1")])

    def test_desens_abandoned_answer(self):
        specs = [
            ("a", rec("S", T2, hist("<PHONE>") + [A("answer X")])),
            (
                "b",
                rec(
                    "S",
                    T0,
                    hist("13800138000")
                    + [A("", [("c9", "bash", "{}")]), T("o9", "c9")],
                ),
            ),
        ]
        res, _ = pipeline(specs)
        self.assertEqual(res.drops1[key_of("a")], ("abandoned_answer", key_of("b")))
        self.assertTrue(res.via[key_of("a")])
        res2, _ = pipeline(specs, desens=False)
        self.assertEqual(res2.keep1, {key_of("a"), key_of("b")})

    def test_nosession_first_user_differs_only_by_desens_shares_bucket(self):
        a = hist("<PHONE>") + [A("done a")]
        b = hist("13800138000") + [A("done b")]
        specs = [("a", rec("no_session_id", T0, a)), ("b", rec("no_session_id", T1, b))]
        res, _ = pipeline(specs)
        self.assertEqual(res.recs[key_of("a")].bucket, res.recs[key_of("b")].bucket)
        self.assertEqual(
            res.recs[key_of("a")].raw_bucket != res.recs[key_of("b")].raw_bucket, True
        )
        self.assertEqual(len(res.keep1), 1)  # 同历史的两个兄弟回答只留一条
        self.assertTrue(all(res.via.values()))
        res2, _ = pipeline(specs, desens=False)
        self.assertNotEqual(
            res2.recs[key_of("a")].bucket, res2.recs[key_of("b")].bucket
        )
        self.assertEqual(len(res2.keep1), 2)

    def test_no_false_merge_when_literal_text_differs(self):
        other = LONG.replace("delayed shipment", "refund request").format("13800138000")
        a = [
            SYS(),
            U(LONG.format("<PHONE>")),
            A("", [("c1", "bash", "{}")]),
            T("o", "c1"),
        ]
        b = [SYS(), U(other), A("", [("c1", "bash", "{}")]), T("o", "c1")]
        res, st = pipeline([("a", rec("S", T0, a)), ("b", rec("S", T1, b))])
        self.assertEqual(res.keep1, {key_of("a"), key_of("b")})
        self.assertEqual(st["equivalent_pairs"], 0)

    def test_desens_same_conversation_in_nosession_bucket(self):
        first_a = [
            SYS(),
            U(LONG.format("<PHONE>")),
            A(
                "first answer with <EMAIL> inside, repeated at length so the text is long enough to compare. "
                * 3
            ),
        ]
        first_b = [
            SYS(),
            U(LONG.format("13800138000")),
            A(
                "first answer with bob@x.org inside, repeated at length so the text is long enough to compare. "
                * 3
            ),
        ]
        a = first_a + [U("turn 2a"), A("a2"), U("turn 3a")]
        b = first_b + [U("turn 2b"), A("b2"), U("turn 3b")]
        res, _ = pipeline(
            [("a", rec("no_session_id", T0, a)), ("b", rec("no_session_id", T2, b))]
        )
        self.assertEqual(res.keep1, {key_of("a"), key_of("b")})
        self.assertEqual(res.keep2, {key_of("b")})
        self.assertEqual(res.drops2[key_of("a")], ("same_conversation", key_of("b")))
        self.assertTrue(res.via[key_of("a")])

    def test_custom_placeholder_regex(self):
        specs = [
            ("a", rec("S", T0, hist("{{phone}}"))),
            ("b", rec("S", T1, hist("13800138000"))),
        ]
        res, _ = pipeline(specs, placeholder_regex=r"\{\{[a-z]+\}\}")
        self.assertEqual(res.keep1, {key_of("b")})
        res2, _ = pipeline(specs)  # 默认正则不认识 {{phone}}
        self.assertEqual(res2.keep1, {key_of("a"), key_of("b")})

    def test_shuffled_inputs_and_threads_give_identical_output(self):
        specs = [
            ("a", rec("S", T0, hist("<PHONE>"))),
            ("b", rec("S", T1, hist("13800138000"))),
            ("c", rec("S", T2, hist("<IP_ADDRESS>") + [A("answer")])),
            (
                "d",
                rec(
                    "S",
                    T0,
                    hist("10.0.0.1") + [A("", [("c9", "bash", "{}")]), T("o9", "c9")],
                ),
            ),
            ("n1", rec("no_session_id", T0, hist("<PHONE>") + [A("x")])),
            ("n2", rec("no_session_id", T1, hist("1380013") + [A("y")])),
            ("p", rec("S2", T0, [SYS(), U("plain"), A("a")])),
            ("q", rec("S2", T1, [SYS(), U("plain")])),
        ]
        outs = []
        for seed, workers in ((0, 1), (1, 4), (2, 8), (3, 2), (4, 16)):
            sh = list(specs)
            random.Random(seed).shuffle(sh)
            res, stats = pipeline(
                sh,
                workers=workers,
                text_chars=2000 if seed % 2 else td.TEXT_CHUNK_CHARS,
            )  # 小块也要同样结果
            outs.append(
                td.render(
                    res,
                    "conversation",
                    [key_of(k) for k, _ in sh],
                    {"src": "mem"},
                    None,
                    stats,
                )
            )
        for o in outs[1:]:
            self.assertEqual(o, outs[0])
        self.assertIn('"desens": true', outs[0]["dropped.jsonl"])

    def test_summary_and_dropped_fields(self):
        specs = [
            ("a", rec("S", T0, hist("<PHONE>"))),
            ("b", rec("S", T1, hist("13800138000"))),
        ]
        res, st = pipeline(specs)
        out = td.render(
            res,
            "conversation",
            [key_of(k) for k, _ in specs],
            {"src": "mem", "desens": True},
            None,
            st,
        )
        row = json.loads(out["dropped.jsonl"].splitlines()[0])
        self.assertEqual(
            set(row), {"key", "bucket", "rule", "level", "witness", "root", "desens"}
        )
        summ = json.loads(out["summary.json"])
        self.assertEqual(summ["dropped_via_desens_by_rule"]["duplicate"], 1)
        self.assertEqual(summ["dropped_via_desens_total"], 1)
        self.assertEqual(
            summ["desens"]["equivalent_pairs"], 2
        )  # 用户消息和工具输出各一对
        self.assertEqual(summ["desens"]["tokens_merged"], 2)
        self.assertEqual(summ["params"]["desens"], True)

    def test_cli_no_desens_flag_and_regex_param(self):
        with tempfile.TemporaryDirectory() as root:
            os.makedirs(os.path.join(root, "m", "dt=1"))
            for name, msgs, ts in (
                ("S_sub_0.jsonl", hist("<PHONE>"), T0),
                ("S_sub_1.jsonl", hist("13800138000"), T1),
            ):
                with open(os.path.join(root, "m", "dt=1", name), "w") as f:
                    f.write(json.dumps(rec("S", ts, msgs)) + "\n")
            for flag, expect in ((None, 1), ("--no-desens", 2)):
                out = os.path.join(root, "out" + str(expect))
                self.assertEqual(
                    td.main(["--src", root, "--out", out] + ([flag] if flag else [])), 0
                )
                with open(os.path.join(out, "summary.json")) as f:
                    summ = json.load(f)
                self.assertEqual(summ["level1_snapshot_remaining"], expect)
                self.assertEqual(summ["params"]["desens"], flag is None)
                self.assertEqual(
                    summ["params"]["placeholder_regex"],
                    td.DEFAULT_PLACEHOLDER_REGEX if flag is None else None,
                )


def _ref_desens_equiv(x, y, rx, max_frag):
    """按规则原文写的 DP 参考实现（只用于小串）：不带“片段与相邻字符拼出转义”的保守检查，所以比被测实现宽松。"""
    import functools

    if x == y:
        return True
    sp = []
    for s in (x, y):
        spans = {m.start(): m.end() for m in rx.finditer(s) if m.end() > m.start()}
        inside = [False] * (len(s) + 1)
        for b, e in spans.items():
            for t in range(b, e):
                inside[t] = True
        if spans:
            lit = len(s) - sum(e - b for b, e in spans.items())
            if lit < 8 or lit * 2 < len(s):
                return False
        sp.append((spans, inside))
    (sx, inx), (sy, iny) = sp
    if not sx and not sy:
        return False

    @functools.cache
    def f(i, j):
        if i == len(x) and j == len(y):
            return True
        if i in sx and j in sy and x[i : sx[i]] == y[j : sy[j]] and f(sx[i], sy[j]):
            return True
        for a, b, spa, ina, other, inother, spo, swap in (
            (i, j, sx, inx, y, iny, sy, False),
            (j, i, sy, iny, x, inx, sx, True),
        ):
            if a in spa:  # a 侧在占位符上：吃掉 b 侧 1..max_frag 个字面字符
                for k in range(1, max_frag + 1):
                    if b + k > len(other) or any(inother[b + t] for t in range(k)):
                        break
                    frag = other[b : b + k]
                    if "\n" in frag or "\\n" in frag:
                        break
                    if f(spa[a], b + k) if not swap else f(b + k, spa[a]):
                        return True
        if (
            i < len(x)
            and j < len(y)
            and i not in sx
            and j not in sy
            and not inx[i]
            and not iny[j]
            and x[i] == y[j]
        ):
            return f(i + 1, j + 1)
        return False

    return f(0, 0)


class DesensPropertyTests(unittest.TestCase):
    def test_true_results_are_confirmed_by_reference_and_easy_cases_are_found(self):
        rnd = random.Random(11)
        rx = td.PLACEHOLDER_RE
        alpha = ["a", "b", "c", " ", "x", "\\n"]
        phs = ["<AB>", "<CD>", "<EF_1>", "****"]

        def rand_text(n_lit, n_ph):
            parts = [rnd.choice(alpha) for _ in range(n_lit)]
            for _ in range(n_ph):
                parts.insert(rnd.randint(0, len(parts)), rnd.choice(phs))
            return "".join(parts)

        trues = misses = valid = 0
        for _ in range(6000):
            base = rand_text(rnd.randint(10, 26), rnd.randint(0, 3))
            mode = rnd.random()
            if mode < 0.5:  # 把 base 的占位符替换成随机片段 -> 通常等价
                other = rx.sub(
                    lambda m: "".join(
                        rnd.choice(alpha[:5]) for _ in range(rnd.randint(1, 4))
                    ),
                    base,
                )
                x, y = base, other
            elif mode < 0.8:  # 在 other 的随机位置再放占位符（两边都有）
                other = rx.sub(
                    lambda m: "".join(
                        rnd.choice(alpha[:5]) for _ in range(rnd.randint(1, 3))
                    ),
                    base,
                )
                i = rnd.randint(0, len(other) - 3)
                other = other[:i] + rnd.choice(phs) + other[i + rnd.randint(1, 3) :]
                x, y = base, other
            else:  # 完全随机
                x, y = base, rand_text(rnd.randint(10, 26), rnd.randint(0, 3))
            mf = rnd.choice([3, 4, 256])
            got = td.desens_equiv(x, y, rx, mf)
            ref = _ref_desens_equiv(x, y, rx, mf)
            if got:
                trues += 1
                self.assertTrue(ref, f"unsound: {x!r} vs {y!r} (max_frag={mf})")
            if mode < 0.5 and ref:
                valid += 1
                misses += not got
        self.rates = (trues, valid, misses)
        self.assertGreater(trues, 500)
        self.assertGreater(valid, 500)
        self.assertLess(
            misses / valid,
            0.3,
            f"greedy alignment misses too many valid pairs: {misses}/{valid}",
        )


if __name__ == "__main__":
    unittest.main()
