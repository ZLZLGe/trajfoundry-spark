from __future__ import annotations

import inspect
import random
import weakref
from dataclasses import asdict, replace
from types import SimpleNamespace

import pytest

from trajfoundry.models import (
    AuditIssue,
    FunctionCall,
    MediaMapping,
    ToolCall,
    ToolDefinition,
)
from trajfoundry.pipeline import _response_reasoning
from trajfoundry.pipeline import _trajectory_from_leaf as reference_leaf
from trajfoundry.spark import bounded
from trajfoundry.spark.bounded import (
    BoundedBuildLimitError,
    BoundedBuildLimits,
    build_scope_bounded,
)
from trajfoundry.spark.build import build_scope
from trajfoundry.spark.output import serialize_trajectory
from trajfoundry.streaming import streaming_prefix_leaves
from trajfoundry.subagents import (
    MountDiagnostic,
    MountEdge,
    SpawnEvidence,
    SubagentPlanLimitError,
    _agent_message_index,
    _attach_relay_ids,
    _contains_spawn,
    _DiagnosticBudget,
    _extract_spawn_evidence,
    _indexed_spawn_candidates,
    _parse_routing_marker,
    _spawn_parent_index,
    _validate_child,
    plan_subagent_mounts,
)

from . import test_pipeline as pipeline_contracts
from . import test_spark_build as contracts
from . import test_subagents as subagent_contracts
from .test_spark_build import _duplicate_capture, _snapshot
from .test_subagents import agent_message, assistant, snapshot, spawn, user


def _diagnostic_helper_case(kind, budget=None):
    kwargs = {"diagnostic_budget": budget} if budget is not None else {}
    parent = snapshot("parent", thread="main", turn="")
    if kind == "spawn":
        parent.response = [assistant(calls=[spawn("call", "child")])]
        return _extract_spawn_evidence([parent], **kwargs)[1]
    relay = agent_message(
        "relay", author="/root/child", recipient="/root", preceding=["call"]
    )
    if kind == "agent":
        parent.agent_messages = [relay, relay.model_copy(deep=True)]
        return _agent_message_index([parent], [parent], **kwargs)[2]
    if kind == "child":
        return _validate_child(parent, _parse_routing_marker(""), 0, **kwargs)
    child = snapshot("child", thread="child", turn="turn-child")
    return _attach_relay_ids(
        [MountEdge(0, 1, "call", "turn", agent_name="/root/child")],
        [parent, child],
        {("s", "main"): [relay]},
        **kwargs,
    )[1]


@pytest.mark.parametrize("kind", ["spawn", "agent", "child", "relay"])
def test_diagnostic_helpers_keep_default_behavior_and_guard_before_retaining(kind):
    expected = _diagnostic_helper_case(kind)
    assert expected
    budget = _DiagnosticBudget(1_000_000)
    assert _diagnostic_helper_case(kind, budget) == expected
    measured = _DiagnosticBudget(1_000_000)
    for diagnostic in expected:
        measured.make(**asdict(diagnostic))
    assert budget.used == measured.used
    with pytest.raises(SubagentPlanLimitError, match="planner diagnostics"):
        _diagnostic_helper_case(kind, _DiagnosticBudget(measured.used - 1))


def test_diagnostic_budget_is_shared_across_helpers_without_recharging_extend():
    duplicate = agent_message(
        "duplicate", author="/root/child", recipient="/root", preceding=[]
    )
    value = snapshot(
        "combined",
        thread="main",
        turn="",
        response=[assistant(calls=[spawn("call", "child")])],
        agent_messages=[duplicate, duplicate.model_copy(deep=True)],
    )
    budget = _DiagnosticBudget(1_000_000)
    _, spawn_diagnostics = _extract_spawn_evidence([value], diagnostic_budget=budget)
    spawn_bytes = budget.used
    _, _, agent_diagnostics = _agent_message_index(
        [value], [value], diagnostic_budget=budget
    )
    assert spawn_diagnostics and agent_diagnostics
    assert budget.used > spawn_bytes > 0
    exact = budget.used
    expected = plan_subagent_mounts([value])
    assert len(expected.diagnostics) == 2
    assert plan_subagent_mounts([value], diagnostic_max_bytes=exact) == expected
    with pytest.raises(SubagentPlanLimitError, match="planner diagnostics"):
        plan_subagent_mounts([value], diagnostic_max_bytes=exact - 1)


def test_bounded_rejects_path_times_call_diagnostics_before_sql_retention(
    monkeypatch, tmp_path
):
    value = snapshot(
        "wide-diagnostic",
        thread="main",
        turn="",
        response=[assistant(calls=[spawn(f"call-{i}", "child") for i in range(256)])],
    ).model_copy(update={"source_path": "/" + "x" * 4096 + ".json"})

    def forbidden(*args, **kwargs):
        pytest.fail("unbounded diagnostic collection reached the SQL storage stage")

    monkeypatch.setattr(bounded._DiskScope, "store_graph_diagnostics", forbidden)
    with pytest.raises(BoundedBuildLimitError, match="planner diagnostics"):
        tuple(
            build_scope_bounded(
                [value],
                workspace_parent=tmp_path,
                limits=replace(BoundedBuildLimits(), max_routing_bytes=256 * 1024),
            )
        )
    assert list(tmp_path.iterdir()) == []


def test_empty_eligible_transcript_matches_streaming_builder(tmp_path):
    capture = _duplicate_capture(
        "empty.json", captured_at="2026-10-08T00:00:01Z", thread="first"
    ).model_copy(update={"history": [], "response": []}, deep=True)

    expected = build_scope([capture])
    actual = tuple(build_scope_bounded([capture], workspace_parent=tmp_path))

    assert actual == expected
    assert _artifacts(actual) == _artifacts(expected)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    "name",
    [
        name
        for name, value in vars(subagent_contracts).items()
        if name.startswith("test_")
        and callable(value)
        and not inspect.signature(value).parameters
    ],
)
def test_diagnostic_admission_preserves_existing_subagent_plans(monkeypatch, name):
    def checked(*args, **kwargs):
        expected = plan_subagent_mounts(*args, **kwargs)
        actual = plan_subagent_mounts(*args, **kwargs, diagnostic_max_bytes=1024 * 1024)
        assert actual == expected
        return actual

    monkeypatch.setattr(subagent_contracts, "plan_subagent_mounts", checked)
    getattr(subagent_contracts, name)()


def test_reverse_spawn_index_matches_scan_including_malformed_wildcards():
    leaves = []
    for index, (name, arguments, call_id, thread) in enumerate(
        [
            ("spawn_agent", {"task_name": "child"}, "call", "main"),
            ("Agent", '{"task_name":"child"}', "call", "main"),
            ("spawn_agent", "malformed", "call", "main"),
            ("spawn_agent", "[]", "call", "main"),
            ("spawn_agent", {"task_name": "other"}, "call", "main"),
            ("spawn_agent", {"task_name": "child"}, "another", "main"),
            ("spawn_agent", {"task_name": "child"}, "call", "other"),
            ("lookup", {"task_name": "child"}, "call", "main"),
        ]
    ):
        call = ToolCall(
            id=call_id, function=FunctionCall(name=name, arguments=arguments)
        )
        leaves.append(
            snapshot(
                f"leaf-{index}",
                thread=thread,
                turn=f"different-turn-{index}",
                response=[assistant(calls=[call, call])],
            )
        )
    index = _spawn_parent_index(leaves, max_bytes=100_000)
    assert index is not None
    for signature in ('{"task_name":"child"}', '{"task_name":"other"}', "{}"):
        event = SpawnEvidence(
            session_id="s",
            parent_thread_id="main",
            parent_turn_id="original-turn",
            spawn_call_id="call",
            task_name="",
            agent_name="",
            agent_name_conflicted=False,
            arguments_json=signature,
            source_path="event",
            spawn_call_name="spawn_agent",
        )
        expected = [
            i
            for i, leaf in enumerate(leaves)
            if (leaf.session_id, leaf.thread_id)
            == (event.session_id, event.parent_thread_id)
            and _contains_spawn(leaf, event)
        ]
        assert _indexed_spawn_candidates(index, event) == expected
    assert _spawn_parent_index(leaves, max_bytes=1) is None


@pytest.mark.parametrize("index_bytes", [0, 1, 1024 * 1024])
def test_replayed_ambiguous_events_share_candidates_even_after_index_fallback(
    index_bytes,
):
    call = spawn("same-call", "child")
    leaves = [
        snapshot(
            f"leaf-{i}",
            thread="main",
            turn=f"leaf-turn-{i}",
            response=[assistant(calls=[call])],
        )
        for i in range(30)
    ]
    events = [
        snapshot(
            f"event-{i}",
            thread="main",
            turn=f"event-turn-{i}",
            response=[assistant(calls=[call])],
        )
        for i in range(40)
    ]
    plan = plan_subagent_mounts(
        leaves,
        all_snapshots=events,
        parent_index_max_bytes=index_bytes,
        diagnostic_target_max_bytes=10_000,
    )
    reference = plan_subagent_mounts(
        leaves, all_snapshots=events, parent_index_max_bytes=0
    )
    assert plan == reference
    ambiguous = [
        item for item in plan.diagnostics if item.code == "ambiguous_parent_leaf"
    ]
    assert len(ambiguous) == 40
    assert plan.spawn_call_counts == (40,) * 30
    assert ambiguous[0].related_leaf_indices == tuple(range(30))
    assert all(
        item.related_leaf_indices is ambiguous[0].related_leaf_indices
        for item in ambiguous
    )
    with pytest.raises(SubagentPlanLimitError, match="candidate targets"):
        plan_subagent_mounts(
            leaves,
            all_snapshots=events,
            parent_index_max_bytes=index_bytes,
            diagnostic_target_max_bytes=1,
        )


def _artifacts(items):
    return [serialize_trajectory(item.node, item.origins) for item in items]


_BUILD_CASES = [
    ("test_build_scope_materializes_one_trajectory_without_sqlite", {}),
    ("test_build_scope_is_empty_for_no_snapshots", {}),
    ("test_final_identity_representative_prefers_pass_over_earlier_quarantine", {}),
    ("test_final_identity_representative_tiebreak_uses_full_source_path", {}),
    ("test_sessionless_prefixes_merge_by_user_and_outputs_remain_distinct", {}),
    *[
        (
            "test_origin_audit_matches_serial_materialized_trajectory",
            {"quarantined": flag},
        )
        for flag in (False, True)
    ],
    *[
        (
            "test_deduplicated_origins_retain_their_own_audit_and_match_serial",
            {"dirty_is_earlier": early, "reverse_input": reverse},
        )
        for early in (False, True)
        for reverse in (False, True)
    ],
    *[
        (
            "test_final_identity_deduplicates_threads_before_branch_numbering",
            {"session": session},
        )
        for session in ("s", "")
    ],
    *[
        (
            "test_spark_derived_audit_survives_json_and_output_round_trips",
            {"mounted_child": child, "call_id": call},
        )
        for child in (False, True)
        for call in (
            "password=fake",
            "Authorization: Bearer fake",
            "password=[REDACTED]",
            "x" * 1000 + " password=fake; " + "x" * 200,
        )
    ],
]


@pytest.mark.parametrize("name,arguments", _BUILD_CASES)
def test_existing_spark_contract_and_byte_exact_parity(
    monkeypatch, tmp_path, name, arguments
):
    def checked(items):
        values = list(items)
        expected = build_scope(item.model_copy(deep=True) for item in values)
        actual = tuple(
            build_scope_bounded(
                (item.model_copy(deep=True) for item in values),
                workspace_parent=tmp_path,
            )
        )
        assert actual == expected
        assert _artifacts(actual) == _artifacts(expected)
        assert list(tmp_path.glob("trajfoundry-spark-scope-*")) == []
        return actual

    monkeypatch.setattr(contracts, "build_scope", checked)
    test = getattr(contracts, name)
    kwargs = dict(arguments)
    if "tmp_path" in inspect.signature(test).parameters:
        kwargs["tmp_path"] = tmp_path
    test(**kwargs)


@pytest.mark.parametrize(
    "name",
    [
        "test_server_tool_duplicate_results_survive_contributor_merge",
        "test_conflicting_replayed_server_results_are_preserved_and_quarantined",
        "test_leaf_origin_priority_does_not_reorder_conflicting_server_variants",
        "test_replayed_server_tool_origin_priority_is_per_occurrence",
        "test_leaf_messages_stay_exact_while_contributor_evidence_is_unioned",
        "test_missing_identity_uses_explicit_metadata_sentinels",
        "test_multimodal_mapping_prefers_leaf_order_and_appends_prefix_only_parts",
    ],
)
def test_streaming_leaf_metadata_matches_existing_contract(monkeypatch, name):
    def checked(leaf, contributors):
        values = list(contributors)
        expected = reference_leaf(leaf, values)
        projected = (
            (
                item.model_copy(update={"history": [], "response": []}),
                _response_reasoning(item),
            )
            for item in values
        )
        actual = bounded._trajectory_from_leaf(leaf, projected)
        assert actual == expected
        return actual

    monkeypatch.setattr(pipeline_contracts, "_trajectory_from_leaf", checked)
    getattr(pipeline_contracts, name)()


@pytest.mark.parametrize("seed", range(5))
def test_cumulative_prefixes_branches_replays_and_flat_issue_union(tmp_path, seed):
    captures = []
    for index in range(24):
        branch = index % 3
        history = [user("shared")]
        for turn in range(index // 3):
            history.extend(
                [assistant(f"branch-{branch}-reply-{turn}"), user(f"next-{turn}")]
            )
        item = snapshot(
            f"capture-{index}",
            thread="main",
            turn=f"turn-{index}",
            history=history,
            response=[assistant(f"branch-{branch}-reply-{index // 3}")],
        )
        item.captured_at = f"2026-10-08T00:00:{23 - index:02d}Z"
        item.tools = [ToolDefinition(name=f"tool-{index % 4}")]
        item.instructions = f"instruction-{index % 2}"
        item.multimodal_file_mapping = [
            MediaMapping(part_id=f"part-{index}", object_name=f"image-{index}.png")
        ]
        captures.append(item)
    replay = captures[-1].model_copy(deep=True)
    replay.source_path = "replay.json"
    replay.source_sha256 = "different"
    replay.issues = [AuditIssue(code="synthetic_error", stage="provider")]
    captures.append(replay)
    random.Random(seed).shuffle(captures)
    actual = tuple(build_scope_bounded(iter(captures), workspace_parent=tmp_path))
    expected = build_scope(captures)
    assert actual == expected
    assert _artifacts(actual) == _artifacts(expected)


def _prefix_batch_capture(name, tokens, history_length):
    messages = [user(token) for token in tokens]
    capture = snapshot(
        name,
        thread="main",
        turn=name,
        history=messages[:history_length],
        response=messages[history_length:],
    )
    capture.session_id = ""
    capture.user_id = "batch-test-user"
    return capture


def _prefix_batch_boundary_captures(batch):
    # A's transcript and the scope root fill the INSERT batch exactly. B's
    # nodes stay pending; C closes B's stack and fills the independent covered
    # batch while only three new nodes await insertion.
    return [
        _prefix_batch_capture("A", ["a", *(["x"] * (batch - 2))], batch - 2),
        _prefix_batch_capture("B-short", ["b"], 0),
        _prefix_batch_capture("B-long", ["b", "z"], 1),
        _prefix_batch_capture("C", ["c"], 0),
    ]


def _assert_disk_prefix_index_matches_memory(captures, database):
    expected = streaming_prefix_leaves(captures)
    scope = bounded._DiskScope(database, BoundedBuildLimits())
    try:
        scope.ingest(captures)
        scope.index()
        actual_paths = {
            path
            for (path,) in scope.db.execute(
                "SELECT source_path FROM captures JOIN nodes ON endpoint=nodes.id "
                "WHERE covered=0"
            )
        }
        assert actual_paths == {leaf.source_path for leaf in expected.leaves}
        for leaf in expected.leaves:
            assert (
                tuple(path for (path,) in scope.paths(source_path=leaf.source_path))
                == expected.contributor_paths[leaf.source_path]
            )
    finally:
        scope.close()
    return expected


def test_prefix_covered_batch_inserts_pending_nodes_at_default_threshold(tmp_path):
    # Do not lower the production 20,000-node threshold for this regression.
    captures = _prefix_batch_boundary_captures(bounded._PREFIX_INSERT_BATCH)
    expected = _assert_disk_prefix_index_matches_memory(
        captures, tmp_path / "prefix-default-batch.sqlite"
    )
    assert {leaf.source_path for leaf in expected.leaves} == {
        "/A.json",
        "/B-long.json",
        "/C.json",
    }
    assert expected.contributor_paths["/B-long.json"] == (
        "/B-long.json",
        "/B-short.json",
    )


def test_prefix_covered_batches_preserve_realistic_length_chains(tmp_path):
    # Ordinary 121-127-message histories also cross the default SQL batches;
    # the failure does not require one giant 20,000-message request.
    captures = [
        _prefix_batch_capture(
            f"group-{group:04d}-length-{length}",
            [f"g{group:04d}", *(["x"] * (length - 1))],
            length - 1,
        )
        for group in range(400)
        for length in (121, 123, 125, 127)
    ]
    expected = _assert_disk_prefix_index_matches_memory(
        captures, tmp_path / "prefix-realistic-batches.sqlite"
    )
    assert len(expected.leaves) == 400
    assert all(len(paths) == 4 for paths in expected.contributor_paths.values())
    assert {
        path for paths in expected.contributor_paths.values() for path in paths
    } == {capture.source_path for capture in captures}


@pytest.mark.parametrize("reverse_input", [False, True])
@pytest.mark.parametrize("external_sort", [False, True])
def test_prefix_covered_batch_preserves_outputs_and_origins(
    monkeypatch, tmp_path, reverse_input, external_sort
):
    monkeypatch.setattr(bounded, "_PREFIX_INSERT_BATCH", 4)
    if external_sort:
        monkeypatch.setattr(bounded, "_PREFIX_RUN_BYTES", 1)
        monkeypatch.setattr(bounded, "_PREFIX_MERGE_FANIN", 2)
    captures = _prefix_batch_boundary_captures(4)
    if reverse_input:
        captures.reverse()
    expected = build_scope(captures)
    actual = tuple(build_scope_bounded(captures, workspace_parent=tmp_path))
    assert len(expected) == 3
    assert actual == expected
    assert _artifacts(actual) == _artifacts(expected)
    assert {origin["source_ref"] for item in actual for origin in item.origins} == {
        capture.source_path for capture in captures
    }
    assert sum(len(item.origins) for item in actual) == 4
    assert list(tmp_path.iterdir()) == []


def test_duplicate_source_is_last_writer_wins_before_prefix_index(tmp_path):
    first = _snapshot("same.json")
    overwritten = first.model_copy(
        update={"history": [], "response": [assistant("unrelated")]}
    )
    extension = first.model_copy(
        update={
            "source_path": "extension.json",
            "history": [*first.history, *first.response, user("next")],
            "response": [assistant("next answer")],
        }
    )
    for values in ([first, overwritten, extension], [overwritten, extension, first]):
        assert tuple(
            build_scope_bounded(values, workspace_parent=tmp_path)
        ) == build_scope(values)


def test_empty_transcript_and_ineligible_snapshot_do_not_distort_prefix(tmp_path):
    capture = _snapshot()
    empty = capture.model_copy(
        update={"source_path": "empty.json", "history": [], "response": []}
    )
    ineligible = capture.model_copy(
        update={
            "source_path": "count.json",
            "operation": "count_tokens",
            "history": [*capture.history, *capture.response, user("next")],
        }
    )
    values = [capture, empty, ineligible]
    assert tuple(build_scope_bounded(values, workspace_parent=tmp_path)) == build_scope(
        values
    )


def test_limits_validate_configuration():
    for name in BoundedBuildLimits.__dataclass_fields__:
        with pytest.raises(ValueError, match=name):
            BoundedBuildLimits(**{name: -1})
    assert BoundedBuildLimits(lookup_cache_bytes=0).lookup_cache_bytes == 0


@pytest.mark.parametrize(
    "field,limit,match",
    [
        ("max_snapshot_bytes", 1, "snapshot"),
        ("max_root_bytes", 1, "flat root"),
    ],
)
def test_oversized_indivisible_inputs_fail_closed_and_cleanup(
    tmp_path, field, limit, match
):
    limits = replace(BoundedBuildLimits(), **{field: limit})
    with pytest.raises(BoundedBuildLimitError, match=match):
        tuple(
            build_scope_bounded([_snapshot()], workspace_parent=tmp_path, limits=limits)
        )
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("field", ["max_routing_bytes", "max_routing_snapshots"])
def test_routing_admission_before_calling_planner(monkeypatch, tmp_path, field):
    event = snapshot(
        "event",
        thread="main",
        turn="1",
        history=[user("delegate")],
        response=[assistant(calls=[spawn("call", "child")])],
    )
    limits = replace(BoundedBuildLimits(), **{field: 1})

    def forbidden(*args, **kwargs):
        pytest.fail("oversized routing metadata reached planner")

    monkeypatch.setattr(bounded, "plan_subagent_mounts", forbidden)
    with pytest.raises(BoundedBuildLimitError, match="routing plan"):
        tuple(build_scope_bounded([event], workspace_parent=tmp_path, limits=limits))
    assert list(tmp_path.iterdir()) == []


def test_no_routing_scope_never_builds_scope_sized_plan(monkeypatch, tmp_path):
    def forbidden(*args, **kwargs):
        pytest.fail("no-routing scope materialized a routing plan")

    monkeypatch.setattr(bounded, "plan_subagent_mounts", forbidden)
    weak = []

    def captures():
        for index in range(150):
            item = _duplicate_capture(
                f"req-{index}.json",
                captured_at="2026-10-08T00:00:01Z",
                thread="main",
                session="",
            )
            item.history = [user(f"unique question {index}")]
            weak.append(weakref.ref(item))
            yield item
            assert sum(ref() is not None for ref in weak) <= 1

    limits = replace(
        BoundedBuildLimits(),
        max_routing_snapshots=1,
        max_routing_bytes=1,
        lookup_cache_bytes=0,
    )
    output = build_scope_bounded(captures(), workspace_parent=tmp_path, limits=limits)
    count = 0
    for item in output:
        count += 1
        assert item.node.metadata.sub_session_id == 0
        assert len(item.origins) == 1
    assert count == 150
    assert list(tmp_path.iterdir()) == []


def test_generator_close_releases_private_scratch_but_preserves_other_files(tmp_path):
    sentinel = tmp_path / "keep.txt"
    sentinel.write_text("untouched")
    output = build_scope_bounded([_snapshot()], workspace_parent=tmp_path)
    next(output)
    assert len(list(tmp_path.glob("trajfoundry-spark-scope-*"))) == 1
    output.close()
    assert list(tmp_path.iterdir()) == [sentinel]
    assert sentinel.read_text() == "untouched"


def test_materialization_failure_propagates_without_yielding(monkeypatch, tmp_path):
    def fail(*args, **kwargs):
        raise RuntimeError("synthetic quality error")

    monkeypatch.setattr(bounded, "enrich_trajectory", fail)
    with pytest.raises(RuntimeError, match="synthetic quality error"):
        next(build_scope_bounded([_snapshot()], workspace_parent=tmp_path))
    assert list(tmp_path.iterdir()) == []


def test_input_failure_propagates_and_releases_scratch(tmp_path):
    def failing():
        yield _snapshot()
        raise RuntimeError("input stream failed")

    with pytest.raises(RuntimeError, match="input stream failed"):
        next(build_scope_bounded(failing(), workspace_parent=tmp_path))
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    "case",
    ["orphan", "ambiguous", "missing-turn", "agent-conflict", "history-only", "cycle"],
)
def test_subagent_plan_edge_cases_match_in_memory(tmp_path, case):
    event = snapshot(
        "event",
        thread="main",
        turn="turn-1",
        history=[user("delegate")],
        response=[assistant(calls=[spawn("call", "child")])],
    )
    child = snapshot(
        "child",
        thread="child",
        turn="child-turn",
        parent_thread="main",
        parent_turn="turn-1",
        forked_from="main",
        marker='{"spawn_call_id":"call","task_name":"child"}',
        history=[user("child work")],
        response=[assistant("done")],
    )
    if case == "orphan":
        values = [child]
    elif case == "ambiguous":
        second = child.model_copy(
            update={"source_path": "second-child.json", "thread_id": "second-child"}
        )
        values = [event, child, second]
    elif case == "missing-turn":
        values = [event.model_copy(update={"turn_id": ""}), child]
    elif case == "agent-conflict":
        record = agent_message(
            "duplicate", author="parent", recipient="child", preceding=[]
        )
        event.agent_messages = [record, record.model_copy(deep=True)]
        unrelated = snapshot(
            "unrelated",
            thread="main",
            turn="else",
            history=[user("unrelated")],
            response=[assistant("standalone")],
        )
        values = [event, child, unrelated]
    elif case == "history-only":
        values = [
            event.model_copy(
                update={
                    "history": [*event.history, *event.response, user("next")],
                    "response": [assistant("done")],
                }
            )
        ]
    else:
        event.parent_thread_id = "child"
        event.parent_turn_id = "child-turn"
        event.forked_from_thread_id = "child"
        event.subagent_marker = '{"spawn_call_id":"back","task_name":"main"}'
        child.response = [assistant(calls=[spawn("back", "main")])]
        values = [event, child]
    for items in (values, list(reversed(values))):
        expected = build_scope(items)
        actual = tuple(build_scope_bounded(items, workspace_parent=tmp_path))
        assert actual == expected
        assert _artifacts(actual) == _artifacts(expected)


def test_prefix_digest_collision_still_compares_exact_tokens(monkeypatch, tmp_path):
    class ConstantHash:
        def digest(self):
            return b"constant"

    monkeypatch.setattr(bounded, "sha256", lambda payload: ConstantHash())
    first = _snapshot("first.json")
    other = first.model_copy(
        update={"source_path": "other.json", "history": [user("a different question")]}
    )
    extension = first.model_copy(
        update={
            "source_path": "extension.json",
            "history": [*first.history, *first.response, user("next")],
            "response": [assistant("next answer")],
        }
    )
    values = [first, other, extension]
    actual = tuple(build_scope_bounded(values, workspace_parent=tmp_path))
    assert actual == build_scope(values)
    assert len(actual) == 2


def test_merged_canonical_lineage_is_admitted_before_tuple_allocation(tmp_path):
    values = [
        _duplicate_capture(
            f"capture-{index}.json",
            captured_at="2026-10-08T00:00:01Z",
            thread=f"thread-{index}",
        )
        for index in range(100)
    ]
    assert len(build_scope(values)) == 1
    limits = replace(BoundedBuildLimits(), max_root_bytes=10_000)
    with pytest.raises(BoundedBuildLimitError, match="output and lineage"):
        next(build_scope_bounded(values, workspace_parent=tmp_path, limits=limits))
    assert list(tmp_path.iterdir()) == []


def test_committed_spool_batches_are_not_a_scope_size_limit(tmp_path):
    count = 1005

    def captures():
        for index in range(count):
            yield _duplicate_capture(
                f"capture-{index}.json",
                captured_at="2026-10-08T00:00:01Z",
                thread="main",
                session="",
            )

    actual = tuple(build_scope_bounded(captures(), workspace_parent=tmp_path))
    assert len(actual) == 1
    assert len(actual[0].origins) == count
    assert list(tmp_path.iterdir()) == []


def test_agent_conflict_diagnostics_share_thread_targets():
    leaves = [
        snapshot(f"leaf-{index}", thread="main", turn=str(index))
        for index in range(100)
    ]
    records = []
    for index in range(20):
        item = agent_message(
            f"duplicate-{index}", author="parent", recipient="child", preceding=[]
        )
        records.extend([item, item.model_copy(deep=True)])
    evidence = leaves[0].model_copy(update={"agent_messages": records})
    _, _, diagnostics = _agent_message_index(leaves, [evidence])
    assert len(diagnostics) == 20
    targets = diagnostics[0].related_leaf_indices
    assert targets == tuple(range(100))
    assert all(item.related_leaf_indices is targets for item in diagnostics)


def test_graph_diagnostics_use_shared_disk_relation_not_cartesian_rows(tmp_path):
    scope = bounded._DiskScope(tmp_path / "scratch.sqlite", BoundedBuildLimits())
    targets = tuple(range(1000))
    diagnostics = tuple(
        MountDiagnostic(
            code="duplicate_agent_message_id",
            detail=f"duplicate item {index}",
            related_leaf_indices=targets,
        )
        for index in range(50)
    )
    try:
        scope.store_graph_diagnostics(SimpleNamespace(diagnostics=diagnostics))
        assert (
            scope.db.execute("SELECT COUNT(*) FROM graph_targets").fetchone()[0] == 1000
        )
        assert scope.db.execute("SELECT COUNT(*) FROM graph_issues").fetchone()[0] == 50
        assert scope.db.execute("SELECT COUNT(*) FROM graph_links").fetchone()[0] == 50
        issues = scope.materialization_issues(
            "unused", 0, bounded._Budget(100_000, "test")
        )
        assert len(issues) == 50
        expected = sorted(
            (
                AuditIssue(
                    code=item.code, detail=item.detail, stage="subagents", path=""
                )
                for item in diagnostics
            ),
            key=bounded._issue_key,
        )
        assert issues == expected
    finally:
        scope.close()


def test_derived_diagnostics_have_separate_admission(tmp_path):
    scope = bounded._DiskScope(
        tmp_path / "scratch.sqlite",
        replace(BoundedBuildLimits(), max_routing_bytes=700),
    )
    diagnostic = MountDiagnostic(
        code="duplicate_agent_message_id",
        detail="duplicate",
        related_leaf_indices=tuple(range(100)),
    )
    try:
        with pytest.raises(BoundedBuildLimitError, match="derived routing diagnostics"):
            scope.store_graph_diagnostics(SimpleNamespace(diagnostics=(diagnostic,)))
        assert (
            scope.db.execute("SELECT COUNT(*) FROM graph_targets").fetchone()[0] < 100
        )
    finally:
        scope.close()


def test_root_diagnostic_budget_checked_before_decoding_any_issue(
    monkeypatch, tmp_path
):
    scope = bounded._DiskScope(tmp_path / "scratch.sqlite", BoundedBuildLimits())
    issue = AuditIssue(code="synthetic", detail="synthetic issue", stage="provider")
    try:
        scope.db.execute(
            "INSERT INTO flat_issues VALUES(?,?)", ("flat", bounded._issue_key(issue))
        )

        def forbidden(*args, **kwargs):
            pytest.fail("issue decoded before complete root diagnostic admission")

        monkeypatch.setattr(AuditIssue, "model_validate_json", forbidden)
        with pytest.raises(BoundedBuildLimitError, match="root diagnostics"):
            scope.materialization_issues(
                "flat", 0, bounded._Budget(1, "root diagnostics")
            )
    finally:
        scope.close()
