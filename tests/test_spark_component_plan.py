from __future__ import annotations

import pickle
import random
from collections import defaultdict
from functools import reduce

import pytest

from trajfoundry.models import (
    AgentMessageEvidence,
    AuditIssue,
    CompactionRecord,
    FunctionCall,
    Message,
    Snapshot,
    ToolCall,
)
from trajfoundry.spark.build import build_scope
from trajfoundry.spark.component_plan import (
    ADAPTIVE_PREFIX_MESSAGES,
    MAX_PREFIX_MESSAGES,
    WHOLE_SCOPE_COMPONENT,
    ScopeComponentSummary,
    component_key,
    merge_summaries,
    summarize_snapshot,
)
from trajfoundry.spark.output import serialize_trajectory


def _message(role: str, content: str) -> Message:
    return Message(
        role=role,
        content=content,
        reasoning_content="" if role == "assistant" else None,
    )


def _capture(path: str, *, question: str = "question", **updates) -> Snapshot:
    return Snapshot(
        source_path=path,
        source_sha256="a" * 64,
        source_name="tokenplan",
        session_id="",
        thread_id=path,
        provider="openai",
        operation="responses",
        outcome="success",
        user_id="user",
        captured_at="2026-09-15T00:00:00Z",
        history=[_message("system", "shared"), _message("user", question)],
        response=[_message("assistant", "answer")],
        wire_complete=True,
    ).model_copy(update=updates, deep=True)


def _summary(captures) -> ScopeComponentSummary:
    return reduce(
        merge_summaries,
        map(summarize_snapshot, captures),
        ScopeComponentSummary(),
    )


def _artifacts(captures):
    return sorted(
        (
            serialize_trajectory(item.node, item.origins)
            for item in build_scope(captures)
        ),
        key=lambda item: item.trajectory_id,
    )


def _split_artifacts(captures):
    summary = _summary(captures)
    groups = defaultdict(list)
    for capture in captures:
        groups[component_key(capture, summary)].append(capture)
    return sorted(
        (item for group in groups.values() for item in _artifacts(group)),
        key=lambda item: item.trajectory_id,
    )


def test_summary_reduction_is_order_independent_and_has_identity() -> None:
    summaries = [
        ScopeComponentSummary(1, 8),
        ScopeComponentSummary(1, None),
        ScopeComponentSummary(1, 2, True),
        ScopeComponentSummary(1, 0),
    ]
    assert reduce(merge_summaries, summaries) == ScopeComponentSummary(4, 0, True)
    assert reduce(merge_summaries, reversed(summaries)) == ScopeComponentSummary(
        4, 0, True
    )
    for summary in summaries:
        assert merge_summaries(summary, ScopeComponentSummary()) == summary
        assert merge_summaries(ScopeComponentSummary(), summary) == summary
    assert merge_summaries(
        merge_summaries(summaries[0], summaries[1]), summaries[2]
    ) == merge_summaries(summaries[0], merge_summaries(summaries[1], summaries[2]))


def test_summary_and_keys_are_compact_and_do_not_retain_message_contents() -> None:
    capture = _capture("private-source", question="private-content" * 100_000)
    summary = _summary([capture])
    key = component_key(capture, summary)
    assert len(pickle.dumps(summary)) < 256
    assert "private" not in repr(summary)
    assert len(key) == 67
    assert "private" not in key
    assert pickle.loads(pickle.dumps(summary)) == summary


def test_different_questions_split_despite_common_system_message() -> None:
    captures = [_capture("first", question="one"), _capture("second", question="two")]
    summary = _summary(captures)
    assert summary == ScopeComponentSummary(2, 3, False)
    assert len({component_key(item, summary) for item in captures}) == 2
    assert _split_artifacts(captures) == _artifacts(captures)


def test_prefix_chain_keeps_contributors_across_protocols_and_threads() -> None:
    first = _capture("first")
    second = _capture(
        "second",
        operation="chat_completions",
        history=[*first.history, *first.response, _message("user", "next")],
        response=[_message("assistant", "done")],
    )
    independent = _capture("independent", question="other")
    summary = _summary([first, second, independent])
    assert component_key(first, summary) == component_key(second, summary)
    assert component_key(first, summary) != component_key(independent, summary)
    expected = _artifacts([first, second, independent])
    assert len(expected) == 2
    assert sorted(len(item.origins) for item in expected) == [1, 2]
    assert _split_artifacts([first, second, independent]) == expected


@pytest.mark.parametrize("short_length", [0, 1, 2])
def test_short_capture_prevents_cutting_through_prefix(short_length) -> None:
    normal = _capture("long")
    short = _capture("short", history=normal.history[:short_length], response=[])
    summary = _summary([normal, short])
    assert summary.min_length == short_length
    assert component_key(normal, summary) == component_key(short, summary)
    assert _split_artifacts([normal, short]) == _artifacts([normal, short])


def test_maximum_component_prefix_length_is_four() -> None:
    first = _capture(
        "first",
        history=[_message("user", str(index)) for index in range(12)],
    )
    second = first.model_copy(update={"source_path": "second"}, deep=True)
    second.history[-1].content = "different"
    summary = _summary([first, second])
    assert MAX_PREFIX_MESSAGES == 4
    assert component_key(first, summary).startswith("p4:")
    assert component_key(first, summary) == component_key(second, summary)


def test_large_safe_scope_uses_longer_prefix_without_cutting_true_prefixes() -> None:
    history = [_message("user", f"message-{index}") for index in range(40)]
    first = _capture("first", history=history, response=[])
    same_prefix = first.model_copy(update={"source_path": "same-prefix"}, deep=True)
    same_prefix.history[-1].content = "different-after-adaptive-prefix"
    different_prefix = first.model_copy(update={"source_path": "different"}, deep=True)
    different_prefix.history[5].content = "different-before-adaptive-prefix"
    summary = ScopeComponentSummary(4_096, 40, False)

    first_key = component_key(first, summary)
    assert ADAPTIVE_PREFIX_MESSAGES == 32
    assert first_key.startswith("p32:")
    assert component_key(same_prefix, summary) == first_key
    assert component_key(different_prefix, summary) != first_key


@pytest.mark.parametrize(
    "updates",
    [
        {"session_id": "real-session"},
        {"subagent_marker": "child"},
        {"parent_thread_id": "parent"},
        {"parent_turn_id": "turn"},
        {"forked_from_thread_id": "parent"},
        {
            "agent_messages": [
                AgentMessageEvidence(
                    origin="history", item_index=0, item={"type": "agent_message"}
                )
            ]
        },
    ],
)
def test_any_routing_evidence_or_real_session_disables_whole_scope(updates) -> None:
    first = _capture("first")
    unsafe = _capture("unsafe", question="different", **updates)
    summary = _summary([first, unsafe])
    assert summary.unsafe
    assert component_key(first, summary) == WHOLE_SCOPE_COMPONENT
    assert component_key(unsafe, summary) == WHOLE_SCOPE_COMPONENT
    assert _split_artifacts([first, unsafe]) == _artifacts([first, unsafe])


@pytest.mark.parametrize("origin", ["history", "response"])
def test_spawn_calls_even_in_history_disable_split(origin) -> None:
    spawn = Message(
        role="assistant",
        content="",
        reasoning_content="",
        tool_calls=[
            ToolCall(
                id="spawn-1", function=FunctionCall(name="spawn_agent", arguments={})
            )
        ],
    )
    unsafe = _capture("unsafe", **{origin: [spawn]})
    safe = _capture("safe")
    summary = _summary([unsafe, safe])
    assert summary.unsafe
    assert component_key(safe, summary) == WHOLE_SCOPE_COMPONENT


def test_ineligible_captures_do_not_shorten_eligible_component_prefix() -> None:
    ignored = _capture("ignored", history=[], response=[], wire_complete=False)
    captures = [_capture("first"), _capture("second", question="different"), ignored]
    summary = _summary(captures)
    assert summary == ScopeComponentSummary(3, 3, False)
    assert component_key(ignored, summary) == WHOLE_SCOPE_COMPONENT
    assert _split_artifacts(captures) == _artifacts(captures)


def test_ineligible_routing_evidence_still_disables_split_conservatively() -> None:
    ignored = _capture("ignored", wire_complete=False, parent_thread_id="parent")
    summary = _summary([_capture("first"), ignored])
    assert summary.unsafe


def test_summary_without_eligible_captures_or_missing_summary_never_splits() -> None:
    ignored = _capture("ignored", operation="count_tokens")
    assert _summary([ignored]) == ScopeComponentSummary(1, None, False)
    assert component_key(ignored, _summary([ignored])) == WHOLE_SCOPE_COMPONENT
    assert (
        component_key(_capture("first"), ScopeComponentSummary())
        == WHOLE_SCOPE_COMPONENT
    )


def test_summary_that_cannot_cover_capture_fails_closed() -> None:
    with pytest.raises(ValueError, match="does not cover"):
        component_key(
            _capture("first", history=[], response=[]), ScopeComponentSummary(1, 3)
        )


def test_reasoning_and_empty_tool_call_projection_do_not_split_final_duplicates() -> (
    None
):
    first = _capture("first")
    second = first.model_copy(
        update={"source_path": "second", "thread_id": "second"}, deep=True
    )
    second.response[0].tool_calls = []
    summary = _summary([first, second])
    assert component_key(first, summary) == component_key(second, summary)
    # A mutated [] is invalid in the current Message contract.  The planner
    # still coalesces it, leaving normal validation to the actual builder.
    second.response[0].tool_calls = None
    expected = _artifacts([first, second])
    assert len(expected) == 1
    assert len(expected[0].origins) == 2
    assert _split_artifacts([first, second]) == expected

    third = second.model_copy(update={"source_path": "third"}, deep=True)
    third.response[0].reasoning_content = "different reasoning"
    assert component_key(first, _summary([first, third])) == component_key(
        third, _summary([first, third])
    )
    assert _split_artifacts([first, third]) == _artifacts([first, third])


def test_canonical_dedup_retains_each_origin_audit_after_component_split() -> None:
    clean = _capture("clean")
    dirty = _capture(
        "dirty", issues=[AuditIssue(code="synthetic_error", stage="provider")]
    )
    other = _capture("other", question="independent")
    captures = [dirty, other, clean]
    assert len(_artifacts(captures)) == 2
    assert _split_artifacts(captures) == _artifacts(captures)


def test_compaction_and_masked_identity_do_not_create_unsafe_finer_keys() -> None:
    first = _capture("first")
    compacted = _capture(
        "compacted",
        compaction_items=[
            CompactionRecord(
                origin="history",
                item_index=0,
                item={"type": "compaction", "encrypted_content": "opaque"},
            )
        ],
    )
    masked = _capture(
        "masked",
        issues=[AuditIssue(code="metadata_session_id_masked", stage="identity")],
    )
    captures = [first, compacted, masked]
    summary = _summary(captures)
    assert len({component_key(item, summary) for item in captures}) == 1
    assert _split_artifacts(captures) == _artifacts(captures)


def test_prefix_key_conservatively_ignores_different_tool_call_arguments() -> None:
    first = _capture("first")
    first.response[0].tool_calls = [
        ToolCall(id="call", function=FunctionCall(name="lookup", arguments={"q": 1}))
    ]
    second = first.model_copy(update={"source_path": "second"}, deep=True)
    second.response[0].tool_calls[0].function.arguments = {"q": 2}
    summary = _summary([first, second])
    assert component_key(first, summary) == component_key(second, summary)
    expected = _artifacts([first, second])
    assert len(expected) == 2
    assert _split_artifacts([first, second]) == expected


def test_literal_no_session_sentinel_and_missing_session_share_prefix_component() -> (
    None
):
    first = _capture("first")
    second = _capture("second", session_id="no_session_id")
    summary = _summary([first, second])
    assert not summary.unsafe
    assert component_key(first, summary) == component_key(second, summary)
    assert _split_artifacts([first, second]) == _artifacts([first, second])


@pytest.mark.parametrize("seed", range(8))
def test_randomized_prefix_branches_and_duplicates_match_unsplit_builder(seed) -> None:
    rng = random.Random(seed)
    captures = []
    for index in range(24):
        history = [
            _message("system", "shared"),
            _message("user", str(rng.randrange(4))),
        ]
        for _ in range(rng.randrange(3)):
            history.extend(
                [
                    _message("assistant", "answer"),
                    _message("user", str(rng.randrange(3))),
                ]
            )
        captures.append(
            _capture(
                str(index),
                history=history,
                operation=rng.choice(["responses", "chat_completions"]),
            )
        )
    rng.shuffle(captures)
    assert _split_artifacts(captures) == _artifacts(captures)
