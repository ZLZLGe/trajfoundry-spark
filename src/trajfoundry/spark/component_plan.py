"""Conservative, constant-size planning hints for parallel missing-session scopes.

These are *prefix components*, not request buckets: every pair that can merge
must retain the same component key.  Protocol, thread, request id and timestamps
must never be used as independent split boundaries for a missing-session user.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from hashlib import sha256
from itertools import chain, islice

from ..canonical import canonical_json
from ..identity import normalize_session_id
from ..models import Message, Snapshot
from ..pipeline import _eligible
from ..tool_names import is_spawn_tool_name

MAX_PREFIX_MESSAGES = 4
# Large no-session users often share a generic system/user preamble.  A
# four-message component is then still a single reducer hot spot.  For large
# scopes we safely use a longer prefix: any true prefix pair must share every
# message up to the shorter capture, so increasing K cannot separate a valid
# contributor pair.  Small scopes keep the historical K=4 key for stability.
ADAPTIVE_PREFIX_MESSAGES = 32
ADAPTIVE_COMPONENT_THRESHOLD = 4_096
WHOLE_SCOPE_COMPONENT = "whole"


@dataclass(frozen=True, slots=True)
class ScopeComponentSummary:
    """A mergeable O(1) summary; it never retains messages or source paths.

    ``min_length`` counts full messages of eligible captures only.  Empty
    captures force zero and consequently disable splitting.  ``unsafe`` is
    monotonic: one capture with routing evidence disables the whole scope.
    """

    count: int = 0
    min_length: int | None = None
    unsafe: bool = False


def _messages(snapshot: Snapshot) -> Iterable[Message]:
    return chain(snapshot.history, snapshot.response)


def _has_routing_evidence(snapshot: Snapshot) -> bool:
    if (
        snapshot.subagent_marker
        or snapshot.parent_thread_id
        or snapshot.parent_turn_id
        or snapshot.forked_from_thread_id
        or snapshot.agent_messages
    ):
        return True
    # Include history as well as response.  A retained leaf's history can
    # participate in mount planning even without a response-side spawn event.
    return any(
        is_spawn_tool_name(call.function.name)
        for message in _messages(snapshot)
        for call in message.tool_calls or ()
    )


def summarize_snapshot(snapshot: Snapshot) -> ScopeComponentSummary:
    """Summarize a capture without assuming Responses requests are independent."""

    return ScopeComponentSummary(
        count=1,
        min_length=(
            len(snapshot.history) + len(snapshot.response)
            if _eligible(snapshot)
            else None
        ),
        # Real sessions require scope-wide branch numbering after canonical
        # deduplication.  Preserve the complete scope for that existing path.
        unsafe=bool(normalize_session_id(snapshot.session_id))
        or _has_routing_evidence(snapshot),
    )


def merge_summaries(
    left: ScopeComponentSummary, right: ScopeComponentSummary
) -> ScopeComponentSummary:
    """Associative, commutative reduction suitable for Spark reduceByKey."""

    lengths = [
        value for value in (left.min_length, right.min_length) if value is not None
    ]
    return ScopeComponentSummary(
        count=left.count + right.count,
        min_length=min(lengths) if lengths else None,
        unsafe=left.unsafe or right.unsafe,
    )


def component_key(snapshot: Snapshot, summary: ScopeComponentSummary) -> str:
    """Return a split key only when the *complete scope* proves it is safe.

    Let K be min(4, the shortest eligible full transcript) for ordinary
    scopes, or min(32, the shortest eligible full transcript) once a large
    safe scope crosses the adaptive threshold.  Exact prefix
    contributors and their leaves necessarily share their first K messages.
    Use only role/content here: those fields are preserved both by exact
    prefix matching and by published canonical identity.  In particular,
    ignoring reasoning and tool-call details is deliberately conservative:
    ``tool_calls=None`` and ``[]`` may differ in the prefix trie but have the
    same published projection.  Extra coalescing costs parallelism, not data.

    Without routing evidence, materialization cannot join separate message
    prefixes.  Missing-session branch ids are always zero, and any roots that
    finally deduplicate also share this key.  Hash collisions merely coalesce
    components; equality is still established by the downstream builder.

    The caller must reduce all captures in the original session/user scope
    before invoking this function.  Never derive the summary from a sample or
    a partition-local subset.  Missing/oversized summary maps should fall back
    to the whole-scope builder, not to an assumed safe default.
    """

    if (
        summary.unsafe
        or summary.count <= 0
        or summary.min_length is None
        or summary.min_length <= 0
        or not _eligible(snapshot)
    ):
        return WHOLE_SCOPE_COMPONENT
    # Defense in depth for caller mistakes.  These checks do not replace the
    # complete-scope reduction, but never route an explicitly unsafe capture
    # down the optimized path.
    if normalize_session_id(snapshot.session_id) or _has_routing_evidence(snapshot):
        return WHOLE_SCOPE_COMPONENT
    component_limit = (
        ADAPTIVE_PREFIX_MESSAGES
        if summary.count >= ADAPTIVE_COMPONENT_THRESHOLD
        else MAX_PREFIX_MESSAGES
    )
    prefix_length = min(component_limit, summary.min_length)
    if len(snapshot.history) + len(snapshot.response) < prefix_length:
        raise ValueError("component summary does not cover this eligible capture")
    digest = sha256(b"trajfoundry-prefix-component-v1\0")
    for message in islice(_messages(snapshot), prefix_length):
        token = canonical_json([message.role, message.content])
        digest.update(len(token).to_bytes(8, "big"))
        digest.update(token)
    return f"p{prefix_length}:{digest.hexdigest()}"


__all__ = [
    "ADAPTIVE_COMPONENT_THRESHOLD",
    "ADAPTIVE_PREFIX_MESSAGES",
    "MAX_PREFIX_MESSAGES",
    "WHOLE_SCOPE_COMPONENT",
    "ScopeComponentSummary",
    "component_key",
    "merge_summaries",
    "summarize_snapshot",
]
