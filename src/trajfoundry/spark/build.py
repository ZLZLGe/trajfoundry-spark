"""SQLite-free, in-memory trajectory materialization for Spark partitions."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass

from ..canonical import trajectory_id
from ..models import AuditIssue, Snapshot, TrajectoryNode
from ..pipeline import (
    _accept_local_relay_mount,
    _eligible,
    _flat_semantic_key,
    _FlatLeaf,
    _initial_audit,
    _issue_key,
    _merged_issues,
    _minimal_evidence,
    _mount_issues_by_index,
    _routing_snapshot,
    _snapshot_identity,
    _snapshot_order_key,
    _trajectory_from_leaf,
)
from ..quality import enrich_trajectory
from ..streaming import streaming_prefix_leaves
from ..subagents import plan_subagent_mounts


@dataclass(frozen=True, slots=True)
class BuiltTrajectory:
    node: TrajectoryNode
    origins: tuple[dict[str, str], ...]


def _descendants(plan: object, root_index: int) -> set[int]:
    children: dict[int, list[int]] = defaultdict(list)
    for edge in plan.edges:
        children[edge.parent_index].append(edge.child_index)
    found: set[int] = set()
    pending = [root_index]
    while pending:
        index = pending.pop()
        if index in found:
            continue
        found.add(index)
        pending.extend(children.get(index, ()))
    return found


def _set_sub_session_id(node: TrajectoryNode, value: int) -> None:
    node.metadata.sub_session_id = value
    for child in (node.sub_agent_trajectory or {}).values():
        _set_sub_session_id(child, value)


def _materialize(
    snapshots: dict[str, Snapshot],
    leaves: dict[int, _FlatLeaf],
    graph_issues: dict[int, list[AuditIssue]],
    edges_by_parent: dict[int, list[tuple[str, int, str]]],
    index: int,
) -> tuple[TrajectoryNode, Snapshot]:
    flat = leaves[index]
    representative = snapshots.get(flat.routing_snapshot.source_path)
    if representative is None:
        raise RuntimeError(f"missing representative snapshot: {flat.routing_snapshot.source_path}")
    node = _trajectory_from_leaf(
        representative,
        (snapshots[path] for path in sorted(flat.contributor_paths)),
    )
    existing = node.normalization_audit
    node.normalization_audit = _initial_audit(
        _merged_issues(
            existing.issues if existing else (),
            flat.issues.values(),
            graph_issues.get(index, ()),
        )
    )
    children: dict[str, TrajectoryNode] = {}
    relays: dict[str, str] = {}
    for call_id, child_index, relay_id in edges_by_parent.get(index, ()):
        child, _ = _materialize(snapshots, leaves, graph_issues, edges_by_parent, child_index)
        children[call_id] = child
        if relay_id and _accept_local_relay_mount(node, call_id=call_id, relay_id=relay_id):
            relays[call_id] = relay_id
    node.sub_agent_trajectory = children or None
    node.sub_agent_relay_mounts = relays or None
    return node, representative


def build_scope(snapshots: Iterable[Snapshot]) -> tuple[BuiltTrajectory, ...]:
    """Aggregate one session/user scope with the same semantics as the local pipeline.

    The scope is intentionally the unit shuffled by Spark.  It is bounded by
    one session (or one no-session user bucket), and no SQLite snapshot is
    created or persisted.
    """

    all_snapshots = {snapshot.source_path: snapshot for snapshot in snapshots}
    if not all_snapshots:
        return ()
    ordered_snapshots = tuple(sorted(all_snapshots.values(), key=_snapshot_order_key))
    result = streaming_prefix_leaves(
        snapshot for snapshot in ordered_snapshots if _eligible(snapshot)
    )
    flat_leaves: dict[str, _FlatLeaf] = {}
    evidence_by_path: dict[str, Snapshot] = {}
    for evidence in result.spawn_evidence:
        evidence_by_path[evidence.source_path] = evidence
    for leaf in result.leaves:
        evidence_by_path.setdefault(leaf.source_path, _minimal_evidence(leaf))
        paths = result.contributor_paths.get(leaf.source_path, (leaf.source_path,))
        contributors = [all_snapshots[path] for path in paths if path in all_snapshots]
        candidate = _trajectory_from_leaf(leaf, contributors)
        key = _flat_semantic_key(leaf, candidate)
        issues = candidate.normalization_audit.issues if candidate.normalization_audit else ()
        existing = flat_leaves.get(key)
        if existing is None:
            flat_leaves[key] = _FlatLeaf(
                routing_snapshot=_routing_snapshot(leaf),
                contributor_paths=set(paths),
                issues={_issue_key(issue): issue for issue in issues},
            )
        else:
            existing.contributor_paths.update(paths)
            for issue in issues:
                existing.issues[_issue_key(issue)] = issue
            if _snapshot_order_key(leaf) < _snapshot_order_key(existing.routing_snapshot):
                existing.routing_snapshot = _routing_snapshot(leaf)
    if not flat_leaves:
        return ()

    routing_leaves = [flat.routing_snapshot for _, flat in sorted(flat_leaves.items())]
    plan = plan_subagent_mounts(routing_leaves, all_snapshots=tuple(evidence_by_path.values()))
    leaves_by_identity = {
        _snapshot_identity(flat.routing_snapshot): flat for flat in flat_leaves.values()
    }
    leaves_by_index = {
        index: leaves_by_identity[_snapshot_identity(snapshot)]
        for index, snapshot in enumerate(plan.leaves)
    }
    graph_issues = _mount_issues_by_index(plan)
    edges_by_parent: dict[int, list[tuple[str, int, str]]] = defaultdict(list)
    for edge in plan.edges:
        edges_by_parent[edge.parent_index].append(
            (edge.spawn_call_id, edge.child_index, edge.relay_id)
        )

    built: list[tuple[BuiltTrajectory, bool]] = []
    root_specs = [
        *((index, False) for index in plan.main_root_indices),
        *((index, True) for index in plan.orphan_indices),
    ]
    for root_index, is_subagent in root_specs:
        node, _representative = _materialize(
            all_snapshots,
            leaves_by_index,
            graph_issues,
            edges_by_parent,
            root_index,
        )
        if is_subagent:
            node.sub_agent_trajectory = None
            node.sub_agent_relay_mounts = None
        node = enrich_trajectory(node, top_level=True, is_subagent=is_subagent)
        descendant_indices = _descendants(plan, root_index)
        origin_paths = sorted(
            {
                path
                for index in descendant_indices
                for path in leaves_by_index[index].contributor_paths
            }
        )
        origins = tuple(
            {
                "source_ref": path,
                "sha256": all_snapshots[path].source_sha256,
                "captured_at": all_snapshots[path].captured_at,
            }
            for path in origin_paths
            if path in all_snapshots
        )
        built.append((BuiltTrajectory(node=node, origins=origins), is_subagent))

    # Match StateStore.assign_sub_session_ids: synthesized identities remain 0;
    # real branches are numbered by creation time, identity, and source file.
    real = []
    for item in built:
        audit = item[0].node.normalization_audit
        if audit is None or not any(
            issue.code == "metadata_session_id_synthesized" for issue in audit.issues
        ):
            real.append(item)
    real.sort(
        key=lambda item: (
            item[0].node.metadata.created_at,
            trajectory_id(item[0].node),
            item[0].node.metadata.source_file,
        )
    )
    for sub_session_id, (item, _) in enumerate(real):
        _set_sub_session_id(item.node, sub_session_id)
    real_item_ids = {id(item) for item, _ in real}
    for item, _ in built:
        if id(item) not in real_item_ids:
            _set_sub_session_id(item.node, 0)
    return tuple(item for item, _ in built)


__all__ = ["BuiltTrajectory", "build_scope"]
