"""Disk-backed, fail-closed scope building for Spark's skewed user buckets.

Exact prefix matching and both deduplication stages match build_scope.
Only a single input snapshot/root and explicitly admitted routing metadata are
resident; scope-sized snapshots, trajectories and lineage remain in SQLite.
Limits cover serialized inputs and estimated diagnostic metadata, not absolute RSS.
SQLite scratch must live on Executor-local storage, never a shared checkpoint.
"""

from __future__ import annotations

import heapq
import json
import logging
import pickle
import shutil
import sqlite3
import tempfile
import time
from collections import OrderedDict
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from hashlib import sha256
from itertools import chain
from pathlib import Path
from typing import Any

import zstandard

from ..canonical import trajectory_id
from ..identity import aggregation_scope, normalize_session_id
from ..models import (
    AuditIssue,
    MediaMapping,
    Metadata,
    Severity,
    Snapshot,
    TrajectoryNode,
)
from ..pipeline import (
    _accept_local_relay_mount,
    _eligible,
    _flat_semantic_key,
    _initial_audit,
    _issue_key,
    _merge_contributor_metadata,
    _merged_issues,
    _minimal_evidence,
    _routing_snapshot,
    _snapshot_order_key,
    _source_file_and_line,
)
from ..quality import enrich_trajectory
from ..streaming import (
    _compatibility,
    _evidence_copy,
    _has_agent_messages,
    _has_linkage,
    _has_spawn_response,
    message_prefix_token,
)
from ..subagents import (
    SubagentPlanLimitError,
    diagnostic_applies_to_snapshot,
    plan_subagent_mounts,
)
from .build import BuiltTrajectory, _set_sub_session_id

logger = logging.getLogger(__name__)
_MIB = 1024**2
_STABLE = "captured_at,turn_id,request_id,source_path,source_sha256"
_PREFIX_RUN_BYTES = 64 * _MIB
_PREFIX_MERGE_FANIN = 64
_PREFIX_INSERT_BATCH = 20_000


class BoundedBuildLimitError(RuntimeError):
    """An indivisible root or its exact routing plan exceeds admission limits."""


@dataclass(frozen=True, slots=True)
class BoundedBuildLimits:
    sqlite_cache_mib: int = 256
    lookup_cache_bytes: int = 256 * _MIB
    max_snapshot_bytes: int = 2 * 1024 * _MIB
    max_root_bytes: int = 2 * 1024 * _MIB
    max_routing_bytes: int = 4 * 1024 * _MIB
    max_routing_snapshots: int = 2_000_000

    def __post_init__(self) -> None:
        for name in self.__dataclass_fields__:
            value = getattr(self, name)
            minimum = 0 if name == "lookup_cache_bytes" else 1
            if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
                raise ValueError(f"{name} must be an integer >= {minimum}")


class _Budget:
    def __init__(self, limit: int, phase: str):
        self.limit, self.phase, self.used = limit, phase, 0

    def add(self, amount: int) -> None:
        self.used += amount
        if self.used > self.limit:
            raise BoundedBuildLimitError(
                f"bounded Spark {self.phase} requires {self.used} serialized bytes; "
                f"limit={self.limit}; refusing to split semantic scope or skip records"
            )


class _Progress:
    def __init__(self, phase: str):
        self.phase = phase
        self.started = self.last_log = time.monotonic()

    def tick(self, count: int) -> None:
        now = time.monotonic()
        if now - self.last_log >= 60:
            logger.info(
                "Spark bounded scope: phase=%s processed=%d elapsed=%.1fs",
                self.phase,
                count,
                now - self.started,
            )
            self.last_log = now


class _ByteCache:
    """Small LRU with conservative accounting, including per-entry overhead."""

    def __init__(self, limit: int) -> None:
        self.limit = max(0, limit)
        self.bytes = 0
        self.items: OrderedDict[Any, tuple[Any, int]] = OrderedDict()

    def get(self, key: Any) -> Any:
        item = self.items.get(key)
        if item is None:
            return None
        self.items.move_to_end(key)
        return item[0]

    def put(self, key: Any, value: Any, payload_bytes: int) -> None:
        weight = payload_bytes + 384
        previous = self.items.pop(key, None)
        if previous is not None:
            self.bytes -= previous[1]
        if weight > self.limit:
            return
        while self.items and self.bytes + weight > self.limit:
            _, (_, old_weight) = self.items.popitem(last=False)
            self.bytes -= old_weight
        self.items[key] = value, weight
        self.bytes += weight


# These contributor helpers mirror the streaming local-pipeline algorithm.
# In particular, repeated transcript bodies are never retained for metadata
# aggregation; the SQL caller supplies body-free Snapshot projections.
def _contributor_changed_field_warnings(changed: set[str]) -> list[AuditIssue]:
    return [
        AuditIssue(
            code=f"contributor_{field}_changed",
            stage="aggregation",
            severity=Severity.WARNING,
            path=f"/{field}",
            detail=(
                f"prefix contributors contain {field} values different "
                "from the leaf; the leaf value was retained"
            ),
        )
        for field in ("instructions", "model", "harness", "termination")
        if field in changed
    ]


class _ContributorLeafSummary:
    """Retain only output media mappings and constant-size warning flags.

    Contributors can be one-shot iterators over disk metadata.  In particular,
    neither complete snapshots nor their repeated transcript histories survive
    the current iteration.  Media ordering is equivalent to a stable sort of
    captures, without retaining those captures merely to sort them.
    """

    def __init__(self, leaf: Snapshot):
        self.leaf = leaf
        self.changed: set[str] = set()
        self.user_id_changed = False
        self.has_leaf = False
        self.leaf_media: dict[str, MediaMapping] = {}
        for entry in leaf.multimodal_file_mapping:
            self.leaf_media[entry.part_id] = entry.model_copy(deep=True)
        self.prefix_media: dict[str, tuple[tuple[str, ...], int, MediaMapping]] = {}

    def observe(self, snapshot: Snapshot) -> None:
        self.has_leaf |= snapshot.source_path == self.leaf.source_path
        self.changed.update(
            field
            for field in ("instructions", "model", "harness", "termination")
            if getattr(snapshot, field) != getattr(self.leaf, field)
        )
        self.user_id_changed |= bool(
            snapshot.user_id and snapshot.user_id != self.leaf.user_id
        )
        order = _snapshot_order_key(snapshot)
        for ordinal, entry in enumerate(snapshot.multimodal_file_mapping):
            if entry.part_id in self.leaf_media:
                continue
            previous = self.prefix_media.get(entry.part_id)
            if previous is None or (order, ordinal) < previous[:2]:
                self.prefix_media[entry.part_id] = (
                    order,
                    ordinal,
                    entry.model_copy(deep=True),
                )

    def media(self) -> list[MediaMapping]:
        return [
            *self.leaf_media.values(),
            *[
                item[2]
                for item in sorted(
                    self.prefix_media.values(), key=lambda item: item[:2]
                )
            ],
        ]


def _trajectory_from_leaf(
    leaf: Snapshot, contributors: Iterable[Snapshot]
) -> TrajectoryNode:
    summary = _ContributorLeafSummary(leaf)
    session_id = normalize_session_id(leaf.session_id)

    def observed_contributors() -> Iterator[Snapshot]:
        for contributor in contributors:
            summary.observe(contributor)
            yield contributor
        if not summary.has_leaf:
            summary.observe(leaf)
            yield leaf

    tools, server_calls, agent_messages, compaction_items, contributor_issues = (
        _merge_contributor_metadata(
            observed_contributors(),
            server_origin_source_path=leaf.source_path,
        )
    )
    issues = _merged_issues(
        leaf.issues,
        contributor_issues,
        _contributor_changed_field_warnings(summary.changed),
    )
    synthesized_identity_issues: list[AuditIssue] = []
    if not leaf.user_id:
        synthesized_identity_issues.append(
            AuditIssue(
                code="metadata_user_id_synthesized",
                stage="aggregation",
                severity=Severity.WARNING,
                path="/metadata/user_id",
                detail="missing user id was published as no_user_id",
            )
        )
    if not session_id:
        synthesized_identity_issues.append(
            AuditIssue(
                code="metadata_session_id_synthesized",
                stage="aggregation",
                severity=Severity.WARNING,
                path="/metadata/session_id",
                detail="missing session id was published as no_session_id",
            )
        )
    issues = _merged_issues(issues, synthesized_identity_issues)
    multimodal_file_mapping = summary.media()
    basename, line_no = _source_file_and_line(leaf.source_path)
    source_type, specific_source = {
        "freerouter": ("api-router", "free-router"),
        "tokenplan": ("api-router", "token-plan"),
        "sxf": ("traj-cooperate", "SXF"),
        "deepinfra": ("api-router", "deep-infra"),
        "boyue": ("api-router", "boyue-rich"),
    }[leaf.source_name]
    if summary.user_id_changed:
        issues = _merged_issues(
            issues,
            [
                AuditIssue(
                    code="contributor_user_id_changed",
                    stage="aggregation",
                    severity=Severity.WARNING,
                    path="/user_id",
                    detail=(
                        "prefix contributors contain user_id values different "
                        "from the leaf; the leaf value was retained"
                    ),
                )
            ],
        )
    return TrajectoryNode(
        messages=[*leaf.history, *leaf.response],
        multimodal_file_mapping=multimodal_file_mapping,
        tools=tools,
        agent_messages=agent_messages,
        compaction_items=compaction_items,
        instructions=leaf.instructions,
        termination=leaf.termination,
        harness=leaf.harness,
        model=leaf.model,
        source=basename,
        server_tool_calls=server_calls,
        metadata=Metadata(
            source_file=basename,
            source_name=leaf.source_name,
            line_no=line_no,
            created_at=leaf.captured_at,
            model=leaf.model,
            user_id=leaf.user_id or "no_user_id",
            end_user_id=leaf.end_user_id,
            session_id=session_id or "no_session_id",
            sessionless_chunk_id=leaf.sessionless_chunk_id if not session_id else "",
            source_type=source_type,
            specific_source=specific_source,
        ),
        normalization_audit=_initial_audit(issues),
    )


class _DiskScope:
    def __init__(self, database: Path, limits: BoundedBuildLimits):
        self.limits = limits
        self.db = sqlite3.connect(database)
        self.prefix_runs = database.with_name(database.stem + "-prefix-runs")
        self.prefix_runs.mkdir(parents=True, exist_ok=True)
        self.compressor = zstandard.ZstdCompressor(level=1)
        self.decompressor = zstandard.ZstdDecompressor()
        self.nodes = _ByteCache(limits.lookup_cache_bytes)
        self.scopes = _ByteCache(min(4 * _MIB, limits.lookup_cache_bytes))
        try:
            self.db.execute("PRAGMA journal_mode=DELETE")
            self.db.execute("PRAGMA synchronous=OFF")
            self.db.execute("PRAGMA temp_store=FILE")
            self.db.execute(f"PRAGMA cache_size={-limits.sqlite_cache_mib * 1024}")
            self.db.execute("PRAGMA mmap_size=0")
            self.db.executescript("""
                CREATE TABLE captures(
                    source_path TEXT PRIMARY KEY, source_sha256 TEXT NOT NULL,
                    captured_at TEXT NOT NULL, turn_id TEXT NOT NULL,
                    request_id TEXT NOT NULL, session_id TEXT NOT NULL,
                    thread_id TEXT NOT NULL, payload BLOB NOT NULL,
                    raw_bytes INTEGER NOT NULL, metadata BLOB NOT NULL,
                    metadata_bytes INTEGER NOT NULL, eligible INTEGER NOT NULL,
                    evidence BLOB, evidence_bytes INTEGER NOT NULL,
                    has_routing INTEGER NOT NULL, endpoint INTEGER, prefix_end INTEGER,
                    aggregation_scope TEXT NOT NULL
                );
                CREATE TABLE scopes(scope_key BLOB PRIMARY KEY, root INTEGER NOT NULL);
                CREATE TABLE nodes(
                    id INTEGER PRIMARY KEY, parent INTEGER, token_hash BLOB,
                    token BLOB, covered INTEGER NOT NULL
                );
                CREATE TABLE flats(
                    flat_key TEXT PRIMARY KEY, source_path TEXT NOT NULL,
                    routing BLOB NOT NULL, routing_bytes INTEGER NOT NULL
                );
                CREATE TABLE flat_members(
                    flat_key TEXT NOT NULL, source_path TEXT PRIMARY KEY
                );
                CREATE INDEX members_flat ON flat_members(flat_key,source_path);
                CREATE TABLE flat_issues(
                    flat_key TEXT NOT NULL, issue BLOB NOT NULL,
                    PRIMARY KEY(flat_key,issue)
                );
                CREATE TABLE plan(
                    idx INTEGER PRIMARY KEY, flat_key TEXT NOT NULL,
                    source_path TEXT NOT NULL, root_order INTEGER, orphan INTEGER
                );
                CREATE TABLE edges(
                    parent INTEGER NOT NULL, child INTEGER NOT NULL,
                    call_id TEXT NOT NULL, relay_id TEXT NOT NULL
                );
                CREATE INDEX edges_parent ON edges(parent);
                CREATE INDEX plan_source ON plan(source_path);
                CREATE TABLE graph_issues(
                    id INTEGER PRIMARY KEY, issue BLOB NOT NULL UNIQUE
                );
                CREATE TABLE graph_targets(
                    target_set INTEGER NOT NULL, idx INTEGER NOT NULL,
                    PRIMARY KEY(target_set,idx)
                );
                CREATE INDEX graph_targets_idx ON graph_targets(idx,target_set);
                CREATE TABLE graph_links(
                    target_set INTEGER NOT NULL, issue_id INTEGER NOT NULL,
                    PRIMARY KEY(target_set,issue_id)
                );
                CREATE TABLE outputs(
                    identifier TEXT PRIMARY KEY, payload BLOB NOT NULL,
                    raw_bytes INTEGER NOT NULL, priority INTEGER NOT NULL,
                    representative TEXT NOT NULL, created_at TEXT NOT NULL,
                    source_file TEXT NOT NULL, synthesized INTEGER NOT NULL,
                    branch INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE origins(
                    identifier TEXT NOT NULL, source_path TEXT NOT NULL,
                    captured_at TEXT NOT NULL, sha256 TEXT NOT NULL,
                    disposition TEXT NOT NULL, reason_codes TEXT NOT NULL,
                    PRIMARY KEY(identifier,captured_at,source_path,sha256)
                );
            """)
        except BaseException:
            self.db.close()
            raise

    def close(self) -> None:
        try:
            self.db.close()
        finally:
            shutil.rmtree(self.prefix_runs, ignore_errors=True)

    def encode(self, model: Snapshot | TrajectoryNode) -> tuple[bytes, int]:
        raw = model.model_dump_json().encode("utf-8")
        return self.compressor.compress(raw), len(raw)

    def snapshot(
        self, source_path: str, budget: _Budget, *, metadata=False
    ) -> Snapshot:
        columns = "metadata,metadata_bytes" if metadata else "payload,raw_bytes"
        row = self.db.execute(
            f"SELECT {columns} FROM captures WHERE source_path=?", (source_path,)
        ).fetchone()
        if row is None:
            raise RuntimeError("bounded Spark snapshot disappeared from scratch")
        budget.add(row[1] + 512)
        return Snapshot.model_validate_json(self.decompressor.decompress(row[0]))

    def ingest(self, snapshots: Iterable[Snapshot]) -> None:
        progress = _Progress("spool")
        for count, item in enumerate(snapshots, 1):
            payload, size = self.encode(item)
            _Budget(self.limits.max_snapshot_bytes, "snapshot").add(size)
            projection = item.model_copy(update={"history": [], "response": []})
            metadata, metadata_size = self.encode(projection)
            eligible = _eligible(item)
            is_evidence = eligible and (
                _has_spawn_response(item)
                or _has_linkage(item)
                or _has_agent_messages(item)
            )
            evidence, evidence_size = (
                self.encode(_evidence_copy(item)) if is_evidence else (None, 0)
            )
            # A history-only spawn can affect routing evidence even when its
            # source response was not captured. Conservatively retain the exact
            # existing planner whenever any routing-related field is present.
            routing = _routing_snapshot(item) if eligible else None
            has_routing = bool(
                eligible and (is_evidence or (routing is not None and routing.history))
            )
            # Match build_scope's dict comprehension: duplicate source paths
            # are last-writer-wins BEFORE prefix coverage is computed.
            self.db.execute(
                "INSERT OR REPLACE INTO captures VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    item.source_path,
                    item.source_sha256,
                    item.captured_at,
                    item.turn_id,
                    item.request_id,
                    item.session_id,
                    item.thread_id,
                    payload,
                    size,
                    metadata,
                    metadata_size,
                    int(eligible),
                    evidence,
                    evidence_size,
                    int(has_routing),
                    None,
                    None,
                    json.dumps(
                        aggregation_scope(
                            item.session_id,
                            item.user_id,
                            item.captured_at,
                            item.sessionless_chunk_id,
                        )
                    ),
                ),
            )
            if count % 1000 == 0:
                self.db.commit()
            progress.tick(count)
        self.db.commit()
        if (
            len(
                self.db.execute(
                    "SELECT DISTINCT aggregation_scope FROM captures LIMIT 2"
                ).fetchall()
            )
            > 1
        ):
            raise ValueError(
                "bounded builder requires one aggregation scope; "
                "group captures by session or user/chunk before building"
            )
        self.db.execute(f"CREATE INDEX captures_stable ON captures({_STABLE})")
        self.db.execute(
            "CREATE INDEX captures_thread ON captures(thread_id,session_id)"
        )

    def scope_root(self, item: Snapshot) -> int:
        # Keys are serialized, never unpickled. Compatibility contains only
        # immutable tuples, strings and the exact compaction signature bytes.
        key = pickle.dumps(_compatibility(item), protocol=5)
        cached = self.scopes.get(key)
        if cached is not None:
            return cached
        row = self.db.execute(
            "SELECT root FROM scopes WHERE scope_key=?", (key,)
        ).fetchone()
        if row:
            root = row[0]
        else:
            root = self.db.execute(
                "INSERT INTO nodes(parent,token_hash,token,covered) VALUES(NULL,NULL,NULL,0)"
            ).lastrowid
            self.db.execute("INSERT INTO scopes VALUES(?,?)", (key, root))
        self.scopes.put(key, root, len(key))
        return root

    def advance(self, parent: int, token: bytes, *, covered: bool) -> int:
        digest = sha256(token).digest()
        key = parent, digest, token
        cached = self.nodes.get(key)
        if cached is None:
            row = self.db.execute(
                "SELECT id,covered FROM nodes WHERE parent=? AND token_hash=? AND token=?",
                (parent, digest, token),
            ).fetchone()
            if row:
                node, was_covered = row
            else:
                node = self.db.execute(
                    "INSERT INTO nodes(parent,token_hash,token,covered) VALUES(?,?,?,?)",
                    (parent, digest, token, int(covered)),
                ).lastrowid
                was_covered = covered
        else:
            node, was_covered = cached
        if covered and not was_covered:
            self.db.execute("UPDATE nodes SET covered=1 WHERE id=?", (node,))
            was_covered = True
        self.nodes.put(key, (node, was_covered), len(token) + len(digest))
        return node

    def _prefix_records(self) -> Iterator[tuple[bytes, tuple[bytes, ...], int, tuple[str, ...], str]]:
        """Yield compact sortable transcript records.

        The old implementation looked up every token edge in SQLite.  That
        makes a large scope a random-I/O workload even though all records are
        immutable.  This projection contains only the compatibility key,
        canonical message tokens, history length and stable source order;
        payloads stay compressed in ``captures`` and are loaded later during
        materialization.
        """

        progress = _Progress("prefix-spool")
        for count, (path, captured_at, turn_id, request_id, source_sha256) in enumerate(
            self.db.execute(
                "SELECT source_path,captured_at,turn_id,request_id,source_sha256 "
                f"FROM captures WHERE eligible=1 ORDER BY {_STABLE}"
            ),
            1,
        ):
            item = self.snapshot(
                path, _Budget(self.limits.max_snapshot_bytes + 512, "snapshot")
            )
            # Keep the complete compatibility tuple, including the
            # compaction signature.  Compaction is part of the exact
            # build_scope scope even though it is not visible in the
            # published identity.
            compatibility = _compatibility(item)
            scope_key = pickle.dumps(compatibility, protocol=5)
            tokens = tuple(
                message_prefix_token(message)
                for message in chain(item.history, item.response)
            )
            stable = (captured_at, turn_id, request_id, path, source_sha256)
            yield scope_key, tokens, len(item.history), stable, path
            progress.tick(count)

    @staticmethod
    def _prefix_sort_key(
        record: tuple[bytes, tuple[bytes, ...], int, tuple[str, ...], str]
    ) -> tuple[bytes, tuple[bytes, ...], tuple[str, ...]]:
        return record[0], record[1], record[3]

    def _write_prefix_run(
        self,
        records: list[tuple[bytes, tuple[bytes, ...], int, tuple[str, ...], str]],
        run_number: int,
    ) -> Path:
        records.sort(key=self._prefix_sort_key)
        path = self.prefix_runs / f"run-{run_number:08d}.pkl"
        with path.open("wb") as handle:
            for record in records:
                pickle.dump(record, handle, protocol=5)
        return path

    @staticmethod
    def _read_prefix_run(path: Path) -> Iterator[tuple[bytes, tuple[bytes, ...], int, tuple[str, ...], str]]:
        with path.open("rb") as handle:
            while True:
                try:
                    yield pickle.load(handle)
                except EOFError:
                    return

    def _merge_prefix_run_group(self, paths: list[Path], destination: Path) -> None:
        streams = [self._read_prefix_run(path) for path in paths]
        heap: list[tuple[tuple[bytes, tuple[bytes, ...], tuple[str, ...]], int, Any]] = []
        for index, stream in enumerate(streams):
            try:
                record = next(stream)
            except StopIteration:
                continue
            heapq.heappush(heap, (self._prefix_sort_key(record), index, record))
        with destination.open("wb") as handle:
            while heap:
                _key, index, record = heapq.heappop(heap)
                pickle.dump(record, handle, protocol=5)
                try:
                    next_record = next(streams[index])
                except StopIteration:
                    continue
                heapq.heappush(
                    heap,
                    (self._prefix_sort_key(next_record), index, next_record),
                )

    def _sorted_prefix_records(
        self,
    ) -> Iterator[tuple[bytes, tuple[bytes, ...], int, tuple[str, ...], str]]:
        """External-sort compact transcript keys with bounded Python memory."""

        runs: list[Path] = []
        batch: list[tuple[bytes, tuple[bytes, ...], int, tuple[str, ...], str]] = []
        batch_bytes = 0
        run_number = 0
        for record in self._prefix_records():
            batch.append(record)
            batch_bytes += sum(len(token) for token in record[1]) + len(record[4]) + 512
            if batch_bytes >= _PREFIX_RUN_BYTES:
                runs.append(self._write_prefix_run(batch, run_number))
                run_number += 1
                batch = []
                batch_bytes = 0
        if batch:
            runs.append(self._write_prefix_run(batch, run_number))

        if not runs:
            return

        # Keep file descriptors bounded for very large scopes.  Each pass is
        # still sequential; it only changes the number of sorted runs.
        pass_number = 0
        while len(runs) > _PREFIX_MERGE_FANIN:
            merged: list[Path] = []
            for offset in range(0, len(runs), _PREFIX_MERGE_FANIN):
                group = runs[offset : offset + _PREFIX_MERGE_FANIN]
                destination = self.prefix_runs / f"merge-{pass_number:04d}-{offset:08d}.pkl"
                self._merge_prefix_run_group(group, destination)
                merged.append(destination)
                for path in group:
                    path.unlink(missing_ok=True)
            runs = merged
            pass_number += 1

        streams = [self._read_prefix_run(path) for path in runs]
        heap: list[tuple[tuple[bytes, tuple[bytes, ...], tuple[str, ...]], int, Any]] = []
        for index, stream in enumerate(streams):
            try:
                record = next(stream)
            except StopIteration:
                continue
            heapq.heappush(heap, (self._prefix_sort_key(record), index, record))
        while heap:
            _key, index, record = heapq.heappop(heap)
            yield record
            try:
                next_record = next(streams[index])
            except StopIteration:
                continue
            heapq.heappush(
                heap,
                (self._prefix_sort_key(next_record), index, next_record),
            )
        for path in runs:
            path.unlink(missing_ok=True)

    def index(self) -> None:
        """Build the exact prefix trie with one sorted streaming pass.

        Records are sorted by compatibility scope and complete canonical
        transcript.  A lexicographic sequence makes all descendants of an
        endpoint contiguous, so the active stack replaces the old per-token
        SQLite ``SELECT/INSERT/UPDATE`` loop.  Nodes are assigned monotonically
        and inserted in batches; no edge lookup or child index is required.
        """

        progress = _Progress("prefix-sort-scan")
        next_node_id = 1
        pending_nodes: list[tuple[int, int | None, None, None, int]] = []
        pending_captures: list[tuple[int | None, int | None, str]] = []
        pending_covered: list[tuple[int]] = []
        current_scope: bytes | None = None
        root: int | None = None
        root_covered = False
        stack: list[list[Any]] = []
        previous_tokens: tuple[bytes, ...] = ()
        processed = 0

        def flush_nodes() -> None:
            if pending_nodes:
                self.db.executemany(
                    "INSERT INTO nodes(id,parent,token_hash,token,covered) VALUES(?,?,?,?,?)",
                    pending_nodes,
                )
                pending_nodes.clear()

        def flush_captures() -> None:
            if pending_captures:
                self.db.executemany(
                    "UPDATE captures SET endpoint=?,prefix_end=? WHERE source_path=?",
                    pending_captures,
                )
                pending_captures.clear()

        def flush_covered() -> None:
            if pending_covered:
                # Coverage and node batches fill independently. An UPDATE
                # against an uninserted node is silently lost by SQLite, so
                # insert every pending node before marking covered prefixes.
                flush_nodes()
                self.db.executemany(
                    "UPDATE nodes SET covered=1 WHERE id=?", pending_covered
                )
                pending_covered.clear()

        def close_stack(until: int = 0) -> None:
            nonlocal root_covered
            while len(stack) > until:
                frame = stack.pop()
                if frame[1]:
                    pending_covered.append((frame[0],))
            if until == 0 and root is not None and root_covered:
                pending_covered.append((root,))
                root_covered = False

        for scope_key, tokens, history_length, _stable, path in self._sorted_prefix_records():
            processed += 1
            if scope_key != current_scope:
                close_stack()
                flush_nodes()
                flush_covered()
                current_scope = scope_key
                root = next_node_id
                next_node_id += 1
                pending_nodes.append((root, None, None, None, 0))
                stack = []
                previous_tokens = ()
                root_covered = False

                self.db.execute(
                    "INSERT INTO scopes(scope_key,root) VALUES(?,?)", (scope_key, root)
                )

            common = 0
            limit = min(len(previous_tokens), len(tokens))
            while common < limit and previous_tokens[common] == tokens[common]:
                common += 1
            close_stack(common)
            assert root is not None
            for token in tokens[common:]:
                parent = root if not stack else stack[-1][0]
                node = next_node_id
                next_node_id += 1
                pending_nodes.append((node, parent, None, None, 0))
                stack.append([node, False, token])

            length = len(tokens)
            # An empty eligible transcript is a valid terminal root.  It has
            # no strict prefix endpoint; in particular, do not turn
            # ``length - 1`` into -1 and index the active stack from the end.
            prefix_depth = min(history_length, length - 1) if length else 0
            prefix_end: int | None = None
            endpoint = root
            if length:
                endpoint = stack[-1][0]
                # Every non-empty transcript extends the scope root, even
                # when its history has already reached the first message.
                # This keeps an empty eligible capture from becoming a leaf
                # merely because it sorted before the first real transcript.
                root_covered = True
                if prefix_depth == 0:
                    prefix_end = root
                else:
                    prefix_end = stack[prefix_depth - 1][0]
                for frame in stack[: max(prefix_depth, 0)]:
                    frame[1] = True
            pending_captures.append((endpoint, prefix_end, path))
            previous_tokens = tokens

            if len(pending_nodes) >= _PREFIX_INSERT_BATCH:
                flush_nodes()
            if len(pending_captures) >= _PREFIX_INSERT_BATCH:
                flush_captures()
            if len(pending_covered) >= _PREFIX_INSERT_BATCH:
                flush_covered()
            if processed % 1000 == 0:
                self.db.commit()
            progress.tick(processed)

        close_stack()
        flush_nodes()
        flush_captures()
        flush_covered()
        self.db.commit()
        self.db.execute(
            "CREATE INDEX captures_endpoint ON captures(endpoint,source_path)"
        )
        self.nodes.items.clear()
        self.nodes.bytes = 0
        self.scopes.items.clear()
        self.scopes.bytes = 0

    def paths(self, *, source_path: str | None = None, flat_key: str | None = None):
        if source_path is not None:
            members = "SELECT source_path FROM captures WHERE source_path=?"
            parameters = (source_path,)
        elif flat_key is not None:
            members = "SELECT source_path FROM flat_members WHERE flat_key=?"
            parameters = (flat_key,)
        else:
            raise ValueError("a source or flat key is required")
        # UNION in the recursive CTE deduplicates shared ancestors. Never
        # persist the potentially quadratic leaf x contributor relationship.
        return self.db.execute(
            "WITH RECURSIVE members(path) AS (" + members + "), "
            "ancestors(id) AS ("
            "SELECT prefix_end FROM captures JOIN members ON source_path=path "
            "WHERE prefix_end IS NOT NULL UNION "
            "SELECT parent FROM nodes JOIN ancestors ON nodes.id=ancestors.id "
            "WHERE parent IS NOT NULL) "
            "SELECT source_path FROM captures WHERE endpoint IN (SELECT id FROM ancestors) "
            "UNION SELECT path FROM members ORDER BY source_path",
            parameters,
        )

    def contributors(self, paths, budget: _Budget) -> Iterator[Snapshot]:
        for (path,) in paths:
            yield self.snapshot(path, budget, metadata=True)

    def flatten(self) -> None:
        progress = _Progress("flat-dedup")
        for count, (path,) in enumerate(
            self.db.execute(
                "SELECT source_path FROM captures JOIN nodes ON endpoint=nodes.id "
                f"WHERE covered=0 ORDER BY {_STABLE}"
            ),
            1,
        ):
            budget = _Budget(self.limits.max_root_bytes, "flat root")
            leaf = self.snapshot(path, budget)
            candidate = _trajectory_from_leaf(
                leaf, self.contributors(self.paths(source_path=path), budget)
            )
            key = _flat_semantic_key(leaf, candidate)
            routing, routing_size = self.encode(_routing_snapshot(leaf))
            # Stable source iteration makes the first representative exactly
            # the minimum _snapshot_order_key, as in build_scope.
            self.db.execute(
                "INSERT OR IGNORE INTO flats VALUES(?,?,?,?)",
                (key, path, routing, routing_size),
            )
            self.db.execute("INSERT INTO flat_members VALUES(?,?)", (key, path))
            for issue in (
                candidate.normalization_audit.issues
                if candidate.normalization_audit
                else ()
            ):
                self.db.execute(
                    "INSERT OR IGNORE INTO flat_issues VALUES(?,?)",
                    (key, _issue_key(issue)),
                )
            evidence, evidence_size = self.encode(_minimal_evidence(leaf))
            self.db.execute(
                "UPDATE captures SET evidence=?,evidence_bytes=? WHERE source_path=? AND evidence IS NULL",
                (evidence, evidence_size, path),
            )
            if count % 1000 == 0:
                self.db.commit()
            progress.tick(count)
        self.db.commit()

    def plan(self) -> None:
        has_routing = self.db.execute(
            "SELECT 1 FROM captures WHERE has_routing=1 LIMIT 1"
        ).fetchone()
        if not has_routing:
            # With no spawn, linkage or agent-message evidence, the exact
            # planner has no edges, orphans or diagnostics. Its leaf ordering
            # reduces to these columns (routing transcripts are all empty).
            rows = self.db.execute(
                "SELECT flat_key,flats.source_path FROM flats JOIN captures USING(source_path) "
                "ORDER BY session_id,thread_id,captured_at,turn_id,request_id,source_path,source_sha256"
            )
            for index, (key, path) in enumerate(rows):
                self.db.execute(
                    "INSERT INTO plan VALUES(?,?,?,?,?)", (index, key, path, index, 0)
                )
            self.db.commit()
            return
        counts = self.db.execute(
            "SELECT (SELECT COUNT(*) FROM flats)+(SELECT COUNT(*) FROM captures WHERE evidence IS NOT NULL),"
            "COALESCE((SELECT SUM(routing_bytes) FROM flats),0)+"
            "COALESCE((SELECT SUM(evidence_bytes) FROM captures),0)"
        ).fetchone()
        if counts[0] > self.limits.max_routing_snapshots:
            raise BoundedBuildLimitError(
                f"bounded Spark exact routing plan has {counts[0]} snapshots; "
                f"limit={self.limits.max_routing_snapshots}; refusing partial subagent plan"
            )
        _Budget(self.limits.max_routing_bytes, "routing plan").add(
            counts[1] + counts[0] * 512
        )
        routing = []
        by_path = {}
        for key, path, payload in self.db.execute(
            "SELECT flat_key,source_path,routing FROM flats ORDER BY flat_key"
        ):
            routing.append(
                Snapshot.model_validate_json(self.decompressor.decompress(payload))
            )
            by_path[path] = key
        evidence = [
            Snapshot.model_validate_json(self.decompressor.decompress(payload))
            for (payload,) in self.db.execute(
                f"SELECT evidence FROM captures WHERE evidence IS NOT NULL ORDER BY {_STABLE}"
            )
        ]
        try:
            plan = plan_subagent_mounts(
                routing,
                all_snapshots=evidence,
                diagnostic_target_max_bytes=self.limits.max_routing_bytes,
                diagnostic_max_bytes=self.limits.max_routing_bytes,
            )
        except SubagentPlanLimitError as error:
            raise BoundedBuildLimitError(str(error)) from error
        for index, item in enumerate(plan.leaves):
            self.db.execute(
                "INSERT INTO plan VALUES(?,?,?,NULL,0)",
                (index, by_path[item.source_path], item.source_path),
            )
        for order, (index, orphan) in enumerate(
            chain(
                ((index, False) for index in plan.main_root_indices),
                ((index, True) for index in plan.orphan_indices),
            )
        ):
            self.db.execute(
                "UPDATE plan SET root_order=?,orphan=? WHERE idx=?",
                (order, int(orphan), index),
            )
        for edge in plan.edges:
            self.db.execute(
                "INSERT INTO edges VALUES(?,?,?,?)",
                (
                    edge.parent_index,
                    edge.child_index,
                    edge.spawn_call_id,
                    edge.relay_id,
                ),
            )
        self.store_graph_diagnostics(plan)
        self.db.commit()

    def store_graph_diagnostics(self, plan) -> None:
        """Store target relations, never D x L Python AuditIssue objects.

        Thread-conflict/cycle diagnostics share their target tuple in the
        planner. Reuse that relation in SQLite too: N diagnostics over L leaves
        require N + L rows, not N * L copied issue payloads or target rows.
        """
        budget = _Budget(self.limits.max_routing_bytes, "derived routing diagnostics")
        shared_sets: dict[tuple[Any, ...], int] = {}
        next_set = 0
        progress = _Progress("routing-diagnostics")

        def target_set(key, targets) -> int:
            nonlocal next_set
            found = shared_sets.get(key) if key is not None else None
            if found is not None:
                return found
            number = next_set
            next_set += 1
            budget.add(128)
            if key is not None:
                shared_sets[key] = number
            for index in targets:
                # Admission precedes insertion. Unshared or broad wildcard
                # diagnostics cannot silently consume unbounded scratch space.
                budget.add(32)
                self.db.execute(
                    "INSERT OR IGNORE INTO graph_targets VALUES(?,?)", (number, index)
                )
            return number

        for count, diagnostic in enumerate(plan.diagnostics, 1):
            issue = AuditIssue(
                code=diagnostic.code,
                stage="subagents",
                path="",
                detail=diagnostic.detail,
            )
            payload = _issue_key(issue)
            budget.add(len(payload) + 512)
            self.db.execute(
                "INSERT OR IGNORE INTO graph_issues(issue) VALUES(?)", (payload,)
            )
            issue_id = self.db.execute(
                "SELECT id FROM graph_issues WHERE issue=?", (payload,)
            ).fetchone()[0]
            groups = []
            related = diagnostic.related_leaf_indices
            if related:
                groups.append(target_set(("shared", id(related)), related))
            if diagnostic.leaf_index is not None:
                index = diagnostic.leaf_index
                groups.append(target_set(("leaf", index), (index,)))
            if (
                not related
                and diagnostic.leaf_index is None
                and diagnostic.parent_thread_id
            ):
                # Use SQL to restrict thread/session candidates. The existing
                # predicate remains authoritative for call/turn/signature
                # matching, including missing-parent and legacy wildcard rules.
                query = "SELECT idx FROM plan JOIN captures USING(source_path) WHERE thread_id=?"
                params = [diagnostic.parent_thread_id]
                if diagnostic.session_id:
                    query += " AND session_id=?"
                    params.append(diagnostic.session_id)
                targets = (
                    index
                    for (index,) in self.db.execute(query, params)
                    if diagnostic_applies_to_snapshot(diagnostic, plan.leaves[index])
                )
                groups.append(
                    target_set(
                        (
                            "predicate",
                            diagnostic.code,
                            diagnostic.session_id,
                            diagnostic.parent_thread_id,
                            diagnostic.parent_turn_id,
                            diagnostic.spawn_call_id,
                            diagnostic.spawn_call_name,
                            diagnostic.spawn_arguments_json,
                        ),
                        targets,
                    )
                )
            for group in groups:
                budget.add(32)
                self.db.execute(
                    "INSERT OR IGNORE INTO graph_links VALUES(?,?)", (group, issue_id)
                )
            progress.tick(count)

    def materialization_issues(
        self, key: str, index: int, budget: _Budget
    ) -> list[AuditIssue]:
        query = (
            "SELECT issue FROM flat_issues WHERE flat_key=? UNION "
            "SELECT graph_issues.issue FROM graph_targets "
            "JOIN graph_links USING(target_set) JOIN graph_issues ON graph_issues.id=issue_id "
            "WHERE idx=?"
        )
        size = self.db.execute(
            "SELECT COALESCE(SUM(length(issue)+512),0) FROM (" + query + ")",
            (key, index),
        ).fetchone()[0]
        # Reject an indivisible root's full derived diagnostic set before any
        # issue object is decoded, not after building most of its Python list.
        budget.add(size)
        return [
            AuditIssue.model_validate_json(payload)
            for (payload,) in self.db.execute(query, (key, index))
        ]

    def materialize(
        self, index: int, budget: _Budget, depth: int = 0
    ) -> tuple[TrajectoryNode, Snapshot]:
        if depth > 128:
            raise BoundedBuildLimitError("bounded Spark subagent depth exceeds 128")
        key, path = self.db.execute(
            "SELECT flat_key,source_path FROM plan WHERE idx=?", (index,)
        ).fetchone()
        leaf = self.snapshot(path, budget)
        node = _trajectory_from_leaf(
            leaf, self.contributors(self.paths(flat_key=key), budget)
        )
        issues = self.materialization_issues(key, index, budget)
        node.normalization_audit = _initial_audit(
            _merged_issues(node.normalization_audit.issues, issues)
        )
        children, relays = {}, {}
        for child_index, call_id, relay_id in self.db.execute(
            "SELECT child,call_id,relay_id FROM edges WHERE parent=? ORDER BY call_id,child",
            (index,),
        ):
            child, _ = self.materialize(child_index, budget, depth + 1)
            children[call_id] = child
            if relay_id and _accept_local_relay_mount(
                node, call_id=call_id, relay_id=relay_id
            ):
                relays[call_id] = relay_id
        node.sub_agent_trajectory = children or None
        node.sub_agent_relay_mounts = relays or None
        return node, leaf

    def store_roots(self) -> None:
        progress = _Progress("materialize")
        for count, (index, orphan) in enumerate(
            self.db.execute(
                "SELECT idx,orphan FROM plan WHERE root_order IS NOT NULL ORDER BY root_order"
            ),
            1,
        ):
            node, representative = self.materialize(
                index, _Budget(self.limits.max_root_bytes, "root")
            )
            if orphan:
                node.sub_agent_trajectory = None
                node.sub_agent_relay_mounts = None
            node = enrich_trajectory(node, top_level=True, is_subagent=bool(orphan))
            audit = node.normalization_audit or _initial_audit([])
            identifier = trajectory_id(node)
            representative_key = (
                {"pass": 0, "quarantined": 1, "excluded": 2}.get(audit.tag.value, 9),
                f"{representative.captured_at}\x1f{representative.source_path}\x1f{representative.source_sha256}",
            )
            previous = self.db.execute(
                "SELECT priority,representative FROM outputs WHERE identifier=?",
                (identifier,),
            ).fetchone()
            if previous is None or representative_key < previous:
                payload, size = self.encode(node)
                _Budget(self.limits.max_root_bytes, "published root").add(size)
                synthesized = not normalize_session_id(node.metadata.session_id) or any(
                    issue.code == "metadata_session_id_synthesized"
                    for issue in audit.issues
                )
                self.db.execute(
                    "INSERT OR REPLACE INTO outputs VALUES(?,?,?,?,?,?,?,?,0)",
                    (
                        identifier,
                        payload,
                        size,
                        *representative_key,
                        node.metadata.created_at,
                        node.metadata.source_file,
                        int(synthesized),
                    ),
                )
            # Preserve each origin's first root audit, not the audit of a
            # later, cleaner final representative. SQL keeps huge lineage off
            # the Python heap, including canonical dedup across many roots.
            self.db.execute(
                "WITH RECURSIVE descendants(idx) AS (SELECT ? UNION "
                "SELECT child FROM edges JOIN descendants ON parent=descendants.idx), "
                "members(path) AS (SELECT flat_members.source_path FROM flat_members "
                "JOIN plan USING(flat_key) JOIN descendants USING(idx)), "
                "ancestors(id) AS (SELECT prefix_end FROM captures JOIN members ON source_path=path "
                "WHERE prefix_end IS NOT NULL UNION SELECT parent FROM nodes JOIN ancestors "
                "ON nodes.id=ancestors.id WHERE parent IS NOT NULL), "
                "paths(path) AS (SELECT source_path FROM captures WHERE endpoint IN (SELECT id FROM ancestors) "
                "UNION SELECT path FROM members) "
                "INSERT OR IGNORE INTO origins SELECT ?,captures.source_path,captured_at,source_sha256,?,? "
                "FROM captures JOIN paths ON captures.source_path=paths.path",
                (
                    index,
                    identifier,
                    audit.tag.value,
                    json.dumps(audit.reason_codes, ensure_ascii=False),
                ),
            )
            if count % 100 == 0:
                self.db.commit()
            progress.tick(count)
        self.db.commit()
        for branch, (identifier,) in enumerate(
            self.db.execute(
                "SELECT identifier FROM outputs WHERE synthesized=0 ORDER BY created_at,identifier,source_file"
            )
        ):
            self.db.execute(
                "UPDATE outputs SET branch=? WHERE identifier=?", (branch, identifier)
            )
        self.db.commit()

    def outputs(self) -> Iterator[BuiltTrajectory]:
        for identifier, payload, raw_size, branch in self.db.execute(
            "SELECT identifier,payload,raw_bytes,branch FROM outputs ORDER BY identifier"
        ):
            budget = _Budget(self.limits.max_root_bytes, "output and lineage")
            budget.add(raw_size)
            # Check the complete merged lineage budget BEFORE constructing its
            # tuple. A single output contract must fit; no record is truncated.
            origin_bytes = self.db.execute(
                "SELECT COALESCE(SUM(length(CAST(source_path AS BLOB))+length(CAST(captured_at AS BLOB))+"
                "length(CAST(sha256 AS BLOB))+length(CAST(disposition AS BLOB))+"
                "length(CAST(reason_codes AS BLOB))+512),0) FROM origins WHERE identifier=?",
                (identifier,),
            ).fetchone()[0]
            budget.add(origin_bytes)
            node = TrajectoryNode.model_validate_json(
                self.decompressor.decompress(payload)
            )
            _set_sub_session_id(node, branch)
            origins = tuple(
                {
                    "source_ref": path,
                    "sha256": digest,
                    "captured_at": captured_at,
                    "disposition": disposition,
                    "reason_codes": json.loads(reasons),
                }
                for path, captured_at, digest, disposition, reasons in self.db.execute(
                    "SELECT source_path,captured_at,sha256,disposition,reason_codes FROM origins "
                    "WHERE identifier=? ORDER BY captured_at,source_path,sha256",
                    (identifier,),
                )
            )
            yield BuiltTrajectory(node=node, origins=origins)


def build_scope_bounded(
    snapshots: Iterable[Snapshot],
    *,
    workspace_parent: str | Path | None = None,
    limits: BoundedBuildLimits | None = None,
) -> Iterator[BuiltTrajectory]:
    """Consume a scope once, spill exact state, then yield deduplicated roots.

    Input must belong to one session or missing-session user/chunk scope;
    mixed scopes fail before prefix aggregation or subagent planning.
    Close the iterator when stopping early so its private scratch directory is
    released. Any admission, disk, materialization or quality error propagates;
    callers must not publish a successful manifest after such an error.
    """
    limits = limits or BoundedBuildLimits()
    with tempfile.TemporaryDirectory(
        prefix="trajfoundry-spark-scope-", dir=workspace_parent
    ) as scratch:
        scope = _DiskScope(Path(scratch) / "scope.sqlite", limits)
        try:
            scope.ingest(snapshots)
            scope.index()
            scope.flatten()
            scope.plan()
            scope.store_roots()
            yield from scope.outputs()
        finally:
            scope.close()


__all__ = ["BoundedBuildLimitError", "BoundedBuildLimits", "build_scope_bounded"]
