"""Small serializable records passed through the Spark pipeline."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ParsedSnapshotRow:
    """One parsed capture carried through Spark Shuffle, never persisted as a stage."""

    source_ref: str
    sha256: str
    scope_kind: str
    scope_id: str
    session_id: str
    user_id: str
    thread_id: str
    captured_at: str
    endpoint: str
    snapshot_json: bytes

    def as_tuple(self) -> tuple[object, ...]:
        return (
            self.source_ref,
            self.sha256,
            self.scope_kind,
            self.scope_id,
            self.session_id,
            self.user_id,
            self.thread_id,
            self.captured_at,
            self.endpoint,
            self.snapshot_json,
        )
