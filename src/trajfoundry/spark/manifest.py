"""Disk-backed output inventory; no O(outputs) Python collection on Driver."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from ..export import SCHEMA_VERSION, ExportStats
from ..feedback import feedback_fields, unix_ms
from ..json_codec import dumps
from .output import SparkObjectDescription


class DiskManifestAccumulator:
    """Ephemeral local metadata with uniqueness enforced before publication."""

    def __init__(
        self,
        path: Path,
        *,
        input_root: str,
        input_format: str,
        config_hash: str,
        started_at_ms: int | None = None,
    ):
        self.input_root = input_root
        self.input_format = input_format
        self.config_hash = config_hash
        self.started_at_ms = unix_ms() if started_at_ms is None else started_at_ms
        self.stats = ExportStats()
        self.connection = sqlite3.connect(path)
        self.connection.execute("PRAGMA temp_store=FILE")
        self.connection.execute("PRAGMA cache_size=-4096")
        self.connection.execute(
            "CREATE TABLE objects(path TEXT PRIMARY KEY, sha256 TEXT, bytes INTEGER, "
            "trajectory_id TEXT UNIQUE)"
        )
        self.object_count = 0

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.connection.close()

    def add_object(
        self, description: SparkObjectDescription, trajectory_id: str | None = None
    ):
        try:
            self.connection.execute(
                "INSERT INTO objects VALUES(?,?,?,?)",
                (
                    description.path,
                    description.sha256,
                    description.bytes,
                    trajectory_id,
                ),
            )
        except sqlite3.IntegrityError as error:
            raise RuntimeError(
                "duplicate trajectory identity or filename; no manifest published"
            ) from error
        self.object_count += 1

    def write_manifest(
        self,
        destination: Path,
        *,
        created_at: str | None = None,
        completed_at_ms: int | None = None,
    ):
        """Stream the v4 files array; only one descriptor is decoded at a time."""
        self.connection.commit()
        total_bytes = self.connection.execute(
            "SELECT COALESCE(SUM(bytes),0) FROM objects "
            "WHERE path != 'lineage.jsonl' AND instr(path,'/')=0 "
            "AND path LIKE '%.jsonl'"
        ).fetchone()[0]
        fields = {
            **feedback_fields(
                self.stats,
                total_bytes=total_bytes,
                started_at_ms=self.started_at_ms,
                completed_at_ms=completed_at_ms,
                parse_failure_count=self.stats.skip_reason_counts.get(
                    "parse_failure", 0
                ),
            ),
            "schema_version": SCHEMA_VERSION,
            "sessionless_partitioning": "normalized-bytes-v1",
            "created_at": created_at or datetime.now(UTC).isoformat(),
            "input_root": self.input_root,
            "input_format": self.input_format,
            "config_hash": self.config_hash,
            "token_estimator": "unicode-word-v1",
            "counts": self.stats.__dict__,
        }
        with destination.open("wb") as handle:
            header = dumps(fields, sort_keys=True)
            handle.write(header[:-1] + b',"files":[')
            separator = b""
            for path, digest, size in self.connection.execute(
                "SELECT path,sha256,bytes FROM objects ORDER BY path"
            ):
                handle.write(separator)
                handle.write(
                    dumps(
                        {"path": path, "sha256": digest, "bytes": size},
                        sort_keys=True,
                    )
                )
                separator = b","
            handle.write(b"]}\n")
