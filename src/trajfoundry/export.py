"""Deterministic flat JSONL export, lineage, and run manifest creation."""

from __future__ import annotations

import os
import re
import shutil
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Literal, Self

import orjson

from .canonical import trajectory_id
from .feedback import feedback_fields, unix_ms
from .identity import normalize_session_id
from .io import file_sha256
from .json_codec import dumps, loads
from .models import QuarantineRecord, TrajectoryNode
from .output_contract import project_trajectory
from .quality import is_strict_sample, validate_derived_fields

SCHEMA_VERSION = "trajfoundry-v4"

_SAFE_SESSION_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_SYNTHESIZED_SESSION_CODE = "metadata_session_id_synthesized"
_SESSION_FILENAME_MARKER = "badsession_"
_SESSIONLESS_FILENAME_MARKER = "no_session_id_"
_SAFE_SESSION_FILENAME_MARKER = "_safe_session_"
_LONG_SESSION_FILENAME_MARKER = "_longsession_"
_SESSION_FILENAME_SUFFIX = re.compile(r"_sub_([0-9]+)\.jsonl$")
_HEX_DIGITS = frozenset("0123456789abcdefABCDEF")
_HEX_BYTE_VALUES = frozenset(b"0123456789abcdefABCDEF")
_SAFE_SESSION_FILENAME_BYTES = frozenset(
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-"
)


@dataclass
class ExportStats:
    accepted: int = 0
    quarantined_trajectories: int = 0
    quarantined_records: int = 0
    excluded_records: int = 0
    duplicate_trajectories: int = 0
    input_files: int = 0
    reason_counts: dict[str, int] = field(default_factory=dict)
    skipped_inputs: int = 0
    skip_reason_counts: dict[str, int] = field(default_factory=dict)

    def add_reasons(self, reasons: list[str]) -> None:
        for reason in reasons:
            self.reason_counts[reason] = self.reason_counts.get(reason, 0) + 1

    def add_skipped(self, reason: str) -> None:
        if not reason:
            raise ValueError("skip reason must not be empty")
        self.skipped_inputs += 1
        self.skip_reason_counts[reason] = self.skip_reason_counts.get(reason, 0) + 1


def _session_was_synthesized(node: TrajectoryNode) -> bool:
    if not node.metadata.session_id or node.metadata.sessionless_chunk_id:
        return True
    audit = node.normalization_audit
    return bool(
        audit and any(issue.code == _SYNTHESIZED_SESSION_CODE for issue in audit.issues)
    )


def _encode_session_filename_component(session_id: str) -> str:
    """Encode an arbitrary session ID into one reversible flat-name component.

    Safe ASCII bytes remain readable.  Every other UTF-8 byte is represented
    as ``_xHH_``.  An underscore that would otherwise start such a token is
    itself escaped, which prevents a literal value such as ``"_x7B_"`` from
    being confused with an encoded ``"{"``.  The result is ASCII-only and
    therefore cannot introduce path separators, control characters, or a
    platform-specific Unicode normalization issue.
    """

    encoded: list[str] = []
    raw = session_id.encode("utf-8")
    for index, byte in enumerate(raw):
        if byte not in _SAFE_SESSION_FILENAME_BYTES:
            encoded.append(f"_x{byte:02X}_")
            continue
        if byte == ord("_"):
            # Keep ordinary underscores readable, but escape one that would
            # form the beginning of a decoder token with the following bytes.
            lookahead = raw[index + 1 : index + 5]
            if (
                len(lookahead) >= 3
                and lookahead[0] == ord("x")
                and lookahead[1] in _HEX_BYTE_VALUES
                and lookahead[2] in _HEX_BYTE_VALUES
                and (len(lookahead) == 3 or lookahead[3] == ord("_"))
            ):
                encoded.append("_x5F_")
                continue
        encoded.append(chr(byte))
    return "".join(encoded)


def _decode_session_filename_component(encoded: str) -> str:
    """Inverse of :func:`_encode_session_filename_component`."""

    raw = bytearray()
    index = 0
    while index < len(encoded):
        if (
            encoded[index] == "_"
            and index + 4 < len(encoded)
            and encoded[index + 1] == "x"
            and encoded[index + 2] in _HEX_DIGITS
            and encoded[index + 3] in _HEX_DIGITS
            and encoded[index + 4] == "_"
        ):
            raw.append(int(encoded[index + 2 : index + 4], 16))
            index += 5
            continue
        character = encoded[index]
        if ord(character) > 0x7F:
            raise ValueError("session filename encoding must be ASCII")
        raw.append(ord(character))
        index += 1
    try:
        return bytes(raw).decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError("session filename contains invalid UTF-8 escapes") from error


def _escaped_session_filename(
    session_id: str,
    sub_session_id: int,
    *,
    marker: str = _SESSION_FILENAME_MARKER,
) -> str:
    encoded = _encode_session_filename_component(session_id)
    # Keep a dedicated separator before ``sub``.  Although an escape token
    # also ends in ``_``, retaining this separator makes the filename grammar
    # unambiguous for literal values that happen to end in ``_xHH``.
    return f"{marker}{encoded}_sub_{sub_session_id}.jsonl"


def session_filename(session_id: str, sub_session_id: int) -> str:
    """Return a deterministic flat filename for a real session identity.

    Existing short, safe IDs retain their exact filenames for backwards
    compatibility.  Unsafe IDs use a ``badsession_`` marker and a reversible
    UTF-8 byte escape (for example, ``bad/session`` becomes
    ``badsession_bad_x2F_session_sub_0.jsonl``).  No digest or lossy
    sanitization is used, so the original ID can be recovered from the name.
    The rare safe ID beginning with the reserved ``badsession_`` marker uses
    a leading-underscore escape marker to keep decoding unambiguous.
    Encoding imposes no application-level length limit; the destination's
    filename or complete object-key limits still apply.
    """

    if type(sub_session_id) is not int or sub_session_id < 0:
        raise ValueError("sub_session_id must be a non-negative integer")
    if not isinstance(session_id, str) or not session_id:
        raise ValueError("session_id must be a non-empty string")
    if _SAFE_SESSION_ID.fullmatch(session_id):
        filename = f"{session_id}_sub_{sub_session_id}.jsonl"
        if not session_id.startswith(
            (_SESSION_FILENAME_MARKER, _SESSIONLESS_FILENAME_MARKER)
        ):
            return filename
        return _escaped_session_filename(
            session_id, sub_session_id, marker=_SAFE_SESSION_FILENAME_MARKER
        )
    return _escaped_session_filename(session_id, sub_session_id)


def decode_session_filename(filename: str) -> tuple[str, int]:
    """Recover a real ``(session_id, sub_session_id)`` from a trajectory name.

    Safe historical names are returned directly.  ``badsession_``,
    ``_safe_session_``, and ``_longsession_`` names are decoded from their
    UTF-8 byte escapes.  A ``no_session_id_`` name has no real session
    identity and is rejected.
    """

    if (
        not isinstance(filename, str)
        or not filename
        or "/" in filename
        or "\\" in filename
        or any(ord(char) < 32 or ord(char) == 127 for char in filename)
    ):
        raise ValueError("trajectory filename must be a safe flat filename")
    match = _SESSION_FILENAME_SUFFIX.search(filename)
    if match is None:
        raise ValueError("trajectory filename has no valid sub_session_id suffix")
    sub_session_id = int(match.group(1))
    stem = filename[: match.start()]
    if stem.startswith(_SESSIONLESS_FILENAME_MARKER):
        raise ValueError("sessionless trajectory filename has no real session_id")
    if stem.startswith(_SESSION_FILENAME_MARKER):
        encoded = stem[len(_SESSION_FILENAME_MARKER) :]
        if not encoded:
            raise ValueError("badsession filename has an empty session_id")
        session_id = _decode_session_filename_component(encoded)
        if _SAFE_SESSION_ID.fullmatch(session_id):
            raise ValueError("badsession filename contains a safe session_id")
    elif stem.startswith(_SAFE_SESSION_FILENAME_MARKER):
        encoded = stem[len(_SAFE_SESSION_FILENAME_MARKER) :]
        if not encoded:
            raise ValueError("safe-session filename has an empty session_id")
        session_id = _decode_session_filename_component(encoded)
        if _SAFE_SESSION_ID.fullmatch(session_id) is None:
            raise ValueError("safe-session filename contains an unsafe session_id")
    elif stem.startswith(_LONG_SESSION_FILENAME_MARKER):
        encoded = stem[len(_LONG_SESSION_FILENAME_MARKER) :]
        if not encoded:
            raise ValueError("longsession filename has an empty session_id")
        session_id = _decode_session_filename_component(encoded)
        if not _SAFE_SESSION_ID.fullmatch(session_id):
            raise ValueError("longsession filename contains an unsafe session_id")
    else:
        session_id = stem
        if _SAFE_SESSION_ID.fullmatch(session_id) is None:
            raise ValueError("trajectory filename contains an invalid session_id")
    if not session_id:
        raise ValueError("trajectory filename has an empty session_id")
    return session_id, sub_session_id


def trajectory_filename(node: TrajectoryNode, identifier: str | None = None) -> str:
    """Return the stable root filename for one materialized trajectory."""

    sub_session_id = node.metadata.sub_session_id
    if type(sub_session_id) is not int or sub_session_id < 0:
        raise ValueError("sub_session_id must be a non-negative integer")
    if _session_was_synthesized(node):
        if sub_session_id != 0:
            raise ValueError("sessionless trajectories must use sub_session_id 0")
        digest = identifier or trajectory_id(node)
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError("trajectory identifier must be a sha256 digest")
        return f"no_session_id_{digest}_sub_0.jsonl"

    return session_filename(node.metadata.session_id, sub_session_id)


def _manifest_paths(root: Path) -> set[str]:
    try:
        manifest = loads((root / "manifest.json").read_bytes())
    except (OSError, orjson.JSONDecodeError):
        return set()
    files = manifest.get("files") if isinstance(manifest, dict) else None
    if not isinstance(files, list):
        return set()
    paths: set[str] = set()
    for entry in files:
        relative = entry.get("path") if isinstance(entry, dict) else None
        if not isinstance(relative, str):
            continue
        pure = PurePosixPath(relative)
        if (
            not pure.is_absolute()
            and pure.as_posix() == relative
            and all(part not in {"", ".", ".."} for part in pure.parts)
            and relative.endswith(".jsonl")
        ):
            paths.add(relative)
    return paths


def _published_flat_manifest_paths(root: Path) -> set[str]:
    """Return top-level JSONL paths from the currently published manifest."""

    return {
        relative
        for relative in _manifest_paths(root)
        if len(PurePosixPath(relative).parts) == 1
    }


def _top_level_jsonl_paths(root: Path) -> set[str]:
    """List every top-level JSONL object managed by this output root."""

    try:
        entries = root.iterdir()
        paths: set[str] = set()
        for path in entries:
            if not path.name.endswith(".jsonl"):
                continue
            if path.is_dir() and not path.is_symlink():
                raise OSError(f"managed JSONL path is a directory: {path.name}")
            paths.add(path.name)
        return paths
    except OSError as error:
        raise OSError("could not enumerate managed output JSONL files") from error


def _remove_top_level_jsonl_paths(root: Path, paths: set[str], *, phase: str) -> None:
    """Remove stale top-level JSONL files, failing on any deletion error."""

    for relative in sorted(paths):
        path = root / relative
        try:
            _unlink_local_output_path(path)
        except OSError as error:
            raise OSError(
                f"could not remove stale output JSONL during {phase} cleanup: "
                f"{relative}"
            ) from error
    if paths:
        OutputSet._fsync_directory(root)


def _unlink_local_output_path(path: Path) -> None:
    """Unlink one managed output path without following symlinks."""

    if path.is_dir() and not path.is_symlink():
        raise OSError("managed output path is a directory")
    path.unlink(missing_ok=True)


def _remove_empty_parents(path: Path, stop: Path) -> None:
    parent = path.parent
    while parent != stop:
        try:
            parent.rmdir()
        except OSError:
            return
        parent = parent.parent


class OutputSet:
    """Stage a flat output set and publish its manifest last.

    ``max_shard_bytes`` is retained as the public option name for backwards
    compatibility.  In the flat v4 layout it is the maximum serialized byte
    length (including the trailing newline) of one trajectory JSONL object.
    """

    def __init__(self, root: Path, *, max_shard_bytes: int = 512 * 1024 * 1024):
        if max_shard_bytes <= 0:
            raise ValueError("max shard bytes must be positive")
        self.max_shard_bytes = max_shard_bytes
        self._started_at_ms = unix_ms()
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        # Remove leftovers from an interrupted/failed publication before any
        # new flat object can replace a published file.  The manifest remains
        # authoritative for the files that must be retained.
        published_paths = _published_flat_manifest_paths(self.root)
        managed_paths = _top_level_jsonl_paths(self.root)
        _remove_top_level_jsonl_paths(
            self.root,
            managed_paths - published_paths,
            phase="preflight",
        )
        self._run_id = uuid.uuid4().hex
        self._stage = root / f".staging-{self._run_id}"
        if self._stage.exists():
            raise FileExistsError(f"output staging path already exists: {self._stage}")
        self._stage.mkdir()
        self._manifest_tmp = root / f".manifest-{self._run_id}.tmp"
        self._published = False
        self._trajectory_paths: list[Path] = []
        self._trajectory_names: set[str] = set()
        self._lineage_tmp = self._stage / "lineage.jsonl.tmp"
        self._lineage = self._lineage_tmp.open("wb")
        self.stats = ExportStats()
        self._has_chunked_sessionless = False
        self._has_unchunked_sessionless = False

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def write_trajectory(
        self, node: TrajectoryNode, origins: list[dict[str, Any]]
    ) -> str:
        projected = project_trajectory(node)
        validate_derived_fields(node)
        pending_nodes = [node]
        chunked = self._has_chunked_sessionless
        unchunked = self._has_unchunked_sessionless
        while pending_nodes:
            current = pending_nodes.pop()
            if not normalize_session_id(current.metadata.session_id):
                chunked |= bool(current.metadata.sessionless_chunk_id)
                unchunked |= not current.metadata.sessionless_chunk_id
            pending_nodes.extend((current.sub_agent_trajectory or {}).values())
        if chunked and unchunked:
            raise ValueError("cannot mix chunked and unchunked sessionless identities")
        self._has_chunked_sessionless = chunked
        self._has_unchunked_sessionless = unchunked
        identifier = trajectory_id(node)
        filename = trajectory_filename(node, identifier)
        if filename in self._trajectory_names:
            raise ValueError(f"duplicate trajectory output filename: {filename}")

        path = self._stage / filename
        pending = path.with_suffix(path.suffix + ".tmp")
        payload = dumps(projected, sort_keys=True) + b"\n"
        if len(payload) > self.max_shard_bytes:
            raise ValueError(
                "trajectory JSONL object exceeds max_shard_bytes: "
                f"{len(payload)} > {self.max_shard_bytes}"
            )
        with pending.open("wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        pending.replace(path)
        self._trajectory_names.add(filename)
        self._trajectory_paths.append(path)

        strict = is_strict_sample(node)
        if strict:
            self.stats.accepted += 1
        else:
            self.stats.quarantined_trajectories += 1
            if node.normalization_audit:
                self.stats.add_reasons(node.normalization_audit.reason_codes)
        self._lineage.write(
            dumps(
                {
                    "trajectory_id": identifier,
                    "representative": node.metadata.source_file,
                    "origin_count": len(origins),
                    "origins": origins,
                },
                sort_keys=True,
            )
            + b"\n"
        )
        self.stats.duplicate_trajectories += max(0, len(origins) - 1)
        return identifier

    def write_record(self, record: QuarantineRecord) -> None:
        """Account for an input that could not form a trajectory."""

        self.stats.quarantined_records += 1
        if record.normalization_audit.tag.value == "excluded":
            self.stats.excluded_records += 1
        self.stats.add_reasons(record.normalization_audit.reason_codes)

    def write_skipped(self, reason: str) -> None:
        self.stats.add_skipped(reason)

    def close(
        self,
        *,
        input_root: str,
        config_hash: str,
        started_at_ms: int | None = None,
        parse_failure_count: int = 0,
        input_format: Literal[
            "freerouter", "tokenplan", "sxf", "deepinfra"
        ] = "freerouter",
    ) -> None:
        if self._published:
            return
        self._lineage.flush()
        os.fsync(self._lineage.fileno())
        self._lineage.close()
        staged_lineage = self._stage / "lineage.jsonl"
        self._lineage_tmp.replace(staged_lineage)
        files = sorted(
            [*self._trajectory_paths, staged_lineage],
            key=lambda path: path.name,
        )
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "created_at": datetime.now(UTC).isoformat(),
            "input_root": input_root,
            "input_format": input_format,
            "config_hash": config_hash,
            "token_estimator": "unicode-word-v1",
            "counts": {
                **self.stats.__dict__,
                "reason_counts": dict(sorted(self.stats.reason_counts.items())),
                "skip_reason_counts": dict(
                    sorted(self.stats.skip_reason_counts.items())
                ),
            },
            "files": [
                {
                    "path": path.name,
                    "sha256": file_sha256(path),
                    "bytes": path.stat().st_size,
                }
                for path in files
            ],
        }
        if self._has_chunked_sessionless:
            manifest["sessionless_partitioning"] = "normalized-bytes-v1"
        manifest.update(
            feedback_fields(
                self.stats,
                total_bytes=sum(
                    item["bytes"]
                    for item in manifest["files"]
                    if item["path"] != "lineage.jsonl"
                ),
                started_at_ms=self._started_at_ms
                if started_at_ms is None
                else started_at_ms,
                parse_failure_count=parse_failure_count,
            )
        )
        manifest_bytes = dumps(manifest, sort_keys=True, indent=2)
        with self._manifest_tmp.open("wb") as handle:
            handle.write(manifest_bytes)
            handle.flush()
            os.fsync(handle.fileno())

        self._fsync_directory(self._stage)
        previous_paths = _manifest_paths(self.root)
        current_paths = {path.name for path in files}
        for path in files:
            path.replace(self.root / path.name)
        self._fsync_directory(self.root)
        self._manifest_tmp.replace(self.root / "manifest.json")
        self._published = True
        self._fsync_directory(self.root)

        # A flat layout cannot atomically swap every data file with the manifest.
        # Publish the new manifest last, then remove only files declared by the
        # previous manifest.  A failed cleanup is surfaced so a retry can use
        # the newly published manifest during its preflight pass.
        for relative in sorted(previous_paths - current_paths):
            old_path = self.root.joinpath(*PurePosixPath(relative).parts)
            try:
                _unlink_local_output_path(old_path)
                _remove_empty_parents(old_path, self.root)
            except OSError as error:
                raise OSError(
                    "could not remove stale output JSONL during "
                    f"post-publication cleanup: {relative}"
                ) from error
        if previous_paths - current_paths:
            self._fsync_directory(self.root)
        shutil.rmtree(self._stage, ignore_errors=True)

    def abort(self) -> None:
        if not self._lineage.closed:
            self._lineage.close()
        shutil.rmtree(self._stage, ignore_errors=True)
        self._manifest_tmp.unlink(missing_ok=True)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, *_: object) -> None:
        if exc_type is not None:
            self.abort()


__all__ = [
    "SCHEMA_VERSION",
    "ExportStats",
    "OutputSet",
    "decode_session_filename",
    "session_filename",
    "trajectory_filename",
]
