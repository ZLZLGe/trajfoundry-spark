"""Flat v4 output helpers for the Spark execution path.

Spark tasks may be retried, and more than one executor can finish at roughly
the same time.  This module therefore keeps all output identities
deterministic and makes individual S3 puts safe to repeat.  It deliberately
does not import PySpark: the same code can run in a worker task, a local
contract test, or the small driver-side manifest finalizer.

The public output contract is the same as :class:`trajfoundry.export.OutputSet`:
one top-level trajectory per ``<session>_sub_<n>.jsonl`` object, one
``lineage.jsonl`` object, and a ``trajfoundry-v4`` manifest published last.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Mapping, Sequence

from ..canonical import trajectory_id
from ..export import SCHEMA_VERSION, ExportStats, trajectory_filename
from ..json_codec import dumps
from ..models import QuarantineRecord, TrajectoryNode
from ..output_contract import project_trajectory
from ..quality import is_strict_sample, validate_derived_fields
from ..s3 import S3Location

InputFormat = Literal["freerouter", "tokenplan", "sxf", "deepinfra"]


@dataclass(frozen=True, slots=True)
class SparkObjectDescription:
    """Description of one object written by an executor.

    ``path`` is relative to the output :class:`~trajfoundry.s3.S3Location` and
    is the only path included in a v4 manifest.  Keeping this object immutable
    makes collecting small metadata records on the driver safe and explicit.
    """

    path: str
    sha256: str
    bytes: int

    def __post_init__(self) -> None:
        if not self.path or self.path.startswith("/") or "/" in self.path:
            raise ValueError("Spark output paths must be flat relative names")
        if len(self.sha256) != 64 or any(
            char not in "0123456789abcdef" for char in self.sha256
        ):
            raise ValueError("sha256 must be a lowercase hexadecimal digest")
        if self.bytes < 0:
            raise ValueError("object bytes must be non-negative")

    def manifest_entry(self) -> dict[str, str | int]:
        return {"path": self.path, "sha256": self.sha256, "bytes": self.bytes}


@dataclass(frozen=True, slots=True)
class TrajectoryArtifact:
    """A projected deterministic trajectory payload and its lineage."""

    trajectory_id: str
    filename: str
    payload: bytes
    origins: tuple[dict[str, Any], ...] = ()

    def __post_init__(self) -> None:
        if len(self.trajectory_id) != 64 or any(
            char not in "0123456789abcdef" for char in self.trajectory_id
        ):
            raise ValueError("trajectory_id must be a lowercase sha256 digest")
        if not self.filename.endswith(".jsonl") or "/" in self.filename:
            raise ValueError("trajectory filename must be a flat JSONL path")
        if not self.payload.endswith(b"\n"):
            raise ValueError("trajectory payload must end with one newline")

    @property
    def description(self) -> SparkObjectDescription:
        return SparkObjectDescription(
            path=self.filename,
            sha256=hashlib.sha256(self.payload).hexdigest(),
            bytes=len(self.payload),
        )

    @property
    def lineage(self) -> dict[str, Any]:
        return lineage_record(
            trajectory_id=self.trajectory_id,
            payload=self.payload,
            origins=self.origins,
        )


def serialize_trajectory(
    node: TrajectoryNode,
    origins: Sequence[Mapping[str, Any]] = (),
    *,
    max_bytes: int | None = None,
) -> TrajectoryArtifact:
    """Project and serialize one complete trajectory deterministically.

    Validation is intentionally identical to the existing local and direct S3
    exporters.  In particular, mutable Pydantic models are revalidated by
    ``project_trajectory`` before serialization and stale derived quality
    fields are rejected before a task can publish an object.
    """

    projected = project_trajectory(node)
    validate_derived_fields(node)
    identifier = trajectory_id(node)
    filename = trajectory_filename(node, identifier)
    payload = dumps(projected, sort_keys=True) + b"\n"
    if max_bytes is not None and len(payload) > max_bytes:
        raise ValueError(
            "trajectory JSONL object exceeds max_bytes: "
            f"{len(payload)} > {max_bytes}"
        )
    copied_origins = tuple(dict(origin) for origin in origins)
    return TrajectoryArtifact(identifier, filename, payload, copied_origins)


def lineage_record(
    *,
    trajectory_id: str,
    payload: bytes | None = None,
    origins: Sequence[Mapping[str, Any]] = (),
    representative: str | None = None,
) -> dict[str, Any]:
    """Return the canonical lineage JSON object for one trajectory.

    ``payload`` is optional and exists for callers that only have an artifact;
    it is not decoded.  The representative defaults to the first origin's
    ``source_ref`` just as the existing exporters do for the root node.
    """

    if len(trajectory_id) != 64 or any(
        char not in "0123456789abcdef" for char in trajectory_id
    ):
        raise ValueError("trajectory_id must be a lowercase sha256 digest")
    copied = [dict(origin) for origin in origins]
    if representative is None:
        candidate = copied[0].get("source_ref") if copied else ""
        representative = candidate if isinstance(candidate, str) else ""
    return {
        "trajectory_id": trajectory_id,
        "representative": representative,
        "origin_count": len(copied),
        "origins": copied,
    }


def lineage_line(artifact: TrajectoryArtifact) -> bytes:
    """Encode one artifact's lineage record as a canonical JSONL line."""

    return dumps(artifact.lineage, sort_keys=True) + b"\n"


def deterministic_s3_put(
    client: Any,
    location: S3Location,
    relative_path: str,
    payload: bytes,
    *,
    content_type: str = "application/jsonl",
) -> SparkObjectDescription:
    """Put one deterministic object, safely allowing Spark task retries.

    We record the SHA-256 digest as user metadata.  A retry whose object already
    has the same digest performs no network write.  Existing objects without
    this metadata are replaced once with the deterministic payload; this keeps
    upgrades from silently accepting stale bytes while remaining compatible
    with objects produced by older TrajFoundry versions.
    """

    if not payload:
        # Empty JSONL is valid for lineage, but a trajectory itself is never
        # empty.  The helper is generic, so do not reject it here.
        pass
    key = location.key(relative_path)
    digest = hashlib.sha256(payload).hexdigest()
    try:
        head = client.head_object(Bucket=location.bucket, Key=key)
    except Exception as error:  # boto3 uses ClientError for a missing object
        response = getattr(error, "response", None)
        status = (
            response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if isinstance(response, dict)
            else None
        )
        code = (
            response.get("Error", {}).get("Code")
            if isinstance(response, dict)
            else None
        )
        missing = status == 404 or code in {"404", "NoSuchKey", "NotFound"}
        if not missing:
            raise
        head = None
    if isinstance(head, dict):
        metadata = head.get("Metadata") or {}
        existing_digest = metadata.get("sha256") or metadata.get("x-amz-meta-sha256")
        existing_size = head.get("ContentLength")
        if existing_digest == digest and (
            existing_size is None or int(existing_size) == len(payload)
        ):
            return SparkObjectDescription(relative_path, digest, len(payload))
    client.put_object(
        Bucket=location.bucket,
        Key=key,
        Body=payload,
        ContentType=content_type,
        Metadata={"sha256": digest},
    )
    return SparkObjectDescription(relative_path, digest, len(payload))


def merge_lineage_parts(
    parts: Sequence[str | Path], destination: str | Path, *, sort_parts: bool = True
) -> SparkObjectDescription:
    """Stream executor lineage parts into one local ``lineage.jsonl`` file.

    Parts are ordered by their path for deterministic output.  Each part is
    copied line-by-line, so complete lineage contents are never collected on
    the driver.  Spark partition IDs should be encoded in part filenames; a
    caller needing global trajectory ordering can sort its partitions before
    writing them.
    """

    source_paths = [Path(part) for part in parts]
    if sort_parts:
        source_paths.sort(key=lambda path: path.name)
    target = Path(destination)
    target.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    size = 0
    with target.open("wb") as handle:
        for source in source_paths:
            with source.open("rb") as part:
                while True:
                    chunk = part.read(8 * 1024 * 1024)
                    if not chunk:
                        break
                    handle.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
    return SparkObjectDescription(target.name, digest.hexdigest(), size)


@dataclass
class ManifestAccumulator:
    """Driver-side accumulator for small object metadata and quality counters."""

    input_root: str
    input_format: InputFormat
    config_hash: str
    stats: ExportStats = field(default_factory=ExportStats)
    _objects: dict[str, SparkObjectDescription] = field(
        default_factory=dict, init=False
    )
    _lineage_records: int = field(default=0, init=False)

    def add_object(self, description: SparkObjectDescription) -> None:
        existing = self._objects.get(description.path)
        if existing is not None and existing != description:
            raise ValueError(f"conflicting output object metadata: {description.path}")
        self._objects[description.path] = description

    def add_trajectory(
        self,
        node: TrajectoryNode,
        origins: Sequence[Mapping[str, Any]],
        *,
        max_bytes: int | None = None,
    ) -> TrajectoryArtifact:
        artifact = serialize_trajectory(node, origins, max_bytes=max_bytes)
        self.add_artifact(artifact)
        self.account_trajectory(node, len(origins))
        return artifact

    def add_artifact(self, artifact: TrajectoryArtifact) -> None:
        self.add_object(artifact.description)
        self._lineage_records += 1

    def account_trajectory(self, node: TrajectoryNode, origins_count: int = 0) -> None:
        """Account quality counters when an executor emits only descriptors."""

        if is_strict_sample(node):
            self.stats.accepted += 1
        else:
            self.stats.quarantined_trajectories += 1
            if node.normalization_audit:
                self.stats.add_reasons(node.normalization_audit.reason_codes)
        self.stats.duplicate_trajectories += max(0, origins_count - 1)

    def add_record(self, record: QuarantineRecord) -> None:
        self.stats.quarantined_records += 1
        if record.normalization_audit.tag.value == "excluded":
            self.stats.excluded_records += 1
        self.stats.add_reasons(record.normalization_audit.reason_codes)

    def add_skipped(self, reason: str) -> None:
        self.stats.add_skipped(reason)

    @property
    def objects(self) -> tuple[SparkObjectDescription, ...]:
        return tuple(self._objects[path] for path in sorted(self._objects))

    @property
    def lineage_records(self) -> int:
        return self._lineage_records

    def manifest_bytes(self, *, created_at: str | None = None) -> bytes:
        """Return a v4 manifest; callers publish it only after validation."""

        created = created_at or datetime.now(UTC).isoformat()
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "created_at": created,
            "input_root": self.input_root,
            "input_format": self.input_format,
            "config_hash": self.config_hash,
            "token_estimator": "unicode-word-v1",
            "counts": {
                **self.stats.__dict__,
                "reason_counts": dict(sorted(self.stats.reason_counts.items())),
                "skip_reason_counts": dict(
                    sorted(self.stats.skip_reason_counts.items())
                ),
            },
            "files": [description.manifest_entry() for description in self.objects],
        }
        return dumps(manifest, sort_keys=True, indent=2)


__all__ = [
    "InputFormat",
    "ManifestAccumulator",
    "SparkObjectDescription",
    "TrajectoryArtifact",
    "deterministic_s3_put",
    "lineage_line",
    "lineage_record",
    "merge_lineage_parts",
    "serialize_trajectory",
]
