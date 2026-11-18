"""Spark-only output stores for S3 or a shared POSIX filesystem.

Local output is shared storage, not an executor's private scratch directory.
The driver claims an empty directory and workers require its run marker before
writing. Files are streamed into temporary files in the destination directory
and linked into place without replacement; retries must contain identical bytes.
"""

from __future__ import annotations

import hashlib
import io
import os
import re
import secrets
import stat
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, BinaryIO
from urllib.parse import unquote, urlsplit

from ..credentials import load_s3_credentials
from ..s3 import create_s3_client, parse_s3_uri
from .output import SparkObjectDescription
from .paths import (
    DIAGNOSTICS_DIRECTORY,
    RUN_MARKER,
    WRITE_TEMP_PREFIX,
    is_retained_s3_auxiliary_name,
    validate_flat_output_name,
    validate_output_name,
)

_CHUNK_BYTES = 8 * 1024 * 1024
_RUN_MARKER = RUN_MARKER
_TEMP_PREFIX = WRITE_TEMP_PREFIX
_EXISTENCE_LIST_MAX_KEYS = 2
_S3_SINGLE_PUT_MAX_BYTES = 5 * 1024 * 1024 * 1024
_MULTIPART_PART_BYTES = 64 * 1024 * 1024
_MULTIPART_MAX_PARTS = 10_000
_MULTIPART_MAX_PART_BYTES = 5 * 1024 * 1024 * 1024


class _MissingDiagnosticsDirectory(FileNotFoundError):
    """An absent optional child, distinct from a missing output root."""


def local_output_path(output_uri: str) -> Path | None:
    """Validate an output destination; return ``None`` only for valid S3 URIs."""
    if not isinstance(output_uri, str):
        raise TypeError("output URI must be a string")
    if not output_uri or any(ord(char) < 32 or ord(char) == 127 for char in output_uri):
        raise ValueError("output URI must not be empty or contain control characters")
    if output_uri.startswith("s3://"):
        parse_s3_uri(output_uri)
        return None
    if "?" in output_uri or "#" in output_uri:
        raise ValueError("output URI must not contain a query or fragment")
    parsed = urlsplit(output_uri)
    if parsed.scheme == "file":
        if not output_uri.startswith("file:///") or parsed.netloc:
            raise ValueError(
                "file output must use file:///absolute/path without an authority"
            )
        path_text = unquote(parsed.path, errors="strict")
    elif not parsed.scheme and not parsed.netloc and output_uri.startswith("/"):
        path_text = output_uri
    else:
        raise ValueError("output must be an S3 URI or an absolute local path")
    if (
        not path_text.startswith("/")
        or path_text.startswith("//")
        or "\\" in path_text
        or any(ord(char) < 32 or ord(char) == 127 for char in path_text)
        or any(part in {".", ".."} for part in path_text.split("/"))
    ):
        raise ValueError("local output contains unsafe path components")
    path = Path(path_text)
    if path == Path("/"):
        raise ValueError("the filesystem root is not an output directory")
    return path


def _flat_name(name: str) -> str:
    return validate_flat_output_name(name)


def _validate_run_id(run_id: str) -> None:
    if not isinstance(run_id, str) or not re.fullmatch(
        r"[A-Za-z0-9_.-]{1,128}", run_id
    ):
        raise ValueError(
            "run_id must contain 1 to 128 letters, digits, dots, underscores or hyphens"
        )


@contextmanager
def _directory_fd(path: Path, *, create: bool = False) -> Iterator[int]:
    """Open each path component without following symlinks, including parents."""
    if not path.is_absolute():
        raise ValueError("filesystem destinations must be absolute")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    descriptor = os.open("/", flags)
    try:
        for component in path.parts[1:]:
            if component in {".", ".."}:
                raise ValueError("unsafe directory component")
            try:
                child = os.open(component, flags, dir_fd=descriptor)
            except FileNotFoundError:
                if not create:
                    raise
                try:
                    os.mkdir(component, mode=0o755, dir_fd=descriptor)
                except FileExistsError:
                    pass
                child = os.open(component, flags, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        yield descriptor
    finally:
        os.close(descriptor)


@contextmanager
def _regular_file(directory: int, name: str) -> Iterator[BinaryIO]:
    descriptor = os.open(
        name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory
    )
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError("output object is not a regular file")
        handle = os.fdopen(descriptor, "rb")
    except BaseException:
        os.close(descriptor)
        raise
    with handle:
        yield handle


def _describe(name: str, handle: BinaryIO) -> SparkObjectDescription:
    digest = hashlib.sha256()
    size = 0
    while chunk := handle.read(_CHUNK_BYTES):
        digest.update(chunk)
        size += len(chunk)
    return SparkObjectDescription(name, digest.hexdigest(), size)


def _atomic_write(
    directory: int, name: str, source: BinaryIO
) -> SparkObjectDescription:
    temporary = _TEMP_PREFIX + secrets.token_hex(16)
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
        0o644,
        dir_fd=directory,
    )
    try:
        digest = hashlib.sha256()
        size = 0
        with os.fdopen(descriptor, "wb") as handle:
            while chunk := source.read(_CHUNK_BYTES):
                handle.write(chunk)
                digest.update(chunk)
                size += len(chunk)
            handle.flush()
            os.fsync(handle.fileno())
        description = SparkObjectDescription(name, digest.hexdigest(), size)
        try:
            os.link(
                temporary,
                name,
                src_dir_fd=directory,
                dst_dir_fd=directory,
                follow_symlinks=False,
            )
        except FileExistsError:
            with _regular_file(directory, name) as existing:
                if _describe(name, existing) != description:
                    raise FileExistsError(
                        "conflicting output bytes; existing file was preserved"
                    ) from None
        os.fsync(directory)
        return description
    finally:
        os.unlink(temporary, dir_fd=directory)


class OutputStore:
    """Common worker/driver interface; implementations never collect whole files."""

    local_path: Path | None = None

    def ensure_empty(self) -> None:
        raise NotImplementedError

    def initialize_run(self, run_id: str) -> None:
        raise NotImplementedError

    def put_bytes(
        self, name: str, payload: bytes, *, content_type: str = "application/jsonl"
    ) -> SparkObjectDescription:
        raise NotImplementedError

    def put_file(
        self, name: str, path: str | Path, *, content_type: str = "application/jsonl"
    ) -> SparkObjectDescription:
        raise NotImplementedError

    def copy_to(self, name: str, destination: str | Path) -> SparkObjectDescription:
        raise NotImplementedError

    def delete(self, name: str) -> None:
        raise NotImplementedError

    def verify(self, description: SparkObjectDescription) -> None:
        raise NotImplementedError

    def finish_run(self) -> None:
        raise NotImplementedError


class LocalOutputStore(OutputStore):
    def __init__(self, path: Path, run_id: str | None = None) -> None:
        self.local_path = path
        self.run_id = run_id
        self._owns_marker = False
        if run_id is not None:
            _validate_run_id(run_id)
            with _directory_fd(path) as directory:
                self._check_marker(directory)

    def _check_marker(self, directory: int) -> None:
        if self.run_id is None:
            raise RuntimeError(
                "driver must initialize the local output run before writing"
            )
        try:
            with _regular_file(directory, _RUN_MARKER) as marker:
                value = marker.read(256)
        except FileNotFoundError as error:
            raise RuntimeError(
                "shared output run marker is missing; verify Driver/Executor mounts"
            ) from error
        if value != (self.run_id + "\n").encode("ascii"):
            raise RuntimeError("shared output run marker belongs to another run")

    @contextmanager
    def _object_directory(
        self, name: str, *, create: bool = False
    ) -> Iterator[tuple[int, str]]:
        validate_output_name(name)
        with _directory_fd(self.local_path) as directory:
            self._check_marker(directory)
            if "/" not in name:
                yield directory, name
                return
            # Validation permits only diagnostics/<known basename>. Open the
            # child independently: O_NOFOLLOW on a compound name would protect
            # only the file, allowing a diagnostics symlink to escape the root.
            flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
            try:
                child = os.open(DIAGNOSTICS_DIRECTORY, flags, dir_fd=directory)
            except FileNotFoundError as error:
                if not create:
                    raise _MissingDiagnosticsDirectory(
                        "diagnostics directory does not exist"
                    ) from error
                try:
                    os.mkdir(DIAGNOSTICS_DIRECTORY, mode=0o755, dir_fd=directory)
                    os.fsync(directory)
                except FileExistsError:
                    pass
                child = os.open(DIAGNOSTICS_DIRECTORY, flags, dir_fd=directory)
            try:
                yield child, name.split("/", 1)[1]
            finally:
                os.close(child)

    def ensure_empty(self) -> None:
        with _directory_fd(self.local_path, create=True) as directory:
            if os.listdir(directory):
                raise ValueError(
                    "output directory is not empty; use a fresh exclusive directory"
                )

    def initialize_run(self, run_id: str) -> None:
        _validate_run_id(run_id)
        self.ensure_empty()
        with _directory_fd(self.local_path) as directory:
            descriptor = os.open(
                _RUN_MARKER,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o644,
                dir_fd=directory,
            )
            try:
                with os.fdopen(descriptor, "wb") as marker:
                    marker.write((run_id + "\n").encode("ascii"))
                    marker.flush()
                    os.fsync(marker.fileno())
                if os.listdir(directory) != [_RUN_MARKER]:
                    raise ValueError("output directory changed while claiming the run")
                os.fsync(directory)
            except BaseException:
                os.unlink(_RUN_MARKER, dir_fd=directory)
                raise
        self.run_id = run_id
        self._owns_marker = True

    def put_bytes(
        self, name: str, payload: bytes, *, content_type: str = "application/jsonl"
    ) -> SparkObjectDescription:
        with self._object_directory(name, create=True) as (directory, basename):
            description = _atomic_write(directory, basename, io.BytesIO(payload))
        return SparkObjectDescription(name, description.sha256, description.bytes)

    def put_file(
        self, name: str, path: str | Path, *, content_type: str = "application/jsonl"
    ) -> SparkObjectDescription:
        with (
            self._object_directory(name, create=True) as (directory, basename),
            Path(path).open("rb") as source,
        ):
            description = _atomic_write(directory, basename, source)
        return SparkObjectDescription(name, description.sha256, description.bytes)

    def copy_to(self, name: str, destination: str | Path) -> SparkObjectDescription:
        validate_output_name(name)
        target = Path(destination)
        _flat_name(target.name)
        with (
            self._object_directory(name) as (directory, basename),
            _regular_file(directory, basename) as source,
            _directory_fd(target.parent, create=True) as target_directory,
        ):
            copied = _atomic_write(target_directory, target.name, source)
        return SparkObjectDescription(name, copied.sha256, copied.bytes)

    def delete(self, name: str) -> None:
        validate_output_name(name)
        try:
            with self._object_directory(name) as (directory, basename):
                try:
                    with _regular_file(directory, basename):
                        pass
                    os.unlink(basename, dir_fd=directory)
                except FileNotFoundError:
                    return
        except _MissingDiagnosticsDirectory:
            return

    def verify(self, description: SparkObjectDescription) -> None:
        with (
            self._object_directory(description.path) as (directory, basename),
            _regular_file(directory, basename) as source,
        ):
            actual = _describe(description.path, source)
        if actual != description:
            raise ValueError("output file hash or size does not match its description")

    def finish_run(self) -> None:
        if not self._owns_marker:
            raise RuntimeError(
                "only the driver that initialized this run may remove its marker"
            )
        with _directory_fd(self.local_path) as directory:
            self._check_marker(directory)
            os.unlink(_RUN_MARKER, dir_fd=directory)
            os.fsync(directory)
        self._owns_marker = False


class S3OutputStore(OutputStore):
    def __init__(
        self,
        output_uri: str,
        client: Any,
        *,
        retain_auxiliary: bool = False,
        allow_existing_output: bool = False,
        output_owner: str | None = None,
        resume_owned_output: bool = False,
    ) -> None:
        if type(retain_auxiliary) is not bool:
            raise TypeError("retain_auxiliary must be a boolean")
        if type(allow_existing_output) is not bool:
            raise TypeError("allow_existing_output must be a boolean")
        self.location = parse_s3_uri(output_uri)
        self.client = client
        self.retain_auxiliary = retain_auxiliary
        self.allow_existing_output = allow_existing_output
        self.output_owner = output_owner
        self.resume_owned_output = resume_owned_output

    def ensure_empty(self) -> None:
        # This is a preflight, not a distributed lock. S3 callers must reserve
        # a fresh prefix exclusively. Explicit retention permits only known
        # run-scoped auxiliary files left by prior attempts, never business data.
        if self.allow_existing_output:
            return
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(
            Bucket=self.location.bucket, Prefix=self.location.prefix
        ):
            if not isinstance(page, dict) or page.get("CommonPrefixes"):
                raise ValueError("invalid S3 output listing; refusing initialization")
            contents = page.get("Contents", [])
            if not isinstance(contents, list):
                raise TypeError("invalid S3 output listing; refusing initialization")
            for entry in contents:
                key = entry.get("Key") if isinstance(entry, dict) else None
                if (
                    not self.retain_auxiliary
                    or not isinstance(key, str)
                    or not key.startswith(self.location.prefix)
                    or not is_retained_s3_auxiliary_name(
                        key[len(self.location.prefix) :]
                    )
                ):
                    raise ValueError(
                        "output prefix is not empty; use a fresh exclusive prefix "
                        "or retain only recognized auxiliary objects"
                    )

    def initialize_run(self, run_id: str) -> None:
        _validate_run_id(run_id)
        if self.resume_owned_output:
            if not self.output_owner or self.allow_existing_output:
                raise ValueError(
                    "owned resume requires an owner and forbids replacement"
                )
            for page in self.client.get_paginator("list_objects_v2").paginate(
                Bucket=self.location.bucket, Prefix=self.location.prefix
            ):
                for item in page.get("Contents", []):
                    if item["Key"] == self.location.key("manifest.json"):
                        raise FileExistsError(
                            "completed output must be validated or skipped, never resumed"
                        )
                    head = self.client.head_object(
                        Bucket=self.location.bucket, Key=item["Key"]
                    )
                    if head.get("Metadata", {}).get("tf-owner") != self.output_owner:
                        raise FileExistsError(
                            "unowned output object; preserve existing files"
                        )
            return
        self.ensure_empty()

    def put_bytes(
        self, name: str, payload: bytes, *, content_type: str = "application/jsonl"
    ) -> SparkObjectDescription:
        description = SparkObjectDescription(
            validate_output_name(name),
            hashlib.sha256(payload).hexdigest(),
            len(payload),
        )
        return self._put(description, payload, content_type)

    def put_file(
        self, name: str, path: str | Path, *, content_type: str = "application/jsonl"
    ) -> SparkObjectDescription:
        validate_output_name(name)
        with Path(path).open("rb") as source:
            description = _describe(name, source)
            source.seek(0)
            return self._put(description, source, content_type)

    def _check_destination(
        self, description: SparkObjectDescription
    ) -> tuple[bool, dict[str, str]]:
        # The caller reserves this prefix exclusively. HEAD is a conflict
        # guard, not an atomic S3 lock: simultaneous independent runs remain
        # forbidden. Within one run, deterministic task retries are allowed.
        key = self.location.key(description.path)
        conditions: dict[str, str] = {}
        try:
            head = self.client.head_object(Bucket=self.location.bucket, Key=key)
        except Exception as error:
            response = getattr(error, "response", None)
            if not isinstance(response, dict):
                raise
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            code = response.get("Error", {}).get("Code")
            if status != 404 and code not in {"404", "NoSuchKey", "NotFound"}:
                # Some S3-compatible gateways return a generic HEAD 403 for a
                # missing key. Never treat that response alone as absence, nor
                # reinterpret explicit signature/token/authentication errors.
                if (
                    status not in {None, 403}
                    or code not in {"403", "Forbidden", "AccessDenied"}
                    or not self._missing_key_confirmed_by_listing(key)
                ):
                    raise
                # Absence was learned from LIST rather than HEAD. Conditional
                # creation also protects against an object appearing meanwhile.
                conditions["IfNoneMatch"] = "*"
        else:
            metadata = head.get("Metadata") or {}
            if self.output_owner and metadata.get("tf-owner") != self.output_owner:
                raise FileExistsError("unowned output object; preserve existing files")
            if self.allow_existing_output:
                # Explicit retry mode allows the new run to replace a prior
                # attempt's object.  A matching object is still idempotent.
                if (
                    metadata.get("sha256") == description.sha256
                    and head.get("ContentLength") == description.bytes
                ):
                    return True, conditions
            elif (
                metadata.get("sha256") == description.sha256
                and head.get("ContentLength") == description.bytes
            ):
                return True, conditions
            else:
                raise FileExistsError(
                    "conflicting S3 output bytes; existing object was preserved"
                )
        return False, conditions

    def _put(
        self,
        description: SparkObjectDescription,
        source: bytes | BinaryIO,
        content_type: str,
    ) -> SparkObjectDescription:
        key = self.location.key(description.path)
        exists, conditions = self._check_destination(description)
        if exists:
            return description
        metadata = {
            "sha256": description.sha256,
            **({"tf-owner": self.output_owner} if self.output_owner else {}),
        }
        if description.bytes > _S3_SINGLE_PUT_MAX_BYTES:
            # A merged diagnostics sidecar can exceed S3's single-PUT limit
            # even when every executor output is small. Keep the existing
            # conflict checks and publish only after all parts are uploaded.
            if not self.allow_existing_output:
                conditions["IfNoneMatch"] = "*"
            self._put_multipart(description, source, content_type, metadata, conditions)
            return description
        self.client.put_object(
            Bucket=self.location.bucket,
            Key=key,
            Body=source,
            ContentType=content_type,
            Metadata=metadata,
            **conditions,
        )
        return description

    def _put_multipart(
        self,
        description: SparkObjectDescription,
        source: bytes | BinaryIO,
        content_type: str,
        metadata: dict[str, str],
        conditions: dict[str, str],
    ) -> None:
        part_bytes = max(
            _MULTIPART_PART_BYTES,
            (description.bytes + _MULTIPART_MAX_PARTS - 1) // _MULTIPART_MAX_PARTS,
        )
        if part_bytes > _MULTIPART_MAX_PART_BYTES:
            raise ValueError("output exceeds S3 multipart size limits")
        stream = io.BytesIO(source) if isinstance(source, bytes) else source
        key = self.location.key(description.path)
        created = self.client.create_multipart_upload(
            Bucket=self.location.bucket,
            Key=key,
            ContentType=content_type,
            Metadata=metadata,
        )
        upload_id = created["UploadId"]
        try:
            parts = []
            digest = hashlib.sha256()
            remaining = description.bytes
            while remaining:
                expected = min(part_bytes, remaining)
                payload = stream.read(expected)
                if not isinstance(payload, bytes) or len(payload) != expected:
                    raise ValueError("multipart source size changed during upload")
                digest.update(payload)
                number = len(parts) + 1
                uploaded = self.client.upload_part(
                    Bucket=self.location.bucket,
                    Key=key,
                    UploadId=upload_id,
                    PartNumber=number,
                    Body=payload,
                    ContentLength=len(payload),
                )
                parts.append({"PartNumber": number, "ETag": uploaded["ETag"]})
                remaining -= len(payload)
                del payload
            if stream.read(1) or digest.hexdigest() != description.sha256:
                raise ValueError("multipart source hash or size changed during upload")
            # Ceph can ignore IfNoneMatch on completion. Repeat the HEAD/LIST
            # conflict guard after the long upload as well. Exclusive ownership
            # of the output prefix remains required; this is not an S3 lock.
            exists, final_conditions = self._check_destination(description)
            if not exists:
                conditions.update(final_conditions)
                self.client.complete_multipart_upload(
                    Bucket=self.location.bucket,
                    Key=key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts},
                    **conditions,
                )
        except BaseException as error:
            try:
                self.client.abort_multipart_upload(
                    Bucket=self.location.bucket, Key=key, UploadId=upload_id
                )
            except Exception:  # noqa: BLE001 - preserve the original upload failure
                error.add_note(
                    "S3 multipart abort failed; uncommitted parts may remain"
                )
            raise
        if exists:
            # Another identical retry completed first; keep its published bytes.
            self.client.abort_multipart_upload(
                Bucket=self.location.bucket, Key=key, UploadId=upload_id
            )

    def _missing_key_confirmed_by_listing(self, key: str) -> bool:
        """Confirm absence using the same credentials and a bounded exact prefix.

        This remains a conflict check, not a distributed lock. Do not paginate
        or assume lexical ordering: a truncated or malformed response cannot
        establish that the exact key is absent, even if its first page is empty.
        An existing but unreadable object must never be overwritten.
        """
        response = self.client.list_objects_v2(
            Bucket=self.location.bucket,
            Prefix=key,
            MaxKeys=_EXISTENCE_LIST_MAX_KEYS,
        )
        if not isinstance(response, dict):
            return False
        metadata = response.get("ResponseMetadata")
        if (
            not isinstance(metadata, dict)
            or metadata.get("HTTPStatusCode") != 200
            or response.get("IsTruncated") is not False
            or response.get("Prefix", key) != key
            or response.get("Name", self.location.bucket) != self.location.bucket
            or response.get("CommonPrefixes")
            or response.get("NextContinuationToken")
        ):
            return False
        contents = response.get("Contents", [])
        if not isinstance(contents, list) or len(contents) > _EXISTENCE_LIST_MAX_KEYS:
            return False
        count = response.get("KeyCount", len(contents))
        if type(count) is not int or count != len(contents):
            return False
        keys = []
        for item in contents:
            if not isinstance(item, dict):
                return False
            item_key = item.get("Key")
            if not isinstance(item_key, str) or not item_key.startswith(key):
                return False
            keys.append(item_key)
        if len(set(keys)) != len(keys):
            return False
        return key not in keys

    def copy_to(self, name: str, destination: str | Path) -> SparkObjectDescription:
        validate_output_name(name)
        target = Path(destination)
        _flat_name(target.name)
        response = self.client.get_object(
            Bucket=self.location.bucket, Key=self.location.key(name)
        )
        body = response["Body"]
        try:
            with _directory_fd(target.parent, create=True) as directory:
                copied = _atomic_write(directory, target.name, body)
        finally:
            body.close()
        return SparkObjectDescription(name, copied.sha256, copied.bytes)

    def delete(self, name: str) -> None:
        self.client.delete_object(
            Bucket=self.location.bucket,
            Key=self.location.key(validate_output_name(name)),
        )

    def verify(self, description: SparkObjectDescription) -> None:
        validate_output_name(description.path)
        response = self.client.get_object(
            Bucket=self.location.bucket, Key=self.location.key(description.path)
        )
        body = response.get("Body")
        try:
            if body is None or not callable(getattr(body, "read", None)):
                raise OSError("S3 output response has no readable body")
            if (
                "ContentLength" in response
                and response["ContentLength"] != description.bytes
            ):
                raise ValueError(
                    "S3 output hash or size does not match its description"
                )
            digest = hashlib.sha256()
            size = 0
            while True:
                # Even a corrupt/oversized remote response is bounded by the
                # expected size plus one byte; never buffer a full object.
                chunk = body.read(min(_CHUNK_BYTES, description.bytes - size + 1))
                if not isinstance(chunk, bytes):
                    raise OSError("S3 output response did not return bytes")
                if not chunk:
                    break
                size += len(chunk)
                if size > description.bytes:
                    raise ValueError(
                        "S3 output hash or size does not match its description"
                    )
                digest.update(chunk)
            if size != description.bytes or digest.hexdigest() != description.sha256:
                raise ValueError(
                    "S3 output hash or size does not match its description"
                )
        finally:
            close = getattr(body, "close", None)
            if callable(close):
                close()

    def finish_run(self) -> None:
        pass


def create_output_store(
    output_uri: str,
    endpoint_url: str | None = None,
    region_name: str = "us-east-1",
    client_factory: Callable[..., Any] = create_s3_client,
    run_id: str | None = None,
    output_credentials_path: str | None = None,
    retain_s3_auxiliary: bool = False,
    allow_existing_output: bool = False,
    output_owner: str | None = None,
    resume_owned_output: bool = False,
) -> OutputStore:
    if type(retain_s3_auxiliary) is not bool:
        raise TypeError("retain_s3_auxiliary must be a boolean")
    if type(allow_existing_output) is not bool:
        raise TypeError("allow_existing_output must be a boolean")
    path = local_output_path(output_uri)
    if path is not None:
        if retain_s3_auxiliary:
            raise ValueError(
                "retaining auxiliary objects is supported only for S3 output"
            )
        return LocalOutputStore(path, run_id)
    options: dict[str, Any] = {}
    if output_credentials_path is not None:
        options["credentials"] = load_s3_credentials(output_credentials_path)
    client = client_factory(
        endpoint_url or "https://s3.amazonaws.com", region_name, **options
    )
    return S3OutputStore(
        output_uri,
        client,
        retain_auxiliary=retain_s3_auxiliary,
        allow_existing_output=allow_existing_output,
        output_owner=output_owner,
        resume_owned_output=resume_owned_output,
    )


__all__ = [
    "LocalOutputStore",
    "OutputStore",
    "S3OutputStore",
    "create_output_store",
    "is_retained_s3_auxiliary_name",
    "local_output_path",
]
