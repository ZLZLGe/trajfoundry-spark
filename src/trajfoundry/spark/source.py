"""Executor-local S3 shard readers for the Spark normalizer.

The driver never lists input objects.  A Spark discovery partition may list
only the immediate user-directory prefixes with S3's delimiter API; the small
bounded result is collected by the driver.  Normal Spark partitions then own
one or more relative prefix shards and list/read objects with short-lived S3
clients.  Payloads are yielded one at a time, so no complete input manifest or
snapshot is materialized in driver memory.
"""

from __future__ import annotations

import hashlib
import io
from collections.abc import Iterable, Iterator
from typing import Any

from ..credentials import load_s3_credentials
from ..s3 import S3Location, create_s3_client, parse_s3_uri


def _selected(source_ref: str, input_format: str) -> bool:
    basename = source_ref.rsplit("/", 1)[-1]
    if input_format in {"freerouter", "deepinfra"}:
        return basename.endswith(".json")
    if input_format == "tokenplan":
        return basename.startswith("req_") and basename.endswith(".json")
    if input_format == "sxf":
        return basename.endswith(".jsonl.zst")
    raise ValueError(f"unsupported input format: {input_format!r}")


def _read_checked(
    client: Any,
    location: S3Location,
    key: str,
    size: int,
    etag: str,
    *,
    max_bytes: int = 64 * 1024 * 1024,
) -> bytes:
    if size > max_bytes:
        raise ValueError("input object exceeds configured byte budget")
    response = client.get_object(
        Bucket=location.bucket,
        Key=key,
        IfMatch=etag,
    )
    body = response.get("Body")
    try:
        if body is None or not callable(getattr(body, "read", None)):
            raise OSError("S3 object response has no readable body")
        if response.get("ETag") != etag:
            raise OSError("S3 object ETag changed while it was being read")
        content_length = response.get("ContentLength")
        if content_length != size:
            raise OSError("S3 object size changed while it was being read")
        payload = body.read(size + 1)
        if not isinstance(payload, bytes) or len(payload) != size:
            raise OSError("S3 object body size did not match the listing")
        return payload
    finally:
        close = getattr(body, "close", None)
        if callable(close):
            close()


def _input_client(
    endpoint_url, region_name, input_credentials_path, max_pool_connections=16
):
    options: dict[str, Any] = {"max_pool_connections": max_pool_connections}
    if input_credentials_path is not None:
        options["credentials"] = load_s3_credentials(input_credentials_path)
    return create_s3_client(
        endpoint_url or "https://s3.amazonaws.com", region_name, **options
    )


def _listed_objects(client, location, shard, input_format):
    if not shard or shard.startswith("/") or ".." in shard.split("/") or "\\" in shard:
        raise ValueError("unsafe input shard")
    prefix = f"{location.prefix}{shard}"
    pages = client.get_paginator("list_objects_v2").paginate(
        Bucket=location.bucket, Prefix=prefix
    )
    for page in pages:
        for item in page.get("Contents", ()) or ():
            key = item.get("Key")
            if (
                not isinstance(key, str)
                or not key.startswith(prefix)
                or key.endswith("/")
            ):
                continue
            source_ref = key[len(location.prefix) :]
            if not source_ref or not _selected(source_ref, input_format):
                continue
            if location.key(source_ref) != key:
                raise OSError("S3 listing returned an unsafe relative key")
            size, etag = item.get("Size"), item.get("ETag")
            if (
                type(size) is not int
                or size < 0
                or not isinstance(etag, str)
                or not etag
            ):
                raise TypeError("S3 listing entry has invalid size or ETag")
            yield source_ref, size, etag


def discover_prefix_shards(
    input_uri: str,
    *,
    input_format: str,
    endpoint_url: str | None,
    region_name: str,
    input_credentials_path: str | None = None,
    max_prefix_shards: int = 1_000_000,
) -> Iterator[str]:
    """Discover one-level user prefixes without listing input objects.

    S3's delimiter listing returns only ``CommonPrefixes`` below the date
    prefix.  The resulting relative directory names are small enough to
    collect as a bounded Spark discovery result; object keys and bodies remain
    executor-local in the existing shard reader.  We intentionally fail
    closed on an unexpected response instead of falling back to a Driver-side
    object listing.
    """

    if type(max_prefix_shards) is not int or max_prefix_shards <= 0:
        raise ValueError("max_prefix_shards must be a positive integer")
    location = parse_s3_uri(input_uri)
    client = _input_client(endpoint_url, region_name, input_credentials_path)
    seen: set[str] = set()
    paginator = client.get_paginator("list_objects_v2")
    pages = paginator.paginate(
        Bucket=location.bucket,
        Prefix=location.prefix,
        Delimiter="/",
    )
    saw_nested_content = False
    for page in pages:
        if not isinstance(page, dict):
            raise OSError("S3 delimiter listing returned a malformed page")
        # Automatic mode intentionally models the token-plan layout as
        # ``<user>/<session>/req_*.json``.  Do not silently skip a selected
        # object placed directly under the date root; that would produce a
        # partial successful output.
        contents = page.get("Contents", ())
        if not isinstance(contents, (list, tuple)):
            raise OSError("S3 delimiter listing returned malformed Contents")
        for item in contents:
            key = item.get("Key") if isinstance(item, dict) else None
            if not isinstance(key, str) or not key.startswith(location.prefix):
                raise OSError("S3 delimiter listing returned an unsafe object")
            source_ref = key[len(location.prefix) :]
            if "/" in source_ref:
                # A compliant delimiter response should put nested keys in
                # CommonPrefixes, not Contents. Seeing them means the
                # endpoint ignored Delimiter (or returned a malformed page),
                # so returning an empty shard list would silently omit data.
                saw_nested_content = True
            if "/" not in source_ref and _selected(source_ref, input_format):
                raise ValueError(
                    "automatic input prefix discovery requires selected objects "
                    "to be below a user prefix"
                )
        common_prefixes = page.get("CommonPrefixes", ())
        if not isinstance(common_prefixes, (list, tuple)):
            raise OSError(
                "S3 delimiter listing returned malformed CommonPrefixes"
            )
        for item in common_prefixes:
            full_prefix = item.get("Prefix") if isinstance(item, dict) else None
            if not isinstance(full_prefix, str) or not full_prefix.startswith(
                location.prefix
            ):
                raise OSError("S3 delimiter listing returned an unsafe prefix")
            shard = full_prefix[len(location.prefix) :]
            # Delimiter listing should produce exactly one immediate directory
            # segment (including the trailing slash), never a session prefix.
            if not shard.endswith("/") or shard.count("/") != 1:
                raise OSError("S3 delimiter listing returned a non-immediate prefix")
            if (
                not shard
                or shard.startswith("/")
                or ".." in shard.split("/")
                or "\\" in shard
                or any(ord(char) < 32 or ord(char) == 127 for char in shard)
            ):
                raise OSError("S3 delimiter listing returned an unsafe prefix")
            seen.add(shard)
            if len(seen) > max_prefix_shards:
                raise RuntimeError(
                    "automatic input prefix discovery exceeded max_prefix_shards"
                )
    if saw_nested_content:
        raise RuntimeError(
            "S3 delimiter listing returned nested objects without CommonPrefixes; "
            "automatic prefix discovery is unsupported by this endpoint"
        )
    yield from sorted(seen)


def list_shard_objects(
    shards: Iterable[str],
    *,
    input_uri: str,
    input_format: str,
    endpoint_url: str | None,
    region_name: str,
    input_credentials_path: str | None = None,
) -> Iterator[tuple[str, int, str]]:
    """Only small immutable object references cross the read-balancing shuffle."""
    location = parse_s3_uri(input_uri)
    client = _input_client(endpoint_url, region_name, input_credentials_path)
    for shard in shards:
        yield from _listed_objects(client, location, shard, input_format)


def read_listed_objects(
    objects: Iterable[tuple[str, int, str]],
    *,
    input_uri: str,
    input_format: str,
    endpoint_url: str | None,
    region_name: str,
    input_credentials_path: str | None = None,
    max_object_bytes: int = 64 * 1024 * 1024,
) -> Iterator[tuple[str, bytes, str]]:
    """Fetch one object at a time after balancing a huge user across read tasks."""
    location = parse_s3_uri(input_uri)
    client = _input_client(endpoint_url, region_name, input_credentials_path)
    for source_ref, size, etag in objects:
        payload = _read_checked(
            client,
            location,
            location.key(source_ref),
            size,
            etag,
            max_bytes=max_object_bytes,
        )
        if input_format == "sxf":
            yield from _iter_sxf_rows(source_ref, payload)
        else:
            yield source_ref, payload, hashlib.sha256(payload).hexdigest()


def _iter_sxf_rows(source_ref: str, payload: bytes) -> Iterator[tuple[str, bytes, str]]:
    try:
        import zstandard
    except ImportError as error:  # pragma: no cover - packaging error
        raise RuntimeError("SXF input requires zstandard") from error
    with io.BufferedReader(
        zstandard.ZstdDecompressor().stream_reader(io.BytesIO(payload))
    ) as reader:
        line_no = 0
        while line := reader.readline():
            row = line.rstrip(b"\r\n")
            yield (
                f"{source_ref}#L{line_no:08d}",
                row,
                hashlib.sha256(row).hexdigest(),
            )
            line_no += 1


def iter_shard_payloads(
    input_uri: str,
    shard: str,
    *,
    input_format: str,
    endpoint_url: str | None,
    region_name: str,
    max_pool_connections: int = 16,
    input_credentials_path: str | None = None,
) -> Iterator[tuple[str, bytes, str]]:
    """List and read one input shard, yielding ``(source_ref, bytes, sha256)``.

    Explicit credentials are loaded inside this executor-side iterator, never
    in a driver closure. A missing or unsafe explicit file fails closed rather
    than silently selecting a different identity from the default SDK chain.
    """

    location = parse_s3_uri(input_uri)
    if shard.startswith("/") or ".." in shard.split("/"):
        raise ValueError(f"unsafe input shard: {shard!r}")
    shard_prefix = f"{location.prefix}{shard}"
    client_options: dict[str, Any] = {"max_pool_connections": max_pool_connections}
    if input_credentials_path is not None:
        client_options["credentials"] = load_s3_credentials(input_credentials_path)
    client = create_s3_client(
        endpoint_url or "https://s3.amazonaws.com",
        region_name,
        **client_options,
    )
    paginator = client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=location.bucket, Prefix=shard_prefix)
    for page in pages:
        for item in page.get("Contents", ()) or ():
            key = item.get("Key")
            if not isinstance(key, str) or not key.startswith(location.prefix):
                continue
            if key.endswith("/"):
                continue
            source_ref = key[len(location.prefix) :]
            if not source_ref or not _selected(source_ref, input_format):
                continue
            if location.key(source_ref) != key:
                raise OSError("S3 listing returned an unsafe relative key")
            size = item.get("Size")
            etag = item.get("ETag")
            if (
                not isinstance(size, int)
                or size < 0
                or not isinstance(etag, str)
                or not etag
            ):
                raise TypeError("S3 listing entry has invalid size or ETag")
            payload = _read_checked(client, location, key, size, etag)
            if input_format == "sxf":
                yield from _iter_sxf_rows(source_ref, payload)
            else:
                yield source_ref, payload, hashlib.sha256(payload).hexdigest()


__all__ = ["discover_prefix_shards", "iter_shard_payloads"]
