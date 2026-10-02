"""Executor-local S3 shard readers for the Spark normalizer.

The driver never lists the input prefix.  A Spark partition owns one or more
relative prefix shards and lists/reads those objects with its own short-lived
S3 client.  Payloads are yielded one at a time, so no complete input manifest
or snapshot is materialized in driver memory.
"""

from __future__ import annotations

import hashlib
import io
from collections.abc import Iterator
from typing import Any

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


def _read_checked(client: Any, location: S3Location, key: str, size: int, etag: str) -> bytes:
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
        payload = body.read()
        if not isinstance(payload, bytes) or len(payload) != size:
            raise OSError("S3 object body size did not match the listing")
        return payload
    finally:
        close = getattr(body, "close", None)
        if callable(close):
            close()


def _iter_sxf_rows(source_ref: str, payload: bytes) -> Iterator[tuple[str, bytes, str]]:
    try:
        import zstandard
    except ImportError as error:  # pragma: no cover - packaging error
        raise RuntimeError("SXF input requires zstandard") from error
    with io.BufferedReader(zstandard.ZstdDecompressor().stream_reader(io.BytesIO(payload))) as reader:
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
) -> Iterator[tuple[str, bytes, str]]:
    """List and read one input shard, yielding ``(source_ref, bytes, sha256)``."""

    location = parse_s3_uri(input_uri)
    if shard.startswith("/") or ".." in shard.split("/"):
        raise ValueError(f"unsafe input shard: {shard!r}")
    shard_prefix = f"{location.prefix}{shard}"
    client = create_s3_client(
        endpoint_url or "https://s3.amazonaws.com",
        region_name,
        max_pool_connections=max_pool_connections,
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
            if not isinstance(size, int) or size < 0 or not isinstance(etag, str) or not etag:
                raise TypeError("S3 listing entry has invalid size or ETag")
            payload = _read_checked(client, location, key, size, etag)
            if input_format == "sxf":
                yield from _iter_sxf_rows(source_ref, payload)
            else:
                yield source_ref, payload, hashlib.sha256(payload).hexdigest()


__all__ = ["iter_shard_payloads"]
