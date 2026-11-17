"""Date inventory, readiness, and shared-filesystem locks for lakehouse backfill."""

from __future__ import annotations

import fcntl
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from ..s3 import parse_s3_uri
from .source import _selected, inventory_fingerprint, inventory_item, merge_inventory

SOURCES = {
    "boyue": {
        "name": "boyue-rich",
        "input_base": "s3://agent-trajectory/lakehouse/boyue-rich/masked-raw/v002/",
        "output_base": "s3://agent-trajectory/lakehouse/boyue-rich/normalized/v001/",
        "date_prefix": "",
    },
    "deepinfra": {
        "name": "deep-infra",
        "input_base": "s3://agent-trajectory/lakehouse/deep-infra/masked-raw/v002/",
        "output_base": "s3://agent-trajectory/lakehouse/deep-infra/normalized/v002/",
        "date_prefix": "dt=",
    },
}


def date_uris(source, day):
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", day):
        raise ValueError("invalid date")
    date.fromisoformat(day)
    spec = SOURCES[source]
    return spec["input_base"] + spec["date_prefix"] + day + "/", spec[
        "output_base"
    ] + "dt=" + day + "/"


def frozen_yesterday():
    return (
        datetime.now(ZoneInfo("Asia/Shanghai")).date() - timedelta(days=1)
    ).isoformat()


def discover_dates(client, source, cutoff):
    spec = SOURCES[source]
    loc = parse_s3_uri(spec["input_base"])
    found = set()
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=loc.bucket, Prefix=loc.prefix, Delimiter="/"
    ):
        for item in page.get("CommonPrefixes", []):
            prefix = item["Prefix"]
            if not prefix.startswith(loc.prefix):
                raise ValueError("unexpected date prefix")
            day = (
                prefix[len(loc.prefix) :].rstrip("/").removeprefix(spec["date_prefix"])
            )
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", day) and day <= cutoff:
                date.fromisoformat(day)
                found.add(day)
    return sorted(found)


def scan_inventory(client, uri, input_format, *, workers=16):
    """Two fingerprints: all physical files for readiness, selected files for read pinning."""
    loc = parse_s3_uri(uri)
    prefixes = []
    manifest_objects = []
    manifest = None

    def root_entries():
        for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=loc.bucket, Prefix=loc.prefix, Delimiter="/"
        ):
            prefixes.extend(item["Prefix"] for item in page.get("CommonPrefixes", []))
            if len(prefixes) > 1_000_000:
                raise ValueError("date directory inventory limit exceeded")
            for item in page.get("Contents", []):
                if item["Key"] == loc.key("manifest.json"):
                    manifest_objects.append(item)
                yield item

    def summarize(objects):
        full = selected = (0, 0, 0)
        latest = 0.0
        parquet = json_objects = 0
        for item in objects:
            key = item["Key"]
            if not key.startswith(loc.prefix):
                raise ValueError("inventory returned object outside date")
            name = key[len(loc.prefix) :]
            if not name or key.endswith("/"):
                continue
            size, etag = item["Size"], item["ETag"]
            if (
                type(size) is not int
                or size < 0
                or not isinstance(etag, str)
                or not etag
            ):
                raise ValueError("invalid inventory descriptor")
            summary = inventory_item((name, size, etag))
            full = merge_inventory(full, summary)
            latest = max(latest, item["LastModified"].timestamp())
            if _selected(name, input_format):
                selected = merge_inventory(selected, summary)
                parquet += int(name.endswith(".parquet"))
                json_objects += int(name.endswith(".json"))
        return full, selected, latest, parquet, json_objects

    def scan_prefix(prefix):
        if not prefix.startswith(loc.prefix):
            raise ValueError("inventory prefix outside date")
        objects = (
            item
            for pg in client.get_paginator("list_objects_v2").paginate(
                Bucket=loc.bucket, Prefix=prefix
            )
            for item in pg.get("Contents", [])
        )
        return summarize(objects)

    full, selected, latest, parquet, json_objects = summarize(root_entries())
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for one, chosen, modified, pq_count, js_count in executor.map(
            scan_prefix, prefixes
        ):
            full = merge_inventory(full, one)
            selected = merge_inventory(selected, chosen)
            latest = max(latest, modified)
            parquet += pq_count
            json_objects += js_count
    for item in manifest_objects:
        if item["Key"] != loc.key("manifest.json"):
            continue
        if item["Size"] > 8 * 1024 * 1024:
            raise ValueError("upstream readiness manifest exceeds byte budget")
        response = client.get_object(
            Bucket=loc.bucket, Key=item["Key"], IfMatch=item["ETag"]
        )
        try:
            if response.get("ETag") != item["ETag"]:
                raise ValueError("upstream manifest changed during inventory")
            manifest = json.loads(response["Body"].read(8 * 1024 * 1024 + 1))
        finally:
            response["Body"].close()
        if not isinstance(manifest, dict):
            raise TypeError("invalid upstream manifest")
    return {
        "observed_at": time.time(),
        "full_fingerprint": inventory_fingerprint(full),
        "selected_fingerprint": inventory_fingerprint(selected),
        "physical_objects": full[0],
        "physical_bytes": full[1],
        "selected_objects": selected[0],
        "selected_bytes": selected[1],
        "parquet_objects": parquet,
        "json_objects": json_objects,
        "latest_modified": latest,
        "upstream_manifest_present": manifest is not None,
        "upstream_succeeded": manifest is not None
        and str(manifest.get("status", "")).upper()
        in {"SUCCEEDED", "SUCCESS", "COMPLETED"},
    }


def readiness(current, previous, *, day, now=None):
    now = time.time() if now is None else now
    if current["upstream_manifest_present"]:
        return current["upstream_succeeded"], "upstream_manifest"
    end = datetime.combine(
        date.fromisoformat(day) + timedelta(days=1),
        datetime.min.time(),
        ZoneInfo("Asia/Shanghai"),
    ).timestamp()
    if now - max(end, current["latest_modified"]) < 72 * 3600:
        return False, "await_72h_without_manifest"
    if not previous or current["full_fingerprint"] != previous["full_fingerprint"]:
        return False, "await_stable_inventory"
    if current["observed_at"] - previous["observed_at"] < 1800:
        return False, "await_30m_between_inventories"
    return True, "historical_stable_inventory"


def atomic_json(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + f".{os.getpid()}.tmp")
    with temporary.open("x", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, sort_keys=True, indent=2)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


@contextmanager
def shared_lock(path):
    """All backfill invocations use the same private shared directory.

    No S3 conditional PUT is assumed: some compatible gateways ignore it.
    Never steal a running state after a process crash; reconcile its Driver first.
    """
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        os.close(descriptor)
