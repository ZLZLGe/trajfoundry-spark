"""Large sidecars publish atomically without exceeding a single S3 PUT."""

from __future__ import annotations

import hashlib
import io
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

from trajfoundry.spark import storage
from trajfoundry.spark.output import SparkObjectDescription

from .test_spark_storage_head403 import KEY, _denied, _MissingHeadForbidden


class MultipartS3(_MissingHeadForbidden):
    def __init__(self):
        super().__init__()
        self.created = []
        self.parts = []
        self.completed = []
        self.aborted = []
        self.fail_at = None
        self.abort_error = False
        self.race = False
        self.failure = RuntimeError("simulated upload failure")

    def create_multipart_upload(self, **kwargs):
        if self.fail_at == "create":
            raise self.failure
        self.created.append(kwargs)
        return {"UploadId": "upload-1"}

    def upload_part(self, **kwargs):
        if self.fail_at == "part" and kwargs["PartNumber"] == 2:
            raise self.failure
        assert kwargs["UploadId"] == "upload-1"
        assert kwargs["ContentLength"] == len(kwargs["Body"])
        self.parts.append(kwargs)
        return {"ETag": str(kwargs["PartNumber"])}

    def complete_multipart_upload(self, **kwargs):
        if self.fail_at == "complete":
            raise self.failure
        if self.race:
            self.objects[KEY] = b"concurrent object"
        if kwargs.get("IfNoneMatch") == "*" and KEY in self.objects:
            raise _denied(
                "CompleteMultipartUpload", code="PreconditionFailed", status=412
            )
        assert kwargs["MultipartUpload"]["Parts"] == [
            {"PartNumber": i, "ETag": str(i)} for i in range(1, len(self.parts) + 1)
        ]
        self.completed.append(kwargs)
        self.objects[KEY] = b"".join(part["Body"] for part in self.parts)
        self.metadata[KEY] = self.created[-1]["Metadata"]

    def abort_multipart_upload(self, **kwargs):
        self.aborted.append(kwargs)
        if self.abort_error:
            raise OSError("abort denied")


@pytest.fixture
def scaled_limits(monkeypatch):
    # Exercise real reads and the full commit path without allocating GiB.
    monkeypatch.setattr(storage, "_S3_SINGLE_PUT_MAX_BYTES", 10)
    monkeypatch.setattr(storage, "_MULTIPART_PART_BYTES", 5)


@pytest.mark.parametrize("as_file", [False, True])
def test_large_upload_is_bounded_verified_and_idempotent(
    tmp_path, scaled_limits, as_file
):
    client = MultipartS3()
    store = storage.S3OutputStore("s3://bucket/output/", client, output_owner="owner")
    payload = b"0123456789abcdefghi"
    source = tmp_path / "source.ndjson"
    source.write_bytes(payload)
    result = (
        store.put_file("data.jsonl", source, content_type="application/x-ndjson")
        if as_file
        else store.put_bytes("data.jsonl", payload, content_type="application/x-ndjson")
    )
    assert client.puts == 0
    assert [len(p["Body"]) for p in client.parts] == [5, 5, 5, 4]
    assert client.created[0]["Metadata"] == {
        "sha256": hashlib.sha256(payload).hexdigest(),
        "tf-owner": "owner",
    }
    assert client.created[0]["ContentType"] == "application/x-ndjson"
    assert client.completed[0]["IfNoneMatch"] == "*"
    assert not client.aborted
    store.verify(result)
    assert store.put_bytes("data.jsonl", payload) == result
    assert len(client.created) == 1
    with pytest.raises(FileExistsError):
        store.put_bytes("data.jsonl", b"different long payload")
    assert client.objects[KEY] == payload
    assert len(client.created) == 1


@pytest.mark.parametrize("size", [0, 10])
def test_small_and_limit_sized_objects_keep_single_put(scaled_limits, size):
    client = MultipartS3()
    storage.S3OutputStore("s3://bucket/output/", client).put_bytes(
        "data.jsonl", b"x" * size
    )
    assert client.puts == 1
    assert not client.created


def test_incident_sized_sidecar_never_uses_single_put(monkeypatch):
    client = MultipartS3()
    store = storage.S3OutputStore("s3://bucket/output/", client)
    multipart = Mock()
    monkeypatch.setattr(store, "_put_multipart", multipart)
    # Actual aggregate of the 2026-10-30 Boyue diagnostic parts: 6.34 GiB.
    description = SparkObjectDescription("data.jsonl", "a" * 64, 6_803_986_071)
    store._put(description, io.BytesIO(), "application/x-ndjson")
    assert multipart.call_count == 1
    assert client.puts == 0


@pytest.mark.parametrize("failure", ["create", "part", "complete"])
def test_failure_never_publishes_partial_object(scaled_limits, failure):
    client = MultipartS3()
    client.fail_at = failure
    with pytest.raises(RuntimeError) as caught:
        storage.S3OutputStore("s3://bucket/output/", client).put_bytes(
            "data.jsonl", b"0123456789abcdefghi"
        )
    assert caught.value is client.failure
    assert not client.objects
    assert not client.completed
    assert len(client.aborted) == (0 if failure == "create" else 1)


def test_abort_failure_preserves_original_upload_exception(scaled_limits):
    client = MultipartS3()
    client.fail_at = "part"
    client.abort_error = True
    with pytest.raises(RuntimeError) as caught:
        storage.S3OutputStore("s3://bucket/output/", client).put_bytes(
            "data.jsonl", b"0123456789abcdefghi"
        )
    assert caught.value is client.failure
    assert "uncommitted parts may remain" in caught.value.__notes__[0]


@pytest.mark.parametrize("payload", [b"short", b"x" * 20, b"x" * 19])
def test_source_changes_abort_before_commit(scaled_limits, payload):
    client = MultipartS3()
    store = storage.S3OutputStore("s3://bucket/output/", client)
    expected = b"0123456789abcdefghi"
    description = SparkObjectDescription(
        "data.jsonl", hashlib.sha256(expected).hexdigest(), len(expected)
    )
    with pytest.raises(ValueError, match="source.*changed"):
        store._put(description, io.BytesIO(payload), "application/x-ndjson")
    assert not client.completed
    assert not client.objects
    assert len(client.aborted) == 1


def test_concurrent_object_is_preserved_on_complete(scaled_limits):
    client = MultipartS3()
    client.race = True
    with pytest.raises(ClientError) as caught:
        storage.S3OutputStore("s3://bucket/output/", client).put_bytes(
            "data.jsonl", b"0123456789abcdefghi"
        )
    assert caught.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412
    assert client.objects[KEY] == b"concurrent object"
    assert len(client.aborted) == 1


@pytest.mark.parametrize("identical", [False, True])
def test_destination_rechecked_before_commit_on_ceph(
    monkeypatch, scaled_limits, identical
):
    client = MultipartS3()
    payload = b"0123456789abcdefghi"
    concurrent = payload if identical else b"different existing data"
    upload_part = client.upload_part

    def race(**kwargs):
        result = upload_part(**kwargs)
        client.objects[KEY] = concurrent
        client.metadata[KEY] = {"sha256": hashlib.sha256(concurrent).hexdigest()}
        return result

    monkeypatch.setattr(client, "upload_part", race)
    store = storage.S3OutputStore("s3://bucket/output/", client)
    if identical:
        store.verify(store.put_bytes("data.jsonl", payload))
    else:
        with pytest.raises(FileExistsError, match="existing object was preserved"):
            store.put_bytes("data.jsonl", payload)
    assert not client.completed
    assert client.objects[KEY] == concurrent
    assert len(client.aborted) == 1


def test_part_size_increases_to_stay_under_part_limit(monkeypatch, scaled_limits):
    monkeypatch.setattr(storage, "_MULTIPART_MAX_PARTS", 3)
    client = MultipartS3()
    storage.S3OutputStore("s3://bucket/output/", client).put_bytes(
        "data.jsonl", b"0123456789abcdefghi"
    )
    assert [len(part["Body"]) for part in client.parts] == [7, 7, 5]


def test_object_above_multipart_capacity_fails_before_upload(
    monkeypatch, scaled_limits
):
    monkeypatch.setattr(storage, "_MULTIPART_MAX_PARTS", 2)
    monkeypatch.setattr(storage, "_MULTIPART_MAX_PART_BYTES", 5)
    client = MultipartS3()
    with pytest.raises(ValueError, match="size limits"):
        storage.S3OutputStore("s3://bucket/output/", client).put_bytes(
            "data.jsonl", b"0123456789abcdefghi"
        )
    assert not client.created
