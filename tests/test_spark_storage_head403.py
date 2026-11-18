"""Ceph may hide a missing key behind HEAD 403; existing keys stay protected."""

from __future__ import annotations

import copy

import pytest
from botocore.exceptions import ClientError

from trajfoundry.spark.storage import S3OutputStore

from .test_spark_storage import _S3

KEY = "output/data.jsonl"


def _denied(operation="HeadObject", *, code="403", status=403):
    return ClientError(
        {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}},
        operation,
    )


class _MissingHeadForbidden(_S3):
    def __init__(self):
        super().__init__()
        self.list_calls = []
        self.always_deny_head = False
        self.head_error = _denied()
        self.list_error = None
        self.put_conditions = []
        self.list_response = {
            "ResponseMetadata": {"HTTPStatusCode": 200},
            "IsTruncated": False,
            "Name": "bucket",
            "Prefix": KEY,
            "KeyCount": 0,
        }

    def head_object(self, *, Bucket, Key):
        if self.always_deny_head or Key not in self.objects:
            raise self.head_error
        return super().head_object(Bucket=Bucket, Key=Key)

    def list_objects_v2(self, **kwargs):
        self.list_calls.append(kwargs)
        if self.list_error:
            raise self.list_error
        return copy.deepcopy(self.list_response)

    def put_object(self, **kwargs):
        self.put_conditions.append(kwargs.get("IfNoneMatch"))
        if kwargs.get("IfNoneMatch") == "*" and kwargs["Key"] in self.objects:
            raise _denied("PutObject", code="PreconditionFailed", status=412)
        return super().put_object(**kwargs)


@pytest.mark.parametrize("as_file", [False, True])
def test_missing_head403_requires_complete_exact_key_listing_before_put(tmp_path, as_file):
    client = _MissingHeadForbidden()
    store = S3OutputStore("s3://bucket/output/", client)
    source = tmp_path / "data"
    source.write_bytes(b"correct\n")
    description = (
        store.put_file("data.jsonl", source)
        if as_file
        else store.put_bytes("data.jsonl", b"correct\n")
    )
    assert client.list_calls == [{"Bucket": "bucket", "Prefix": KEY, "MaxKeys": 2}]
    assert client.puts == 1
    assert client.put_conditions == ["*"]
    store.verify(description)
    assert client.body.closed
    # A retry uses the existing object's ordinary successful HEAD, not another
    # LIST or PUT; the original byte and hash guards are unchanged.
    assert store.put_bytes("data.jsonl", b"correct\n") == description
    assert len(client.list_calls) == client.puts == 1
    with pytest.raises(FileExistsError, match="existing object was preserved"):
        store.put_bytes("data.jsonl", b"replacement\n")
    assert client.objects[KEY] == b"correct\n"
    assert client.puts == 1


def test_similar_prefix_is_not_the_exact_object_key():
    client = _MissingHeadForbidden()
    sibling = KEY + ".retained"
    client.objects[sibling] = b"keep"
    client.list_response.update(Contents=[{"Key": sibling}], KeyCount=1)
    store = S3OutputStore("s3://bucket/output/", client)
    store.put_bytes("data.jsonl", b"new")
    assert client.objects[sibling] == b"keep"
    assert client.objects[KEY] == b"new"


def test_key_created_after_absence_listing_is_not_overwritten(monkeypatch):
    client = _MissingHeadForbidden()
    original_list = client.list_objects_v2

    def racing_list(**kwargs):
        response = original_list(**kwargs)
        client.objects[KEY] = b"concurrent-object"
        return response

    monkeypatch.setattr(client, "list_objects_v2", racing_list)
    with pytest.raises(ClientError) as error:
        S3OutputStore("s3://bucket/output/", client).put_bytes("data.jsonl", b"new")
    assert error.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412
    assert client.put_conditions == ["*"]
    assert client.puts == 0
    assert client.objects[KEY] == b"concurrent-object"


@pytest.mark.parametrize("existing_payload", [b"correct", b"different"])
def test_exact_existing_key_head403_is_never_overwritten_or_reinterpreted(existing_payload):
    client = _MissingHeadForbidden()
    client.always_deny_head = True
    client.objects[KEY] = existing_payload
    client.list_response.update(Contents=[{"Key": KEY}], KeyCount=1)
    store = S3OutputStore("s3://bucket/output/", client)
    with pytest.raises(ClientError) as error:
        store.put_bytes("data.jsonl", b"correct")
    assert error.value is client.head_error
    assert client.puts == 0
    assert client.objects[KEY] == existing_payload
    assert client.body is None


@pytest.mark.parametrize(
    "response",
    [
        None,
        [],
        {},
        {"ResponseMetadata": {"HTTPStatusCode": 200}},
        {"ResponseMetadata": {"HTTPStatusCode": 200}, "IsTruncated": True},
        {"ResponseMetadata": {"HTTPStatusCode": 403}, "IsTruncated": False},
        {"ResponseMetadata": None, "IsTruncated": False},
    ],
)
def test_absent_truncated_or_failed_listing_cannot_prove_absence(response):
    client = _MissingHeadForbidden()
    client.list_response = response
    with pytest.raises(ClientError) as error:
        S3OutputStore("s3://bucket/output/", client).put_bytes("data.jsonl", b"new")
    assert error.value is client.head_error
    assert client.puts == 0


@pytest.mark.parametrize(
    "changes",
    [
        {"IsTruncated": 0},
        {"IsTruncated": "false"},
        {"Contents": None},
        {"Contents": {}},
        {"Contents": [None], "KeyCount": 1},
        {"Contents": [{}], "KeyCount": 1},
        {"Contents": [{"Key": 17}], "KeyCount": 1},
        {"Contents": [{"Key": "other-prefix/data.jsonl"}], "KeyCount": 1},
        {"Contents": [{"Key": KEY + "x"}] * 2, "KeyCount": 2},
        {"Contents": [{"Key": KEY + str(i)} for i in range(3)], "KeyCount": 3},
        {"KeyCount": 1},
        {"KeyCount": False},
        {"KeyCount": "0"},
        {"Prefix": "other-prefix/"},
        {"Name": "other-bucket"},
        {"CommonPrefixes": [{"Prefix": KEY}]},
        {"NextContinuationToken": "more-keys"},
    ],
)
def test_malformed_listing_never_authorizes_put(changes):
    client = _MissingHeadForbidden()
    client.list_response.update(changes)
    with pytest.raises(ClientError) as error:
        S3OutputStore("s3://bucket/output/", client).put_bytes("data.jsonl", b"new")
    assert error.value is client.head_error
    assert client.puts == 0


@pytest.mark.parametrize("listing_error", [_denied("ListObjectsV2"), TimeoutError("timeout")])
def test_listing_permission_and_network_errors_propagate_without_put(listing_error):
    client = _MissingHeadForbidden()
    client.list_error = listing_error
    with pytest.raises(type(listing_error)) as error:
        S3OutputStore("s3://bucket/output/", client).put_bytes("data.jsonl", b"new")
    assert error.value is listing_error
    assert client.puts == 0


@pytest.mark.parametrize(
    ("code", "status"),
    [
        ("SignatureDoesNotMatch", 403),
        ("InvalidAccessKeyId", 403),
        ("ExpiredToken", 403),
        ("InvalidToken", 403),
        ("AccessDenied", 500),
        ("ServiceUnavailable", 503),
    ],
)
def test_explicit_authentication_or_server_error_does_not_enter_absence_fallback(code, status):
    client = _MissingHeadForbidden()
    client.head_error = _denied(code=code, status=status)
    with pytest.raises(ClientError) as error:
        S3OutputStore("s3://bucket/output/", client).put_bytes("data.jsonl", b"new")
    assert error.value is client.head_error
    assert client.list_calls == []
    assert client.puts == 0


def test_output_delete_permission_failure_is_not_suppressed(monkeypatch):
    client = _MissingHeadForbidden()
    store = S3OutputStore("s3://bucket/output/", client)
    store.put_bytes("data.jsonl", b"correct")

    def forbidden_delete(**_kwargs):
        raise _denied("DeleteObject", code="AccessDenied")

    monkeypatch.setattr(client, "delete_object", forbidden_delete)
    with pytest.raises(ClientError) as error:
        store.delete("data.jsonl")
    assert error.value.operation_name == "DeleteObject"
    assert client.objects[KEY] == b"correct"


def test_put_permission_failure_is_not_suppressed_after_confirmed_absence(monkeypatch):
    client = _MissingHeadForbidden()

    def forbidden_put(**_kwargs):
        raise _denied("PutObject", code="AccessDenied")

    monkeypatch.setattr(client, "put_object", forbidden_put)
    with pytest.raises(ClientError) as error:
        S3OutputStore("s3://bucket/output/", client).put_bytes("data.jsonl", b"new")
    assert error.value.operation_name == "PutObject"
    assert client.objects == {}


def test_get_permission_failure_is_not_suppressed_after_successful_put(monkeypatch):
    client = _MissingHeadForbidden()
    store = S3OutputStore("s3://bucket/output/", client)
    description = store.put_bytes("data.jsonl", b"correct")

    def forbidden_get(**_kwargs):
        raise _denied("GetObject", code="AccessDenied")

    monkeypatch.setattr(client, "get_object", forbidden_get)
    with pytest.raises(ClientError) as error:
        store.verify(description)
    assert error.value.operation_name == "GetObject"
    assert client.objects[KEY] == b"correct"
