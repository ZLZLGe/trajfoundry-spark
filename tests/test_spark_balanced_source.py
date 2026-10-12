import hashlib
import io
from unittest.mock import Mock

import pytest

from trajfoundry.spark import source


def test_listing_emits_only_references_and_reads_are_separate(monkeypatch):
    payload = b'{"synthetic":true}'
    client = Mock()
    client.get_paginator.return_value.paginate.return_value = [
        {
            "Contents": [
                {"Key": "in/user/req_a.json", "Size": len(payload), "ETag": '"a"'},
                {"Key": "in/user/other.json", "Size": 2, "ETag": '"b"'},
                {"Key": "in/other/req_a.json", "Size": 2, "ETag": '"b"'},
            ]
        }
    ]
    monkeypatch.setattr(source, "create_s3_client", Mock(return_value=client))
    options = {
        "input_uri": "s3://bucket/in/",
        "input_format": "tokenplan",
        "endpoint_url": None,
        "region_name": "unused",
    }
    refs = list(source.list_shard_objects(["user/"], **options))
    assert refs == [("user/req_a.json", len(payload), '"a"')]
    client.get_object.assert_not_called()
    body = io.BytesIO(payload)
    client.get_object.return_value = {
        "Body": body,
        "ContentLength": len(payload),
        "ETag": '"a"',
    }
    assert list(source.read_listed_objects(iter(refs), **options)) == [
        ("user/req_a.json", payload, hashlib.sha256(payload).hexdigest())
    ]
    client.get_object.assert_called_once_with(
        Bucket="bucket", Key="in/user/req_a.json", IfMatch='"a"'
    )
    assert body.closed


def test_auto_prefix_discovery_uses_delimiter_and_returns_immediate_shards(monkeypatch):
    client = Mock()
    client.get_paginator.return_value.paginate.return_value = [
        {
            "CommonPrefixes": [
                {"Prefix": "in/day/no_session_id/"},
                {"Prefix": "in/day/user-b/"},
            ],
            "Contents": [{"Key": "in/day/_SUCCESS"}],
        },
        {"CommonPrefixes": [{"Prefix": "in/day/user-a/"}]},
    ]
    monkeypatch.setattr(source, "create_s3_client", Mock(return_value=client))
    assert list(
        source.discover_prefix_shards(
            "s3://bucket/in/day/",
            input_format="tokenplan",
            endpoint_url=None,
            region_name="unused",
        )
    ) == ["no_session_id/", "user-a/", "user-b/"]
    client.get_paginator.return_value.paginate.assert_called_once_with(
        Bucket="bucket", Prefix="in/day/", Delimiter="/"
    )


def test_auto_prefix_discovery_is_bounded_and_fails_on_nested_prefix(monkeypatch):
    client = Mock()
    client.get_paginator.return_value.paginate.return_value = [
        {"CommonPrefixes": [{"Prefix": "in/day/user/session/"}]}
    ]
    monkeypatch.setattr(source, "create_s3_client", Mock(return_value=client))
    with pytest.raises(OSError, match="non-immediate"):
        list(
            source.discover_prefix_shards(
                "s3://bucket/in/day/",
                input_format="tokenplan",
                endpoint_url=None,
                region_name="unused",
            )
        )

    client.get_paginator.return_value.paginate.return_value = [
        {
            "CommonPrefixes": [
                {"Prefix": "in/day/user-a/"},
                {"Prefix": "in/day/user-b/"},
            ]
        }
    ]
    with pytest.raises(RuntimeError, match="max_prefix_shards"):
        list(
            source.discover_prefix_shards(
                "s3://bucket/in/day/",
                input_format="tokenplan",
                endpoint_url=None,
                region_name="unused",
                max_prefix_shards=1,
            )
        )


def test_auto_prefix_discovery_rejects_selected_root_objects(monkeypatch):
    client = Mock()
    client.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Key": "in/day/req_root.json"}]}
    ]
    monkeypatch.setattr(source, "create_s3_client", Mock(return_value=client))
    with pytest.raises(ValueError, match="below a user prefix"):
        list(
            source.discover_prefix_shards(
                "s3://bucket/in/day/",
                input_format="tokenplan",
                endpoint_url=None,
                region_name="unused",
            )
        )


def test_auto_prefix_discovery_rejects_endpoint_that_ignores_delimiter(monkeypatch):
    client = Mock()
    client.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Key": "in/day/user/req_a.json"}]}
    ]
    monkeypatch.setattr(source, "create_s3_client", Mock(return_value=client))
    with pytest.raises(RuntimeError, match="ignores|unsupported"):
        list(
            source.discover_prefix_shards(
                "s3://bucket/in/day/",
                input_format="tokenplan",
                endpoint_url=None,
                region_name="unused",
            )
        )


def test_auto_prefix_discovery_requires_directory_input_uri(monkeypatch):
    with pytest.raises(ValueError, match="end with"):
        list(
            source.discover_prefix_shards(
                "s3://bucket/in/day",
                input_format="tokenplan",
                endpoint_url=None,
                region_name="unused",
            )
        )


@pytest.mark.parametrize(
    "contents",
    [None, "", {}, [None], [{"Size": 1}], {"Key": "value"}],
)
def test_auto_prefix_discovery_rejects_malformed_contents(monkeypatch, contents):
    client = Mock()
    client.get_paginator.return_value.paginate.return_value = [
        {"Contents": contents}
    ]
    monkeypatch.setattr(source, "create_s3_client", Mock(return_value=client))
    with pytest.raises(OSError, match="malformed|unsafe object"):
        list(
            source.discover_prefix_shards(
                "s3://bucket/in/day/",
                input_format="tokenplan",
                endpoint_url=None,
                region_name="unused",
            )
        )


@pytest.mark.parametrize("page", [None, {"CommonPrefixes": {"Prefix": "in/day/user/"}}])
def test_auto_prefix_discovery_rejects_malformed_delimiter_page(monkeypatch, page):
    client = Mock()
    client.get_paginator.return_value.paginate.return_value = [page]
    monkeypatch.setattr(source, "create_s3_client", Mock(return_value=client))
    with pytest.raises(OSError, match="malformed|unsafe"):
        list(
            source.discover_prefix_shards(
                "s3://bucket/in/day/",
                input_format="tokenplan",
                endpoint_url=None,
                region_name="unused",
            )
        )


def test_oversize_object_rejected_before_download(monkeypatch):
    client = Mock()
    monkeypatch.setattr(source, "create_s3_client", Mock(return_value=client))
    with pytest.raises(ValueError, match="byte budget"):
        list(
            source.read_listed_objects(
                [("user/req_a.json", 100, '"a"')],
                input_uri="s3://bucket/in/",
                input_format="tokenplan",
                endpoint_url=None,
                region_name="unused",
                max_object_bytes=10,
            )
        )
    client.get_object.assert_not_called()


def test_rebalanced_read_retains_etag_check_and_closes_body(monkeypatch):
    client = Mock()
    body = io.BytesIO(b"abc")
    client.get_object.return_value = {
        "Body": body,
        "ContentLength": 3,
        "ETag": '"changed"',
    }
    monkeypatch.setattr(source, "create_s3_client", Mock(return_value=client))
    with pytest.raises(OSError, match="ETag changed"):
        list(
            source.read_listed_objects(
                [("user/req_a.json", 3, '"a"')],
                input_uri="s3://bucket/in/",
                input_format="tokenplan",
                endpoint_url=None,
                region_name="unused",
            )
        )
    assert body.closed
