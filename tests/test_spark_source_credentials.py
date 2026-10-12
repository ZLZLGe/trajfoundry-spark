from __future__ import annotations

from unittest.mock import Mock

import pytest

from trajfoundry.credentials import CredentialFileError, S3Credentials
from trajfoundry.spark import source


def _empty_client() -> Mock:
    client = Mock()
    client.get_paginator.return_value.paginate.return_value = [{"Contents": []}]
    return client


def test_default_input_credentials_preserve_sdk_chain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loader = Mock(side_effect=AssertionError("explicit loader must not run"))
    factory = Mock(return_value=_empty_client())
    monkeypatch.setattr(source, "load_s3_credentials", loader)
    monkeypatch.setattr(source, "create_s3_client", factory)

    assert (
        list(
            source.iter_shard_payloads(
                "s3://bucket/input/",
                "user/",
                input_format="tokenplan",
                endpoint_url=None,
                region_name="us-east-1",
                max_pool_connections=3,
            )
        )
        == []
    )

    loader.assert_not_called()
    factory.assert_called_once_with(
        "https://s3.amazonaws.com",
        "us-east-1",
        max_pool_connections=3,
    )


def test_explicit_credentials_are_loaded_lazily_on_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    credentials = S3Credentials("synthetic-key", "synthetic-secret")
    loader = Mock(return_value=credentials)
    client = _empty_client()
    factory = Mock(return_value=client)
    monkeypatch.setattr(source, "load_s3_credentials", loader)
    monkeypatch.setattr(source, "create_s3_client", factory)

    iterator = source.iter_shard_payloads(
        "s3://bucket/input/",
        "user/",
        input_format="tokenplan",
        endpoint_url="https://example.invalid",
        region_name="us-east-1",
        input_credentials_path="/shared/private/credentials.json",
    )
    loader.assert_not_called()
    factory.assert_not_called()
    assert list(iterator) == []
    loader.assert_called_once_with("/shared/private/credentials.json")
    factory.assert_called_once_with(
        "https://example.invalid",
        "us-east-1",
        max_pool_connections=16,
        credentials=credentials,
    )
    client.get_paginator.return_value.paginate.assert_called_once_with(
        Bucket="bucket",
        Prefix="input/user/",
    )


def test_explicit_credential_load_failure_propagates_without_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failure = CredentialFileError("credential file could not be opened safely")
    loader = Mock(side_effect=failure)
    factory = Mock(side_effect=AssertionError("no fallback client may be created"))
    monkeypatch.setattr(source, "load_s3_credentials", loader)
    monkeypatch.setattr(source, "create_s3_client", factory)

    with pytest.raises(CredentialFileError) as caught:
        list(
            source.iter_shard_payloads(
                "s3://bucket/input/",
                "user/",
                input_format="tokenplan",
                endpoint_url=None,
                region_name="us-east-1",
                input_credentials_path="/shared/private/missing.json",
            )
        )
    assert caught.value is failure
    loader.assert_called_once_with("/shared/private/missing.json")
    factory.assert_not_called()


def test_read_partition_forwards_only_credential_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trajfoundry.spark import job

    reader = Mock(return_value=iter(()))
    monkeypatch.setattr(job, "iter_shard_payloads", reader)
    assert (
        list(
            job._read_partition(
                ["user/"],
                input_uri="s3://bucket/input/",
                input_format="tokenplan",
                endpoint_url=None,
                region_name="us-east-1",
                input_credentials_path="/shared/private/credentials.json",
            )
        )
        == []
    )
    reader.assert_called_once_with(
        "s3://bucket/input/",
        "user/",
        input_format="tokenplan",
        endpoint_url=None,
        region_name="us-east-1",
        input_credentials_path="/shared/private/credentials.json",
    )
