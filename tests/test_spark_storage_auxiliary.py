"""Auxiliary retention is an explicit narrow exception to empty S3 outputs."""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from trajfoundry.spark import storage
from trajfoundry.spark.storage import (
    S3OutputStore,
    create_output_store,
    is_retained_s3_auxiliary_name,
)

from .test_spark_storage import _S3

RUN_ID = "a" * 32
KNOWN_AUXILIARIES = [
    f".trajfoundry-access-probe-{RUN_ID}",
    f"._lineage_part_{RUN_ID}_000001.ndjson",
    f"._diagnostic_part_{RUN_ID}_000001.ndjson",
    f"._objects_part_{RUN_ID}_000001.ndjson",
    f"._objects_part_{RUN_ID}_1000000.ndjson",
]


@pytest.mark.parametrize("name", KNOWN_AUXILIARIES)
def test_known_flat_auxiliary_names_have_exact_whitelist(name):
    assert is_retained_s3_auxiliary_name(name)


@pytest.mark.parametrize(
    "name",
    [
        "manifest.json",
        "lineage.jsonl",
        "input_diagnostics.ndjson",
        "session_sub_0.jsonl",
        "unknown.bin",
        "._objects_part_000001.ndjson",
        f"._other_part_{RUN_ID}_000001.ndjson",
        f"._objects_part_{RUN_ID}_00001.ndjson",
        f"._objects_part_{RUN_ID}_000001.ndjson.bak",
        f"._objects_part_{RUN_ID}_000001.ndjson\n",
        f"._objects_part_{RUN_ID.upper()}_000001.ndjson",
        ".trajfoundry-access-probe-short",
        f".trajfoundry-access-probe-{RUN_ID}.json",
        f".trajfoundry-access-probe-{RUN_ID.upper()}",
        f"subdir/.trajfoundry-access-probe-{RUN_ID}",
        f"../.trajfoundry-access-probe-{RUN_ID}",
        f"./.trajfoundry-access-probe-{RUN_ID}",
        f"%2F.trajfoundry-access-probe-{RUN_ID}",
        f"\\.trajfoundry-access-probe-{RUN_ID}",
        "",
        None,
        1,
    ],
)
def test_unknown_business_and_nested_names_are_not_auxiliary(name):
    assert not is_retained_s3_auxiliary_name(name)


@pytest.mark.parametrize("retain", [False, True])
def test_empty_s3_output_works_with_either_mode(retain):
    store = S3OutputStore("s3://bucket/output/", _S3(), retain_auxiliary=retain)
    store.initialize_run(RUN_ID)


@pytest.mark.parametrize("name", KNOWN_AUXILIARIES)
def test_default_mode_still_rejects_known_auxiliary_objects(name):
    client = _S3()
    client.objects[f"output/{name}"] = b"keep"
    with pytest.raises(ValueError, match="not empty"):
        S3OutputStore("s3://bucket/output/", client).initialize_run(RUN_ID)
    assert client.objects == {f"output/{name}": b"keep"}
    assert client.puts == 0


def test_explicit_retention_permits_only_known_auxiliaries_without_writes_or_deletes():
    client = _S3()
    client.objects = {f"output/{name}": b"keep" for name in KNOWN_AUXILIARIES}
    before = dict(client.objects)
    client.delete_object = Mock(side_effect=AssertionError("deletion is forbidden"))
    store = create_output_store(
        "s3://bucket/output/",
        client_factory=lambda *_: client,
        retain_s3_auxiliary=True,
    )
    assert store.retain_auxiliary is True
    store.initialize_run(RUN_ID)
    store.finish_run()
    assert client.objects == before
    assert client.puts == 0
    client.delete_object.assert_not_called()


@pytest.mark.parametrize(
    "name",
    [
        "manifest.json",
        "lineage.jsonl",
        "input_diagnostics.ndjson",
        "session_sub_0.jsonl",
        "unknown.bin",
        "._objects_part_000001.ndjson",
        f"subdir/.trajfoundry-access-probe-{RUN_ID}",
        f"../.trajfoundry-access-probe-{RUN_ID}",
    ],
)
def test_retention_cannot_reuse_business_data_or_unknown_files(name):
    client = _S3()
    client.objects = {
        f"output/{KNOWN_AUXILIARIES[0]}": b"auxiliary",
        f"output/{name}": b"original",
    }
    before = dict(client.objects)
    with pytest.raises(ValueError, match="not empty"):
        S3OutputStore(
            "s3://bucket/output/", client, retain_auxiliary=True
        ).initialize_run(RUN_ID)
    assert client.objects == before
    assert client.puts == 0


def test_auxiliary_admission_streams_pages_and_never_skips_a_later_business_file():
    class ManyPages(_S3):
        pages_consumed = 0

        def paginate(self, **_kwargs):
            for index in range(100):
                self.pages_consumed += 1
                yield {
                    "Contents": [{"Key": f"output/._objects_part_{RUN_ID}_{index:06d}.ndjson"}]
                }
            self.pages_consumed += 1
            yield {"Contents": [{"Key": "output/manifest.json"}]}
            raise AssertionError("should fail before reading any more pages")

    client = ManyPages()
    with pytest.raises(ValueError, match="not empty"):
        S3OutputStore(
            "s3://bucket/output/", client, retain_auxiliary=True
        ).initialize_run(RUN_ID)
    assert client.pages_consumed == 101
    assert client.puts == 0


@pytest.mark.parametrize(
    "page",
    [
        None,
        [],
        {"Contents": None},
        {"Contents": {}},
        {"Contents": [None]},
        {"Contents": [{}]},
        {"Contents": [{"Key": None}]},
        {"Contents": [{"Key": f"different-prefix/{KNOWN_AUXILIARIES[0]}"}]},
        {"CommonPrefixes": [{"Prefix": "output/subdirectory/"}]},
    ],
)
def test_malformed_or_out_of_prefix_listing_is_refused(page):
    client = _S3()
    client.paginate = Mock(return_value=iter([page]))
    with pytest.raises((TypeError, ValueError), match="listing|not empty"):
        S3OutputStore(
            "s3://bucket/output/", client, retain_auxiliary=True
        ).initialize_run(RUN_ID)
    assert client.puts == 0


def test_retention_remains_s3_only_before_loading_credentials_or_creating_files(
    monkeypatch, tmp_path
):
    loader = Mock(side_effect=AssertionError("no credential loading"))
    factory = Mock(side_effect=AssertionError("no S3 client"))
    monkeypatch.setattr(storage, "load_s3_credentials", loader)
    output = tmp_path / "output"
    with pytest.raises(ValueError, match="only for S3"):
        create_output_store(
            str(output),
            client_factory=factory,
            output_credentials_path="/unused/credentials.json",
            retain_s3_auxiliary=True,
        )
    assert not output.exists()
    loader.assert_not_called()
    factory.assert_not_called()


@pytest.mark.parametrize("value", ["false", "true", 0, 1, None])
def test_retention_mode_requires_a_boolean(value):
    with pytest.raises(TypeError, match="boolean"):
        create_output_store("s3://bucket/output/", retain_s3_auxiliary=value)
    with pytest.raises(TypeError, match="boolean"):
        S3OutputStore("s3://bucket/output/", _S3(), retain_auxiliary=value)
