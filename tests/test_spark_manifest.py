import json

import pytest

from trajfoundry.spark.manifest import DiskManifestAccumulator
from trajfoundry.spark.output import ManifestAccumulator, SparkObjectDescription


def test_streamed_manifest_matches_existing_contract(tmp_path):
    kwargs = {
        "input_root": "s3://fake/input/",
        "input_format": "tokenplan",
        "config_hash": "abc",
    }
    old = ManifestAccumulator(**kwargs)
    with DiskManifestAccumulator(tmp_path / "meta.db", **kwargs) as disk:
        for name in ("z.jsonl", "a.jsonl", "lineage.jsonl"):
            description = SparkObjectDescription(name, "a" * 64, 10)
            old.add_object(description)
            disk.add_object(description)
        old.stats.input_files = disk.stats.input_files = 42
        old.stats.reason_counts = disk.stats.reason_counts = {"synthetic": 2}
        disk.write_manifest(tmp_path / "manifest.json", created_at="test")
        assert json.loads((tmp_path / "manifest.json").read_bytes()) == json.loads(
            old.manifest_bytes(created_at="test")
        )
        assert disk.object_count == 3
        assert not hasattr(disk, "_objects")


@pytest.mark.parametrize("same_path", [True, False])
def test_disk_inventory_rejects_duplicate_identity_and_filename(tmp_path, same_path):
    with DiskManifestAccumulator(
        tmp_path / "meta.db",
        input_root="s3://fake/",
        input_format="tokenplan",
        config_hash="x",
    ) as disk:
        disk.add_object(SparkObjectDescription("a.jsonl", "a" * 64, 10), "id1")
        with pytest.raises(RuntimeError, match="duplicate trajectory"):
            disk.add_object(
                SparkObjectDescription(
                    "a.jsonl" if same_path else "b.jsonl", "a" * 64, 10
                ),
                "id2" if same_path else "id1",
            )
