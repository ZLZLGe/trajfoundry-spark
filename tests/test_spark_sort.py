from __future__ import annotations

import random

import pytest

from trajfoundry.spark.sort import sort_partition


@pytest.mark.parametrize("spill", [False, True])
def test_mixed_capture_sizes_sort_with_bounded_spill_frames(
    monkeypatch, tmp_path, spill
):
    from pyspark import serializers, shuffle

    monkeypatch.setattr(shuffle, "_get_local_dirs", lambda _kind: [str(tmp_path)])
    # Exercise actual spill writing/reading without allocating multi-GB inputs.
    monkeypatch.setattr(
        shuffle.ExternalSorter, "_next_limit", lambda self: 0 if spill else float("inf")
    )
    original_write_int = serializers.write_int
    frame_sizes = []

    def write_int(value, stream):
        if value >= 0:
            assert value < 128 * 1024, "oversized spill serialization frame"
            frame_sizes.append(value)
        original_write_int(value, stream)

    monkeypatch.setattr(serializers, "write_int", write_int)
    rng = random.Random(42)
    rows = [(index, b"x") for index in range(40)] + [
        (index, rng.randbytes(64 * 1024)) for index in range(40, 201)
    ]
    rng.shuffle(rows)
    result = list(sort_partition(iter(rows), memory_limit=1))
    assert result == sorted(rows, key=lambda row: row[0])
    assert bool(frame_sizes) is spill
    assert not any(p.is_file() for p in tmp_path.rglob("*"))
