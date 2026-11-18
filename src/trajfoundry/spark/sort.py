"""Spill capture rows without combining them into oversized pickle frames."""

from __future__ import annotations


def sort_partition(rows, *, memory_limit):
    from pyspark.serializers import (
        BatchedSerializer,
        CompressedSerializer,
        CPickleSerializer,
    )
    from pyspark.shuffle import ExternalSorter

    sorter = ExternalSorter(memory_limit)
    # Spark 3.5 ignores ExternalSorter's serializer argument and always uses
    # adaptive batches. A run of small rows can grow the next spill batch past
    # the signed 32-bit frame limit when larger captures follow. Keep each
    # capture in its own compressed frame, preserving external merge sorting.
    sorter.serializer = BatchedSerializer(CompressedSerializer(CPickleSerializer()), 1)
    return iter(sorter.sorted(rows, key=lambda row: row[0]))


def repartition_and_sort(rdd, *, num_partitions, partition_func):
    memory_limit = rdd._memory_limit() * 0.9
    return rdd.partitionBy(num_partitions, partition_func).mapPartitions(
        lambda rows: sort_partition(rows, memory_limit=memory_limit),
        preservesPartitioning=True,
    )
