#!/usr/bin/env python3
"""Spark Client/Cluster entrypoint for TokenPlan normalization."""

from __future__ import annotations

import argparse
import json
import logging

from trajfoundry.spark.config import SparkNormalizeConfig
from trajfoundry.spark.job import run_spark_job


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-uri", required=True)
    parser.add_argument(
        "--output-uri",
        required=True,
        help="fresh S3 prefix or absolute shared directory (also file:///path)",
    )
    parser.add_argument("--endpoint-url")
    parser.add_argument("--region-name", default="us-east-1")
    parser.add_argument(
        "--input-credentials-file",
        help="absolute secure JSON file path readable by each Executor; values stay out of Spark config",
    )
    parser.add_argument(
        "--output-credentials-file",
        help="absolute secure JSON file path readable by Driver and Executors; values stay out of Spark config",
    )
    parser.add_argument(
        "--retain-s3-auxiliary",
        action="store_true",
        help="retain run-scoped auxiliary objects in S3 output; not supported for local output",
    )
    parser.add_argument(
        "--allow-existing-output",
        action="store_true",
        help="explicit retry mode: reuse/replace objects in an existing S3 output prefix",
    )
    parser.add_argument("--input-format", choices=("tokenplan",), default="tokenplan")
    parser.add_argument("--shuffle-partitions", type=int)
    parser.add_argument("--read-parallelism", type=int)
    parser.add_argument("--max-output-objects", type=int, default=2_000_000)
    parser.add_argument("--max-partition-objects", type=int, default=2_000_000)
    parser.add_argument("--scope-memory-mib", type=int, default=64)
    parser.add_argument("--max-scope-snapshots", type=int, default=1000)
    parser.add_argument("--max-input-object-mib", type=int, default=64)
    parser.add_argument("--max-routing-mib", type=int, default=256)
    parser.add_argument("--max-routing-snapshots", type=int, default=250_000)
    parser.add_argument("--max-root-mib", type=int, default=64)
    prefix = parser.add_mutually_exclusive_group(required=True)
    prefix.add_argument(
        "--prefix-shard",
        action="append",
        help="relative input prefix shard; repeat for explicit shards",
    )
    prefix.add_argument(
        "--auto-prefix-shards",
        action="store_true",
        help="discover immediate user prefixes below input-uri on one Spark executor",
    )
    parser.add_argument(
        "--max-prefix-shards",
        type=int,
        default=100_000,
        help="fail closed if automatic discovery returns more prefixes than this",
    )
    args = parser.parse_args()
    config = SparkNormalizeConfig(
        input_uri=args.input_uri,
        output_uri=args.output_uri,
        input_format=args.input_format,
        endpoint_url=args.endpoint_url,
        region_name=args.region_name,
        prefix_shards=tuple(args.prefix_shard or ()),
        auto_prefix_shards=args.auto_prefix_shards,
        max_prefix_shards=args.max_prefix_shards,
        input_credentials_path=args.input_credentials_file,
        output_credentials_path=args.output_credentials_file,
        retain_s3_auxiliary=args.retain_s3_auxiliary,
        allow_existing_output=args.allow_existing_output,
        shuffle_partitions=args.shuffle_partitions,
        default_parallelism=args.read_parallelism,
        max_output_objects=args.max_output_objects,
        max_partition_objects=args.max_partition_objects,
        scope_memory_bytes=args.scope_memory_mib * 1024 * 1024,
        max_scope_snapshots=args.max_scope_snapshots,
        max_input_object_bytes=args.max_input_object_mib * 1024 * 1024,
        max_routing_bytes=args.max_routing_mib * 1024 * 1024,
        max_routing_snapshots=args.max_routing_snapshots,
        max_root_bytes=args.max_root_mib * 1024 * 1024,
    )
    result = run_spark_job(config)
    print(json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
