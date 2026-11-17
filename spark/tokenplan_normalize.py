#!/usr/bin/env python3
"""Spark Client/Cluster entrypoint for TokenPlan normalization."""

from __future__ import annotations

import argparse
import json
import logging

from trajfoundry.spark.config import SparkNormalizeConfig
from trajfoundry.spark.job import run_spark_job


def positive_integer_or_unlimited(value: str) -> int | None:
    """Parse a positive object limit, or the explicit unlimited sentinel."""

    if value.lower() == "unlimited":
        return None
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "must be a positive integer or 'unlimited'"
        ) from error
    if parsed <= 0:
        raise argparse.ArgumentTypeError(
            "must be a positive integer or 'unlimited'"
        )
    return parsed


def argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-uri", required=True)
    parser.add_argument(
        "--output-uri",
        required=True,
        help="fresh S3 prefix or absolute shared directory (also file:///path)",
    )
    parser.add_argument("--endpoint-url")
    parser.add_argument("--region-name", default="us-east-1")
    parser.add_argument("--output-owner")
    parser.add_argument("--resume-owned-output", action="store_true")
    parser.add_argument("--input-inventory-sha256")
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
    parser.add_argument("--input-format", choices=("tokenplan", "boyue", "deepinfra"), default="tokenplan")
    parser.add_argument("--shuffle-partitions", type=int)
    parser.add_argument("--read-parallelism", type=int)
    parser.add_argument(
        "--max-output-objects",
        type=positive_integer_or_unlimited,
        default=None,
        metavar="N|unlimited",
        help="Driver-side total output object limit; default is unlimited",
    )
    parser.add_argument("--max-partition-objects", type=int, default=20_000_000)
    parser.add_argument("--scope-memory-mib", type=int, default=256)
    parser.add_argument("--max-scope-snapshots", type=int, default=10_000)
    parser.add_argument("--max-scope-summaries", type=int, default=1_000_000)
    parser.add_argument("--max-scope-summary-mib", type=int, default=512)
    parser.add_argument("--max-input-object-mib", type=int, default=2048)
    parser.add_argument("--max-routing-mib", type=int, default=4096)
    parser.add_argument("--max-routing-snapshots", type=int, default=2_000_000)
    parser.add_argument("--max-root-mib", type=int, default=2048)
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
        default=1_000_000,
        help="fail closed if automatic discovery returns more prefixes than this",
    )
    return parser


def build_config(argv=None) -> SparkNormalizeConfig:
    """Parse normalizer arguments and build the validated Spark config.

    Keeping argument parsing separate lets a platform-specific launcher (for
    example the Flamingo SDK launcher used from Jupyter) create the Spark
    session first and then inject it into :func:`run_spark_job`.  The ordinary
    ``spark-submit`` entrypoint remains unchanged.
    """

    args = argument_parser().parse_args(argv)
    return SparkNormalizeConfig(
        input_uri=args.input_uri,
        output_uri=args.output_uri,
        input_format=args.input_format,
        output_owner=args.output_owner,
        resume_owned_output=args.resume_owned_output,
        input_inventory_sha256=args.input_inventory_sha256,
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
        max_scope_summaries=args.max_scope_summaries,
        max_scope_summary_bytes=args.max_scope_summary_mib * 1024 * 1024,
        max_input_object_bytes=args.max_input_object_mib * 1024 * 1024,
        max_routing_bytes=args.max_routing_mib * 1024 * 1024,
        max_routing_snapshots=args.max_routing_snapshots,
        max_root_bytes=args.max_root_mib * 1024 * 1024,
    )


def main(argv=None, *, spark=None) -> dict:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    config = build_config(argv)
    if spark is None:
        # Preserve the original CLI call shape for existing cluster/client
        # callers and tests that replace run_spark_job with a one-argument
        # adapter.
        result = run_spark_job(config)
    else:
        result = run_spark_job(config, spark=spark)
    print(json.dumps(result, sort_keys=True), flush=True)
    return result


if __name__ == "__main__":
    main()
