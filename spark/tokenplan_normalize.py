#!/usr/bin/env python3
"""DolphinScheduler/Spark Cluster entrypoint for TokenPlan normalization."""

from __future__ import annotations

import argparse
import json

from trajfoundry.spark.config import SparkNormalizeConfig
from trajfoundry.spark.job import run_spark_job


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-uri", required=True)
    parser.add_argument("--output-uri", required=True)
    parser.add_argument("--endpoint-url")
    parser.add_argument("--region-name", default="us-east-1")
    parser.add_argument("--input-format", choices=("tokenplan",), default="tokenplan")
    parser.add_argument(
        "--prefix-shard",
        action="append",
        required=True,
        help="relative input prefix shard; repeat for all shards",
    )
    args = parser.parse_args()
    config = SparkNormalizeConfig(
        input_uri=args.input_uri,
        output_uri=args.output_uri,
        input_format=args.input_format,
        endpoint_url=args.endpoint_url,
        region_name=args.region_name,
        prefix_shards=tuple(args.prefix_shard),
    )
    result = run_spark_job(config)
    print(json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
