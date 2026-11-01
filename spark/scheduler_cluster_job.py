"""Pod-side S3 checks and authenticated completion evidence for scheduled jobs."""

from __future__ import annotations

import argparse
import json
import os
import re
import socket
import sys
from pathlib import Path

IDENTITY_FLAGS = {"--tf-run-id", "--tf-driver-pod", "--tf-pod-uid"}


class DriverJobError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def invalid_arguments(*_args, **_kwargs):
    raise DriverJobError("invalid_arguments")


def quiet_parser(parser):
    parser.allow_abbrev = False
    parser.error = invalid_arguments
    parser.exit = invalid_arguments
    parser.print_help = lambda *_args, **_kwargs: None
    return parser


def identity_arguments(argv):
    for flag in IDENTITY_FLAGS:
        if sum(value.split("=", 1)[0] == flag for value in argv) != 1:
            raise DriverJobError("identity_arguments_required_once")
    parser = quiet_parser(argparse.ArgumentParser(add_help=False))
    parser.add_argument("--tf-run-id", required=True)
    parser.add_argument("--tf-driver-pod", required=True)
    parser.add_argument("--tf-pod-uid", required=True, type=int)
    identity, remaining = parser.parse_known_args(argv)
    if not re.fullmatch(r"[a-f0-9]{32}", identity.tf_run_id):
        raise DriverJobError("invalid_run_id")
    if not re.fullmatch(
        r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", identity.tf_driver_pod
    ):
        raise DriverJobError("invalid_driver_pod")
    if (
        identity.tf_pod_uid <= 0
        or identity.tf_pod_uid != os.geteuid()
        or socket.gethostname() != identity.tf_driver_pod
        or os.environ.get("TF_CLUSTER_DRIVER_POD") != identity.tf_driver_pod
        or os.environ.get("TF_CLUSTER_EXPECTED_UID") != str(identity.tf_pod_uid)
    ):
        raise DriverJobError("driver_identity_mismatch")
    return identity, remaining


def load_support():
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import tokenplan_normalize
    from run_scheduled_tokenplan import check_storage
    from validate_cluster import s3_manifest_sha256

    return tokenplan_normalize, check_storage, s3_manifest_sha256


def main(argv=None):
    stage = "identity"
    try:
        identity, remaining = identity_arguments(
            list(sys.argv[1:] if argv is None else argv)
        )
        stage = "arguments"
        normalize, check_storage, verify_manifest = load_support()
        args = quiet_parser(normalize.argument_parser()).parse_args(remaining)
        if args.allow_existing_output:
            raise DriverJobError("existing_output_not_allowed")
        if not args.input_credentials_file or not args.output_credentials_file:
            raise DriverJobError("explicit_credentials_paths_required")
        stage = "storage_preflight"
        check_storage(args)
        stage = "normalize"
        result = normalize.main(remaining)
        stage = "verify_manifest"
        from trajfoundry.s3 import parse_s3_uri

        output_uri = parse_s3_uri(args.output_uri).uri
        if not isinstance(result, dict) or result.get("output_backend") != "s3":
            raise DriverJobError("invalid_job_result")
        if parse_s3_uri(result["output_uri"]).uri != output_uri:
            raise DriverJobError("output_identity_mismatch")
        for field in (
            "input_files",
            "trajectories",
            "accepted",
            "quarantined_trajectories",
            "verified_trajectory_objects",
        ):
            if type(result.get(field)) is not int or result[field] < 0:
                raise DriverJobError("invalid_job_counts")
        if (
            result["trajectories"] != result["verified_trajectory_objects"]
            or result["trajectories"]
            != result["accepted"] + result["quarantined_trajectories"]
        ):
            raise DriverJobError("invalid_job_counts")
        expected_hash = result.get("manifest_sha256")
        if not isinstance(expected_hash, str) or not re.fullmatch(
            r"[a-f0-9]{64}", expected_hash
        ):
            raise DriverJobError("invalid_manifest_hash")
        if verify_manifest(args, output_uri) != expected_hash:
            raise DriverJobError("manifest_hash_mismatch")
        completion = {
            "run_id": identity.tf_run_id,
            "driver_pod": identity.tf_driver_pod,
            "uid": os.geteuid(),
            "output_uri": output_uri,
            "manifest_sha256": expected_hash,
            "input_files": result["input_files"],
            "trajectories": result["trajectories"],
            "passed": True,
        }
        print(
            "TF_CLUSTER_COMPLETION " + json.dumps(completion, sort_keys=True),
            flush=True,
        )
        return 0
    except Exception as error:  # noqa: BLE001 - never expose SDK/config values
        failure = {
            "passed": False,
            "stage": stage,
            "error_code": error.code
            if isinstance(error, DriverJobError)
            else "driver_job_failed",
            "error_type": type(error).__name__,
        }
        print("TF_CLUSTER_JOB_ERROR " + json.dumps(failure, sort_keys=True), flush=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
