"""Date-based Cluster entrypoint; plans unless --execute is explicit."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import subprocess
import sys
import uuid
from datetime import date
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "scheduled_submit_cluster", Path(__file__).with_name("submit_cluster.py")
)
assert _spec and _spec.loader
submit = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(submit)

CONFIG_KEYS = {"submit_args", "input_base_uri", "output_base_uri", "runs_parent"}
MANAGED_FLAGS = {
    "--execute",
    "--control-dir",
    "--input-uri",
    "--output-uri",
    "--output-dir",
    "--allow-existing-output",
    "--help",
}


class ScheduledError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def invalid_arguments(_message):
    # argparse's default error includes raw argv, which may contain a secret.
    raise ScheduledError("invalid_arguments")


def parser():
    result = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    result.error = invalid_arguments
    result.add_argument("--config", type=submit.absolute, required=True)
    result.add_argument("--biz-date", required=True)
    mode = result.add_mutually_exclusive_group()
    mode.add_argument("--plan", action="store_true", help="preview only (default)")
    mode.add_argument("--execute", action="store_true", help="submit and wait")
    return result


def emit(event, **values):
    print(json.dumps({"event": event, **values}, sort_keys=True), flush=True)


def make_schedule(config_path, biz_date):
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", biz_date):
        raise ScheduledError("invalid_biz_date")
    try:
        date.fromisoformat(biz_date)
    except ValueError:
        raise ScheduledError("invalid_biz_date") from None
    config = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(config, dict) or set(config) != CONFIG_KEYS:
        raise ScheduledError("invalid_config_shape")
    fixed = config["submit_args"]
    if not isinstance(fixed, list) or not all(isinstance(v, str) for v in fixed):
        raise ScheduledError("invalid_submit_args")
    for value in fixed:
        flag = value.removeprefix("--job-arg=").split("=", 1)[0]
        if flag == "-h" or (
            flag.startswith("--")
            and any(banned.startswith(flag) for banned in MANAGED_FLAGS)
        ):
            raise ScheduledError("managed_flag_in_config")
    parent = submit.absolute(config["runs_parent"])
    submit.safe_directory(parent)
    if not parent.is_dir():
        raise ScheduledError("runs_parent_missing")
    run_dir = parent / f"tokenplan-{biz_date}-{uuid.uuid4().hex}"
    # make_plan owns source-path/import/template/credential-metadata validation.
    # URI parsing is imported only after selecting this checkout's source tree.
    candidates = [str(v) for v in fixed]
    command = candidates + [
        "--control-dir",
        str(run_dir / "control"),
        "--input-uri",
        config["input_base_uri"].rstrip("/") + f"/dt={biz_date}/",
        "--output-uri",
        config["output_base_uri"].rstrip("/") + f"/dt={biz_date}/",
    ]
    arg_parser = submit.parser()
    arg_parser.allow_abbrev = False
    arg_parser.error = invalid_arguments
    args = arg_parser.parse_args(command)
    if args.input_format == "boyue":
        # Boyue archives use YYYY-MM-DD directly, while normalized output and
        # the other supported sources use dt=YYYY-MM-DD. Keep both submit
        # entrypoints and the Driver's storage checks on the same input URI.
        args.input_uri = config["input_base_uri"].rstrip("/") + f"/{biz_date}/"
        command[command.index("--input-uri") + 1] = args.input_uri
    plan = submit.make_plan(args)
    if args.input_credentials_file is None or args.output_credentials_file is None:
        raise ScheduledError("explicit_credentials_paths_required")
    from trajfoundry.pipeline import NORMALIZER_REVISION

    public = {
        "biz_date": biz_date,
        "normalizer_revision": NORMALIZER_REVISION,
        "input_uri": args.input_uri,
        "output_uri": plan["output_uri"],
        "run_dir": str(run_dir),
        "control_dir": str(args.control_dir),
        "project_root": str(args.project_root),
        "profile": plan["profile"],
        "executor_instances": plan["executor_instances"],
        "total_executor_cores": plan["total_executor_cores"],
        "total_executor_memory_gib": plan["total_executor_memory_gib"],
        "driver_cores": args.driver_cores,
        "driver_memory_gib": args.driver_memory_gib,
        "driver_overhead_gib": args.driver_overhead_gib,
        "submitter_uid": os.geteuid(),
        "pod_uid": args.run_as_user,
    }
    return args, run_dir, command, public


def check_storage(args):
    """Read-only checks, before any run directory or Cluster submission exists."""
    from trajfoundry.credentials import load_s3_credentials
    from trajfoundry.s3 import create_s3_client, parse_s3_uri

    for uri, credential_path, is_output in (
        (args.output_uri, args.output_credentials_file, True),
        (args.input_uri, args.input_credentials_file, False),
    ):
        location = parse_s3_uri(uri)
        client = create_s3_client(
            args.endpoint_url,
            args.region_name,
            credentials=load_s3_credentials(credential_path),
            max_pool_connections=1,
        )
        try:
            if is_output:
                page = client.list_objects_v2(
                    Bucket=location.bucket, Prefix=location.prefix, MaxKeys=1
                )
                if page.get("Contents") or page.get("KeyCount", 0):
                    raise ScheduledError("output_not_empty")
            else:
                pages = client.get_paginator("list_objects_v2").paginate(
                    Bucket=location.bucket,
                    Prefix=location.prefix,
                    PaginationConfig={"PageSize": 1000},
                )
                if not any(
                    obj.get("Size", 0) > 0 and not obj["Key"].endswith("/")
                    for page in pages
                    for obj in page.get("Contents", ())
                ):
                    raise ScheduledError("input_empty")
        finally:
            client.close()


def relay_progress(path, offset):
    """Only relay existing structured progress, never arbitrary Driver errors."""
    if not path.is_file():
        return offset
    if path.stat().st_size < offset:
        offset = 0
    with path.open("rb") as handle:
        handle.seek(offset)
        for _ in range(4096):
            before = handle.tell()
            line = handle.readline(65536)
            if not line or (not line.endswith(b"\n") and len(line) < 65536):
                return before
            marker = b"TF_SPARK_PROGRESS "
            if marker in line:
                try:
                    value = json.loads(line.partition(marker)[2])
                except (ValueError, UnicodeError):
                    continue
                if isinstance(value, dict):
                    print(
                        "TF_SPARK_PROGRESS " + json.dumps(value, sort_keys=True),
                        flush=True,
                    )
        return handle.tell()


def launch_environment(args):
    environment = submit.child_environment()
    environment.update(
        SPARK_HOME=str(args.spark_submit.parent.parent),
        PYTHONPATH=str(args.project_root / "src"),
        PYTHONUNBUFFERED="1",
    )
    return environment


def execute(args, run_dir, command):
    run_dir.mkdir(mode=0o700, parents=False, exist_ok=False)
    with (run_dir / "arguments.json").open("x", encoding="utf-8") as handle:
        json.dump(command + ["--execute"], handle)
        handle.write("\n")
    emit("submitted_launcher", run_dir=str(run_dir), control_dir=str(args.control_dir))
    offset = 0
    with (run_dir / "launcher.log").open("x", encoding="utf-8") as log:
        process = subprocess.Popen(
            [
                sys.executable,
                str(Path(__file__).with_name("submit_cluster.py")),
                *command,
                "--execute",
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
            cwd=args.project_root,
            env=launch_environment(args),
        )
        while True:
            try:
                child_exit = process.wait(timeout=60)
                break
            except subprocess.TimeoutExpired:
                offset = relay_progress(args.control_dir / "driver.log", offset)
                emit("waiting", run_dir=str(run_dir), control_dir=str(args.control_dir))
    relay_progress(args.control_dir / "driver.log", offset)
    final_path = args.control_dir / "final.json"
    if not final_path.is_file():
        raise ScheduledError("completion_report_missing")
    final = json.loads(final_path.read_text(encoding="utf-8"))
    if (
        child_exit != 0
        or final.get("passed") is not True
        or type(final.get("exit_code")) is not int
        or final["exit_code"] != 0
    ):
        raise ScheduledError("cluster_execution_failed")
    emit("succeeded", run_dir=str(run_dir), final_report=str(final_path))


def main(argv=None):
    stage = "arguments"
    try:
        options = parser().parse_args(argv)
        stage = "plan"
        args, run_dir, command, public = make_schedule(options.config, options.biz_date)
        emit("plan", execute=options.execute, **public)
        if not options.execute:
            return 0
        stage = "identity"
        if os.geteuid() != args.run_as_user:
            raise ScheduledError("submitter_pod_uid_mismatch")
        stage = "storage_preflight"
        check_storage(args)
        stage = "execute_and_validate"
        execute(args, run_dir, command)
        return 0
    except Exception as error:  # noqa: BLE001 - never expose SDK/config values
        emit(
            "failed",
            stage=stage,
            error_code=error.code
            if isinstance(error, ScheduledError)
            else "operation_failed",
            error_type=type(error).__name__,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
