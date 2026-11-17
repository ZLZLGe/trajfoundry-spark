#!/usr/bin/env python3
"""Serial, resumable Boyue/DeepInfra date backfill; plans unless --execute."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import submit_cluster

from trajfoundry.credentials import load_s3_credentials
from trajfoundry.s3 import create_s3_client, parse_s3_uri
from trajfoundry.spark.backfill import (
    SOURCES,
    atomic_json,
    date_uris,
    discover_dates,
    frozen_yesterday,
    readiness,
    scan_inventory,
    shared_lock,
)
from trajfoundry.spark.job import _source_package_hash


def emit(event, **fields):
    print(json.dumps({"event": event, **fields}, sort_keys=True), flush=True)


def load(path, default=None):
    return json.loads(path.read_text()) if path.exists() else default


def require_no_running_dates(state_root):
    for path in state_root.glob("*.state.json"):
        if load(path, {}).get("status") == "running":
            raise RuntimeError("unreconciled_running_driver")


def submission_has_finished(args, control_dir, final):
    """A failed local submitter is not evidence that its remote Driver stopped."""
    if final.get("cleanup_required") is False:
        return True  # The submitter failed before attempting submission.
    plan = load(control_dir / "plan.json", {})
    if not plan.get("driver_pod") or not plan.get("run_id"):
        return False
    from validate_cluster import fetch_driver_pod

    try:
        pod = fetch_driver_pod(args, plan["driver_pod"])
    except Exception:  # noqa: BLE001 - preserve the running state on uncertainty
        return False
    metadata = pod.get("metadata", {})
    labels = metadata.get("labels", {})
    return (
        metadata.get("name") == plan["driver_pod"]
        and metadata.get("namespace") == args.namespace
        and bool(metadata.get("uid"))
        and labels.get("spark-role") == "driver"
        and labels.get("trajfoundry-run-id") == plan["run_id"]
        and pod.get("status", {}).get("phase") in {"Succeeded", "Failed"}
    )


def make_command(fixed, state_root, source, day, output_uri, inventory, owner, resume):
    run_dir = state_root / "runs" / f"{source}-{day}-{uuid.uuid4().hex}"
    command = list(fixed)
    input_uri, _ = date_uris(source, day)
    command += [
        "--input-format",
        source,
        "--control-dir",
        str(run_dir / "control"),
        "--input-uri",
        input_uri,
        "--output-uri",
        output_uri,
        "--job-arg=--input-inventory-sha256",
        "--job-arg=" + inventory["selected_fingerprint"],
        "--job-arg=--output-owner",
        "--job-arg=" + owner,
    ]
    if resume:
        command.append("--job-arg=--resume-owned-output")
    return run_dir, command


def run_date(client, options, fixed, source, day):
    stem = f"{source}-{day}"
    path = options.state_root / f"{stem}.state.json"
    observation = options.state_root / f"{stem}.observation.json"
    state = load(path, {})
    if state.get("status") == "running":
        # A dead supervisor does not imply a dead Spark Driver.
        raise RuntimeError("unreconciled_running_driver")
    input_uri, output_uri = date_uris(source, day)
    if options.output_base:
        output_uri = options.output_base.rstrip("/") + f"/dt={day}/"
    with shared_lock(options.state_root / f"{stem}.lock"):
        # Historical completed dates are sealed; explicit --recheck refreshes
        # older inventory while the ordinary lookback is limited to seven days.
        if state.get("status") == "succeeded" and not options.recheck:
            from datetime import date, timedelta

            if (
                day
                < (date.fromisoformat(options.cutoff) - timedelta(days=6)).isoformat()
            ):
                emit("sealed", source=source, date=day)
                return True
        inventory = scan_inventory(client, input_uri, source)
        previous = load(observation)
        ready, reason = readiness(inventory, previous, day=day)
        if (
            previous is None
            or previous["full_fingerprint"] != inventory["full_fingerprint"]
        ):
            atomic_json(observation, inventory)
        emit(
            "inventory",
            source=source,
            date=day,
            ready=ready,
            readiness=reason,
            selected_objects=inventory["selected_objects"],
            selected_bytes=inventory["selected_bytes"],
            parquet_objects=inventory["parquet_objects"],
            input_uri=input_uri,
            output_uri=output_uri,
        )
        if not ready or options.observe_only:
            return False
        if inventory["selected_objects"] == 0:
            atomic_json(
                path,
                {
                    "status": "empty",
                    "source": source,
                    "date": day,
                    "inventory": inventory,
                    "finished_at": time.time(),
                },
            )
            emit("no_selected_inputs", source=source, date=day)
            return True
        identity = {
            "input_uri": input_uri,
            "output_uri": output_uri,
            "input_fingerprint": inventory["selected_fingerprint"],
            "source_hash": _source_package_hash(),
            "submit_args": fixed,
        }
        owner = hashlib.sha256(
            json.dumps(identity, sort_keys=True).encode()
        ).hexdigest()
        if state.get("owner") and state["owner"] != owner:
            raise RuntimeError("input_or_code_changed_existing_output_preserved")
        if state.get("status") == "succeeded":
            emit("unchanged", source=source, date=day)
            return True
        run_dir, command = make_command(
            fixed,
            options.state_root,
            source,
            day,
            output_uri,
            inventory,
            owner,
            bool(state.get("owner")),
        )
        args = submit_cluster.parser().parse_args(command)
        plan = submit_cluster.make_plan(args)
        emit(
            "plan",
            source=source,
            date=day,
            executors=plan["executor_instances"],
            cores=plan["total_executor_cores"],
            memory_gib=plan["total_executor_memory_gib"],
            execute=options.execute,
        )
        if not options.execute:
            return False
        # A first attempt requires an empty date. Owner checks in the Driver
        # protect retries, including unknown objects and previously completed manifests.
        location = parse_s3_uri(output_uri)
        if not state.get("owner") and client.list_objects_v2(
            Bucket=location.bucket, Prefix=location.prefix, MaxKeys=1
        ).get("Contents"):
            raise RuntimeError("unowned_nonempty_output_preserved")
        run_dir.mkdir(mode=0o700)
        state = {
            "status": "running",
            "owner": owner,
            "source": source,
            "date": day,
            "identity": identity,
            "inventory": inventory,
            "run_dir": str(run_dir),
            "attempt": state.get("attempt", 0) + 1,
            "started_at": time.time(),
        }
        atomic_json(path, state)
        with (run_dir / "launcher.log").open("x") as log:
            process = subprocess.Popen(
                [
                    sys.executable,
                    str(ROOT / "spark/submit_cluster.py"),
                    *command,
                    "--execute",
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
                cwd=ROOT,
                env={
                    **submit_cluster.child_environment(),
                    "PYTHONPATH": str(ROOT / "src"),
                    "PYTHONUNBUFFERED": "1",
                },
            )
            while True:
                try:
                    exit_code = process.wait(timeout=60)
                    break
                except subprocess.TimeoutExpired:
                    emit(
                        "running",
                        source=source,
                        date=day,
                        elapsed_seconds=int(time.time() - state["started_at"]),
                        run_dir=str(run_dir),
                    )
        final = load(run_dir / "control/final.json", {})
        passed = exit_code == 0 and final.get("passed") is True
        if not passed and not submission_has_finished(args, run_dir / "control", final):
            state.update(final=final, submission_exit_code=exit_code)
            atomic_json(path, state)
            raise RuntimeError("unreconciled_running_driver")
        state.update(
            status="succeeded" if passed else "failed",
            finished_at=time.time(),
            final=final,
        )
        atomic_json(path, state)
        emit(state["status"], source=source, date=day, run_dir=str(run_dir))
        return passed


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config", type=Path, required=True, help="nonsecret submit_args JSON"
    )
    parser.add_argument(
        "--state-root",
        type=Path,
        required=True,
        help="private shared directory, reused across all invocations",
    )
    parser.add_argument("--source", action="append", choices=tuple(SOURCES))
    parser.add_argument("--date", action="append")
    parser.add_argument("--cutoff", default=frozen_yesterday())
    parser.add_argument(
        "--output-base", help="fresh acceptance prefix, requires one source"
    )
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--observe-only", action="store_true")
    parser.add_argument(
        "--recheck", action="store_true", help="rescan sealed historical dates"
    )
    options = parser.parse_args(argv)
    if options.cutoff > frozen_yesterday():
        parser.error("cutoff cannot include today or future dates")
    sources = options.source or list(SOURCES)
    if options.output_base and len(sources) != 1:
        parser.error("output-base requires one source")
    if not options.state_root.is_absolute() or options.state_root.is_symlink():
        parser.error("state-root must be an absolute shared directory")
    options.state_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    (options.state_root / "runs").mkdir(mode=0o700, exist_ok=True)
    config = json.loads(options.config.read_text())
    fixed = config["submit_args"]
    forbidden = {
        "--input-uri",
        "--output-uri",
        "--input-format",
        "--control-dir",
        "--allow-existing-output",
        "--output-owner",
        "--resume-owned-output",
        "--input-inventory-sha256",
        "--execute",
    }
    if any(
        value.removeprefix("--job-arg=").split("=", 1)[0] in forbidden
        for value in fixed
    ):
        raise ValueError("managed argument in configuration")

    def flag(name):
        return fixed[fixed.index(name) + 1]

    client = create_s3_client(
        flag("--endpoint-url"),
        flag("--region-name"),
        credentials=load_s3_credentials(flag("--input-credentials-file")),
        max_pool_connections=32,
    )
    try:
        with shared_lock(options.state_root / "global.lock"):
            # Failed attempts are independent. An unreconciled running Driver
            # blocks new submissions across both sources, even after a crash.
            require_no_running_dates(options.state_root)
            work = [
                (source, day)
                for source in sources
                for day in (
                    options.date or discover_dates(client, source, options.cutoff)
                )
                if day <= options.cutoff
            ]
            atomic_json(
                options.state_root / "plan.json",
                {"cutoff": options.cutoff, "dates": work, "created_at": time.time()},
            )
            success = failed = pending = 0
            for source, day in work:
                try:
                    done = run_date(client, options, fixed, source, day)
                    success += int(done)
                    if not done:
                        state = load(
                            options.state_root / f"{source}-{day}.state.json", {}
                        )
                        if state.get("status") == "failed":
                            failed += 1
                        else:
                            pending += 1
                except Exception as error:  # noqa: BLE001 - one failed date must not hide other dates
                    failed += 1
                    emit(
                        "date_blocked",
                        source=source,
                        date=day,
                        error_type=type(error).__name__,
                        error_code=str(error)
                        if type(error) is RuntimeError
                        and re.fullmatch(r"[a-z_]+", str(error))
                        else "operation_failed",
                    )
                # Exceptions after writing a running state must halt the whole
                # controller, including launch failures and local I/O failures.
                require_no_running_dates(options.state_root)
            report = {
                "dates": len(work),
                "succeeded": success,
                "pending": pending,
                "blocked": failed,
            }
            atomic_json(options.state_root / "summary.json", report)
            emit("finished", **report)
            return int(bool(failed or pending))
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
