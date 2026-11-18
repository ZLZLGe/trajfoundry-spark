"""Run a frozen, Boyue-only historical batch with independent output checks."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import signal
import ssl
import subprocess
import sys
import tempfile
import time
from collections import Counter
from contextlib import ExitStack
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import HTTPSHandler, ProxyHandler, Request, build_opener
from zoneinfo import ZoneInfo

from botocore.exceptions import BotoCoreError, ClientError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from trajfoundry.credentials import load_s3_credentials
from trajfoundry.output_contract import parse_trajectory_record
from trajfoundry.s3 import create_s3_client, parse_s3_uri
from trajfoundry.spark.backfill import (
    atomic_json,
    date_uris,
    discover_dates,
    frozen_yesterday,
    shared_lock,
)
from trajfoundry.spark.job import _source_package_hash
from trajfoundry.validation import _Manifest


def load(path, default=None):
    return json.loads(path.read_text()) if path.exists() else default


def configuration(path):
    fixed = load(path)["submit_args"]
    forbidden = {
        "--input-uri",
        "--output-uri",
        "--input-format",
        "--control-dir",
        "--execute",
        "--allow-existing-output",
        "--output-owner",
        "--resume-owned-output",
        "--input-inventory-sha256",
    }
    if not isinstance(fixed, list) or any(not isinstance(x, str) for x in fixed):
        raise ValueError("invalid_submit_arguments")
    if any(x.removeprefix("--job-arg=").split("=", 1)[0] in forbidden for x in fixed):
        raise ValueError("managed_argument_in_configuration")

    def flag(name):
        if fixed.count(name) != 1:
            raise ValueError("missing_or_repeated_configuration_flag")
        return fixed[fixed.index(name) + 1]

    release = Path(flag("--project-root"))
    if (
        not release.is_absolute()
        or not (release / "spark/backfill_lakehouse.py").is_file()
    ):
        raise ValueError("invalid_frozen_release")
    if flag("--profile") != "starter":
        raise ValueError("unexpected_resource_profile")
    if (
        load(release / "release.json", {}).get("source_sha256")
        != _source_package_hash()
    ):
        raise ValueError("normalization_source_changed_since_acceptance")
    job_args = [
        x.removeprefix("--job-arg=") for x in fixed if x.startswith("--job-arg=")
    ]
    for name, value in [("--read-parallelism", "128"), ("--shuffle-partitions", "256")]:
        if job_args.count(name) != 1 or job_args[job_args.index(name) + 1] != value:
            raise ValueError("unexpected_job_parallelism")
    return fixed, flag, release


def require_no_running_states(root):
    if any(load(p, {}).get("status") == "running" for p in root.glob("*.state.json")):
        raise RuntimeError("unreconciled_running_driver")


def require_no_active_drivers(root, flag):
    run_ids = {
        load(p)["run_id"]
        for pattern in ["runs/*/control/plan.json", "acceptance-*/control/plan.json"]
        for p in root.glob(pattern)
    }
    context = ssl.create_default_context(cafile=flag("--ca-cert"))
    context.verify_flags |= ssl.VERIFY_X509_PARTIAL_CHAIN
    opener = build_opener(ProxyHandler({}), HTTPSHandler(context=context))
    url = (
        flag("--master").removeprefix("k8s://").rstrip("/")
        + "/api/v1/namespaces/"
        + flag("--namespace")
        + "/pods?"
        + urlencode({"labelSelector": "spark-role=driver,trajfoundry-run-id"})
    )
    request = Request(
        url,
        headers={
            "Authorization": "Bearer "
            + Path(flag("--oauth-token-file")).read_text().strip()
        },
    )
    with opener.open(request, timeout=30) as response:
        pods = json.load(response)["items"]
    if any(
        p["metadata"].get("labels", {}).get("trajfoundry-run-id") in run_ids
        and p["status"]["phase"] not in {"Succeeded", "Failed"}
        for p in pods
    ):
        raise RuntimeError("unreconciled_running_driver")


def verify_acceptance(root):
    for day in ["2025-12-02", "2026-10-28"]:
        run = root / f"acceptance-boyue-{day}-r2"
        if load(run / "control/final.json", {}).get("passed") is not True:
            raise ValueError("boyue_acceptance_missing")
        if load(run / "acceptance.json", {}).get("source") != "boyue":
            raise ValueError("boyue_independent_acceptance_missing")


def read_progress(run_dir):
    path = Path(run_dir) / "control/driver.log"
    if not path.exists():
        return None
    with path.open("rb") as handle:
        handle.seek(0, os.SEEK_END)
        handle.seek(max(0, handle.tell() - 256 * 1024))
        lines = handle.read().decode(errors="replace").splitlines()
    for line in reversed(lines):
        if line.startswith("TF_SPARK_PROGRESS "):
            try:
                return json.loads(line.split(" ", 1)[1])
            except ValueError:
                continue
    return None


def validate_result(client, state):
    """Hash the full manifest, compare its counters, then check bounded samples."""
    input_uri, output_uri = date_uris("boyue", state["date"])
    identity, final = state["identity"], state["final"]
    job = final.get("job", {})
    if (
        state["source"] != "boyue"
        or identity["input_uri"] != input_uri
        or identity["output_uri"] != output_uri
        or final.get("passed") is not True
        or job.get("output_uri") != output_uri
        or job.get("verified_trajectory_objects") != job.get("trajectories")
    ):
        raise ValueError("invalid_completed_date_identity")
    location = parse_s3_uri(output_uri)
    response = client.get_object(
        Bucket=location.bucket, Key=location.key("manifest.json")
    )
    body = response["Body"]
    try:
        if response.get("Metadata", {}).get("tf-owner") != state["owner"]:
            raise ValueError("output_owner_mismatch")
        digest, size = hashlib.sha256(), 0
        with tempfile.TemporaryFile() as handle:
            while chunk := body.read(8 * 1024 * 1024):
                size += len(chunk)
                if size > 512 * 1024 * 1024:
                    raise ValueError("manifest_exceeds_validation_budget")
                digest.update(chunk)
                handle.write(chunk)
            if digest.hexdigest() != job.get("manifest_sha256"):
                raise ValueError("manifest_hash_mismatch")
            handle.seek(0)
            manifest = json.load(handle)
    finally:
        body.close()
    _Manifest.model_validate(manifest)
    if manifest["input_root"] != input_uri or manifest["input_format"] != "boyue":
        raise ValueError("manifest_input_mismatch")
    comparisons = {
        "record_count": "trajectories",
        "accepted_trajectory_count": "accepted",
        "quarantined_trajectory_count": "quarantined_trajectories",
        "parse_failure_count": "parse_failures",
    }
    if any(manifest[a] != job.get(b) for a, b in comparisons.items()):
        raise ValueError("manifest_count_mismatch")
    if manifest["counts"]["input_files"] != job.get("input_files"):
        raise ValueError("manifest_input_count_mismatch")
    if manifest["token_estimator"] != "unicode-word-v1":
        raise ValueError("token_estimator_changed")
    trajectory_files = [
        x
        for x in manifest["files"]
        if x["path"].endswith(".jsonl") and x["path"] != "lineage.jsonl"
    ]
    if len(trajectory_files) != job["trajectories"]:
        raise ValueError("manifest_trajectory_inventory_mismatch")
    # Pick evenly distributed ordinary-size outputs; every object was already
    # hash-verified by the distributed Spark job, including larger trajectories.
    candidates = [x for x in trajectory_files if x["bytes"] <= 64 * 1024 * 1024]
    samples = candidates[:: max(1, len(candidates) // 8)][:8]
    if trajectory_files and not samples:
        raise ValueError("no_trajectory_within_schema_sample_budget")
    for entry in samples:
        response = client.get_object(
            Bucket=location.bucket, Key=location.key(entry["path"])
        )
        try:
            payload = response["Body"].read(entry["bytes"] + 1)
        finally:
            response["Body"].close()
        if (
            len(payload) != entry["bytes"]
            or hashlib.sha256(payload).hexdigest() != entry["sha256"]
        ):
            raise ValueError("trajectory_sample_hash_mismatch")
        value = json.loads(payload)
        parse_trajectory_record(value)
        if (
            type(value["total_tokens"]) is not int
            or value["total_tokens"] < 0
            or value["metadata"]["source_name"] != "boyue"
            or value["metadata"]["specific_source"] != "boyue-rich"
        ):
            raise ValueError("trajectory_sample_fields_mismatch")
    return {
        "valid": True,
        "manifest_sha256": digest.hexdigest(),
        "input_records": job["input_files"],
        "trajectories": job["trajectories"],
        "accepted": job["accepted"],
        "quarantined_trajectories": job["quarantined_trajectories"],
        "sampled_trajectories": len(samples),
        "verified_at": time.time(),
    }


def error_code(error):
    message = str(error)
    return (
        message
        if type(error) in {ValueError, RuntimeError}
        and re.fullmatch(r"[a-z_]+", message)
        else type(error).__name__
    )


class BoyueBatch:
    def __init__(self, options, fixed, release, client, plan):
        self.options, self.fixed, self.release, self.client, self.plan = (
            options,
            fixed,
            release,
            client,
            plan,
        )
        self.root, self.batch = options.state_root, options.batch_root
        self.outcomes = load(self.batch / "outcomes.json", {})
        self.current = None
        self.stopping = False

    def state(self, day):
        return load(self.root / f"boyue-{day}.state.json", {})

    def stop(self, *_):
        self.stopping = True

    def report(self, status="running", error=None):
        rows, totals = [], Counter()
        for day in self.plan["dates"]:
            outcome, state = self.outcomes.get(day, {}), self.state(day)
            effective = outcome.get("status", "not_started")
            if state.get("status") == "running":
                effective = "running"
            if self.current == day and effective in {
                "not_started",
                "pending",
                "failed",
            }:
                effective = "inventory"
            rows.append(
                {"date": day, "status": effective, "reason": outcome.get("reason")}
            )
            if effective == "succeeded":
                totals.update(
                    {
                        k: state["final"]["job"].get(k, 0)
                        for k in [
                            "input_files",
                            "trajectories",
                            "accepted",
                            "quarantined_trajectories",
                        ]
                    }
                )
        current_state = self.state(self.current) if self.current else {}
        progress = (
            read_progress(current_state["run_dir"])
            if current_state.get("run_dir")
            else None
        )
        value = {
            "source": "boyue",
            "cutoff": self.plan["cutoff"],
            "total_dates": len(rows),
            "status": status,
            "pid": os.getpid(),
            "updated_at": time.time(),
            "current": {"source": "boyue", "date": self.current}
            if self.current
            else None,
            "progress": progress,
            "counts": dict(Counter(x["status"] for x in rows)),
            "totals": dict(totals),
            "dates": rows,
        }
        if error:
            value["error"] = error
        atomic_json(self.batch / "outcomes.json", self.outcomes)
        atomic_json(self.batch / "status.json", value)
        counts = value["counts"]
        timestamp = datetime.now(ZoneInfo("Asia/Shanghai")).strftime(
            "%Y-%m-%d %H:%M:%S +0800"
        )
        text = [
            f"更新时间：{timestamp}（每 30 秒更新）",
            "",
            f"仅 Boyue；截至 {self.plan['cutoff']}，共 {len(rows)} 个日期。状态：{status}。",
            "",
            "| 成功 | 空输入 | 运行中 | 输入盘点 | 等待或待处理 | 受阻 |",
            "|---:|---:|---:|---:|---:|---:|",
            f"| {counts.get('succeeded', 0)} | {counts.get('empty', 0)} | {counts.get('running', 0)} | {counts.get('inventory', 0)} | {counts.get('not_started', 0) + counts.get('pending', 0) + counts.get('failed', 0)} | {counts.get('blocked', 0)} |",
            "",
        ]
        if self.current:
            text.append(f"当前日期：**{self.current}**。")
        if progress:
            text.append(
                f"当前阶段：{progress['phase']}；任务已运行约 {progress['elapsed_seconds'] / 60:.1f} 分钟。"
            )
            for stage in progress.get("stages", []):
                text.append(
                    f"阶段分片：{stage['tasks_completed']}/{stage['tasks_total']}；运行中 {stage['tasks_running']}；失败 {stage['tasks_failed']}。"
                )
            text.append("分片比例只代表当前阶段。")
        text.extend(
            [
                "",
                f"正式成功产物：轨迹 {totals['trajectories']:,} 条，接受 {totals['accepted']:,} 条，隔离 {totals['quarantined_trajectories']:,} 条。",
                "",
                "输出：s3://agent-trajectory/lakehouse/boyue-rich/normalized/v001/dt=日期/",
                "",
            ]
        )
        temporary = self.batch / "progress.md.tmp"
        temporary.write_text("\n".join(text))
        os.replace(temporary, self.batch / "progress.md")
        return value

    def verify(self, day):
        self.outcomes[day] = {
            "status": "succeeded",
            "validation": validate_result(self.client, self.state(day)),
        }

    def run_date(self, day):
        require_no_running_states(self.root)
        state = self.state(day)
        if state.get("status") == "succeeded":
            try:
                self.verify(day)
            except (
                ValueError,
                KeyError,
                TypeError,
                OSError,
                BotoCoreError,
                ClientError,
            ) as error:
                self.outcomes[day] = {"status": "blocked", "reason": error_code(error)}
            return
        if state.get("status") == "empty":
            self.outcomes[day] = {"status": "empty"}
            return
        if state.get("attempt", 0) >= 2:
            self.outcomes[day] = {
                "status": "blocked",
                "reason": "two_failed_business_attempts",
            }
            return
        self.current = day
        self.report()
        path = self.batch / "controllers" / f"{day}-{time.time_ns()}.log"
        command = [
            sys.executable,
            str(self.release / "spark/backfill_lakehouse.py"),
            "--config",
            str(self.options.config),
            "--state-root",
            str(self.root),
            "--source",
            "boyue",
            "--date",
            day,
            "--cutoff",
            self.plan["cutoff"],
            "--execute",
        ]
        if self.stopping:
            return
        with path.open("x") as log:
            process = subprocess.Popen(
                command,
                cwd=self.release,
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
                env={
                    **os.environ,
                    "PYTHONPATH": str(self.release / "src"),
                    "PYTHONUNBUFFERED": "1",
                },
            )
            while True:
                try:
                    process.wait(timeout=30)
                    break
                except subprocess.TimeoutExpired:
                    if self.stopping and self.state(day).get("status") != "running":
                        process.terminate()
                    self.report("stopping" if self.stopping else "running")
        require_no_running_states(self.root)
        state = self.state(day)
        if state.get("status") == "succeeded":
            try:
                self.verify(day)
            except (
                ValueError,
                KeyError,
                TypeError,
                OSError,
                BotoCoreError,
                ClientError,
            ) as error:
                self.outcomes[day] = {"status": "blocked", "reason": error_code(error)}
            return
        if state.get("status") == "empty":
            self.outcomes[day] = {"status": "empty"}
            return
        if self.stopping:
            return
        events = []
        for line in path.read_text(errors="replace").splitlines():
            try:
                events.append(json.loads(line))
            except ValueError:
                continue
        blocked = [x for x in events if x.get("event") == "date_blocked"]
        inventories = [x for x in events if x.get("event") == "inventory"]
        previous = self.outcomes.get(day, {})
        if blocked or not inventories:
            reason = (
                blocked[-1].get("error_code", "operation_failed")
                if blocked
                else "controller_failed_before_inventory"
            )
            errors = previous.get("controller_errors", 0) + 1
            permanent = reason in {
                "input_or_code_changed_existing_output_preserved",
                "unowned_nonempty_output_preserved",
            }
            self.outcomes[day] = {
                "status": "blocked" if permanent or errors >= 2 else "pending",
                "reason": reason,
                "controller_errors": errors,
            }
        elif state.get("status") == "failed":
            self.outcomes[day] = {
                "status": "blocked" if state["attempt"] >= 2 else "failed",
                "reason": "business_attempt_failed",
            }
        else:
            self.outcomes[day] = {
                "status": "pending",
                "reason": inventories[-1]["readiness"],
            }

    def run(self):
        try:
            # Account for all sealed dates before starting new work, including
            # a recent completed date that appears near the end of the list.
            for day in self.plan["dates"]:
                if self.stopping:
                    break
                if self.state(day).get("status") in {"succeeded", "empty"}:
                    self.run_date(day)
                    self.report("verifying_existing_outputs")
            while not self.stopping:
                for day in self.plan["dates"]:
                    if self.stopping:
                        break
                    if self.outcomes.get(day, {}).get("status") in {
                        "succeeded",
                        "empty",
                        "blocked",
                    }:
                        continue
                    self.run_date(day)
                    self.current = None
                    self.report()
                self.current = None
                result = self.report(
                    "paused" if self.stopping else "waiting_for_next_inventory"
                )
                if self.stopping:
                    return 0
                counts = result["counts"]
                if sum(
                    counts.get(x, 0) for x in ["succeeded", "empty", "blocked"]
                ) == len(self.plan["dates"]):
                    self.report(
                        "finished_with_blocked_dates"
                        if counts.get("blocked")
                        else "completed"
                    )
                    return int(bool(counts.get("blocked")))
                for _ in range(60):
                    if self.stopping:
                        break
                    time.sleep(30)
                    self.report("waiting_for_next_inventory")
            self.report("paused")
            return 0
        except Exception as error:
            self.report("blocked", error_code(error))
            raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ["config", "state-root", "batch-root"]:
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--cutoff", required=True)
    parser.add_argument("--execute", action="store_true")
    options = parser.parse_args(argv)
    date.fromisoformat(options.cutoff)
    if options.cutoff > frozen_yesterday():
        parser.error("cutoff must exclude today and future dates")
    for path in [options.config, options.state_root, options.batch_root]:
        if not path.is_absolute() or path.is_symlink():
            parser.error("paths must be absolute and not symlinks")
    fixed, flag, release = configuration(options.config)
    verify_acceptance(options.state_root)
    require_no_running_states(options.state_root)
    client = create_s3_client(
        flag("--endpoint-url"),
        flag("--region-name"),
        credentials=load_s3_credentials(flag("--input-credentials-file")),
        max_pool_connections=32,
    )
    try:
        identity = {
            "source": "boyue",
            "cutoff": options.cutoff,
            "release": str(release),
            "source_hash": _source_package_hash(),
            "submit_args": fixed,
        }
        saved = load(options.batch_root / "plan.json")
        if saved:
            if any(saved.get(k) != v for k, v in identity.items()):
                raise ValueError("batch_identity_changed")
            days = saved["dates"]
        else:
            days = discover_dates(client, "boyue", options.cutoff)
        if days != sorted(set(days)) or any(day > options.cutoff for day in days):
            raise ValueError("invalid_frozen_date_list")
        for day in days:
            date_uris("boyue", day)
        plan = {**identity, "dates": days}
        print(
            json.dumps(
                {
                    "source": "boyue",
                    "dates": len(days),
                    "cutoff": options.cutoff,
                    "execute": options.execute,
                }
            ),
            flush=True,
        )
        if not options.execute:
            return 0
        require_no_active_drivers(options.state_root, flag)
        options.batch_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        (options.batch_root / "controllers").mkdir(mode=0o700, exist_ok=True)
        with ExitStack() as stack:
            for name in ["continuation.lock", "supervisor.lock"]:
                stack.enter_context(shared_lock(options.state_root / name))
            require_no_running_states(options.state_root)
            atomic_json(options.batch_root / "plan.json", plan)
            # Keep per-date submissions on the approved argument snapshot even
            # if the deployment's general configuration is edited later.
            atomic_json(
                options.batch_root / "submit-config.json", {"submit_args": fixed}
            )
            options.config = options.batch_root / "submit-config.json"
            batch = BoyueBatch(options, fixed, release, client, plan)
            for sig in [signal.SIGTERM, signal.SIGINT]:
                signal.signal(sig, batch.stop)
            return batch.run()
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
