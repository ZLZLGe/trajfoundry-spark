"""Bounded, best-effort Driver progress without triggering Spark actions."""

from __future__ import annotations

import json
import threading
import time
from datetime import UTC, datetime
from typing import Any

MAX_STAGES = 32
MAX_JOBS = 64
PHASES = {
    "starting",
    "read_parse_plan",
    "build_publish",
    "merge_inventory",
    "merge_sidecars",
    "publish_manifest",
}


class SparkProgressReporter:
    """Emit heartbeat/task counters; counters are telemetry, not output accounting."""

    def __init__(self, context: Any, *, interval_seconds: float = 60.0):
        if interval_seconds <= 0:
            raise ValueError("progress interval must be positive")
        self._context = context
        self._interval = interval_seconds
        self._started = time.monotonic()
        self._phase = "starting"
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def snapshot(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "time": datetime.now(UTC).isoformat(),
            "phase": self._phase,
            "elapsed_seconds": round(time.monotonic() - self._started, 1),
        }
        try:
            tracker = self._context.statusTracker()
            jobs = tracker.getActiveJobsIds()
            stages = tracker.getActiveStageIds()
            result["active_jobs"] = [int(item) for item in jobs[:MAX_JOBS]]
            result["active_stage_count"] = len(stages)
            result["stages_truncated"] = len(stages) > MAX_STAGES
            summaries = []
            for identifier in stages[:MAX_STAGES]:
                info = tracker.getStageInfo(identifier)
                # A stage can finish between the two status calls.
                if info is not None:
                    summaries.append(
                        {
                            "stage_id": int(identifier),
                            "tasks_total": int(info.numTasks),
                            "tasks_completed": int(info.numCompletedTasks),
                            "tasks_running": int(info.numActiveTasks),
                            "tasks_failed": int(info.numFailedTasks),
                        }
                    )
            result["stages"] = summaries
        except Exception as error:  # noqa: BLE001 - diagnostics must not abort work
            # Do not print exception strings: backend errors may contain secrets.
            result["status_unavailable"] = type(error).__name__
        return result

    def emit(self) -> None:
        print("TF_SPARK_PROGRESS " + json.dumps(self.snapshot(), sort_keys=True), flush=True)

    def set_phase(self, phase: str) -> None:
        if phase not in PHASES:
            raise ValueError("unknown Spark progress phase")
        self._phase = phase
        self.emit()

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("progress reporter already started")
        self.emit()

        def report() -> None:
            while not self._stop.wait(self._interval):
                try:
                    self.emit()
                except Exception:  # noqa: BLE001, S110 - a closed log must not abort work
                    pass

        self._thread = threading.Thread(
            target=report, name="trajfoundry-spark-progress", daemon=True
        )
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1.0)
