import json
import threading
from types import SimpleNamespace

import pytest

from trajfoundry.spark.progress import MAX_JOBS, MAX_STAGES, SparkProgressReporter


class Tracker:
    def getActiveJobsIds(self):
        return list(range(100))

    def getActiveStageIds(self):
        return list(range(100))

    def getStageInfo(self, identifier):
        if identifier == 0:
            return None
        return SimpleNamespace(
            numTasks=128, numCompletedTasks=65, numActiveTasks=63, numFailedTasks=0
        )


def test_bounded_progress_snapshot():
    context = SimpleNamespace(statusTracker=lambda: Tracker())
    value = SparkProgressReporter(context).snapshot()
    assert value["phase"] == "starting"
    assert value["elapsed_seconds"] >= 0
    assert len(value["active_jobs"]) == MAX_JOBS
    assert value["active_stage_count"] == 100
    assert value["stages_truncated"] is True
    assert len(value["stages"]) == MAX_STAGES - 1
    assert value["stages"][0]["tasks_completed"] == 65


def test_status_failure_is_sanitized():
    def fail():
        raise RuntimeError("secret-value-must-not-appear")

    value = SparkProgressReporter(SimpleNamespace(statusTracker=fail)).snapshot()
    assert value["status_unavailable"] == "RuntimeError"
    assert "secret-value" not in json.dumps(value)


def test_phase_emits_parseable_progress(capsys):
    reporter = SparkProgressReporter(SimpleNamespace(statusTracker=lambda: Tracker()))
    reporter.set_phase("build_publish")
    output = capsys.readouterr().out
    assert output.startswith("TF_SPARK_PROGRESS ")
    assert json.loads(output.partition(" ")[2])["phase"] == "build_publish"
    with pytest.raises(ValueError, match="unknown"):
        reporter.set_phase("secret")


def test_periodic_reporter_stops(monkeypatch):
    reporter = SparkProgressReporter(None, interval_seconds=0.01)
    event = threading.Event()
    calls = []

    def emit():
        calls.append(1)
        if len(calls) >= 2:
            event.set()

    monkeypatch.setattr(reporter, "emit", emit)
    reporter.start()
    try:
        assert event.wait(2)
        with pytest.raises(RuntimeError, match="already"):
            reporter.start()
    finally:
        reporter.close()
    assert not reporter._thread.is_alive()


def test_progress_requires_positive_interval():
    with pytest.raises(ValueError):
        SparkProgressReporter(None, interval_seconds=0)
