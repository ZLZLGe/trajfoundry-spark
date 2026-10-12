"""Narrow Spark output names shared by metadata and storage validation.

Trajectory and manifest paths remain flat. Only known audit sidecars may live
one level below the output root, under ``diagnostics/``. Names are not URL
decoded or path-normalized: unsafe spellings must be rejected, not rewritten.
"""

from __future__ import annotations

import re

DIAGNOSTICS_DIRECTORY = "diagnostics"
RUN_MARKER = ".trajfoundry-spark-run"
WRITE_TEMP_PREFIX = ".trajfoundry-write-"
_PROBE_NAME = r"\.trajfoundry-access-probe-[0-9a-f]{32}"
_PART_NAME = r"\._(?:lineage|diagnostic|objects)_part_"
_DIAGNOSTIC_NAME = re.compile(
    rf"(?:input_diagnostics\.ndjson|{_PROBE_NAME}|"
    rf"{_PART_NAME}(?:[0-9a-f]{{32}}_)?[0-9]{{6,}}\.ndjson)"
)
_RETAINED_NAME = re.compile(
    rf"(?:{_PROBE_NAME}|{_PART_NAME}[0-9a-f]{{32}}_[0-9]{{6,}}\.ndjson)"
)


def validate_flat_output_name(name: str) -> str:
    if (
        not isinstance(name, str)
        or not name
        or name in {".", "..", RUN_MARKER}
        or name.startswith(WRITE_TEMP_PREFIX)
        or "/" in name
        or "\\" in name
        or any(ord(char) < 32 or ord(char) == 127 for char in name)
    ):
        raise ValueError("Spark output names must be safe flat filenames")
    return name


def validate_output_name(name: object) -> str:
    """Accept a safe flat file or one explicitly known diagnostics sidecar."""
    if isinstance(name, str) and name.startswith(DIAGNOSTICS_DIRECTORY + "/"):
        basename = name[len(DIAGNOSTICS_DIRECTORY) + 1 :]
        validate_flat_output_name(basename)
        if _DIAGNOSTIC_NAME.fullmatch(basename) is None:
            raise ValueError("only known Spark sidecars may be under diagnostics/")
        return name
    return validate_flat_output_name(name)


def is_retained_s3_auxiliary_name(name: object) -> bool:
    """Recognize old/new run-scoped auxiliaries, never published output data."""
    if not isinstance(name, str):
        return False
    if name.startswith(DIAGNOSTICS_DIRECTORY + "/"):
        name = name[len(DIAGNOSTICS_DIRECTORY) + 1 :]
    return _RETAINED_NAME.fullmatch(name) is not None
