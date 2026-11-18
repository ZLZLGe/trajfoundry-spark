"""Spark execution helpers for the independent TrajFoundry Spark project.

The existing :mod:`trajfoundry` package remains the source of truth for
capture parsing, trajectory models, and the output contract.  This package
only supplies the distributed execution boundary used by the Spark jobs.
"""

from .config import SparkNormalizeConfig
from .job import run_spark_job

__all__ = ["SparkNormalizeConfig", "run_spark_job"]
