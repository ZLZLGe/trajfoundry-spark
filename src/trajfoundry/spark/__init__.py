"""Spark execution helpers for the independent TrajFoundry Spark project.

The existing :mod:`trajfoundry` package remains the source of truth for
capture parsing, trajectory models, and the output contract.  This package
only supplies the distributed execution boundary used by the Spark jobs.
"""

from .config import SparkNormalizeConfig

__all__ = ["SparkNormalizeConfig"]
