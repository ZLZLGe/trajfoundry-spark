"""Resource-center launcher; load current shared Spark deployment on every run."""

import runpy
import sys
from pathlib import Path

if __name__ == "__main__":
    project = Path("/share/gezhilong/trajfoundry-spark")
    sys.path.insert(0, str(project / "spark"))
    runpy.run_path(str(project / "spark/submit_scheduler.py"), run_name="__main__")
