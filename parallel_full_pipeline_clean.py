#!/usr/bin/env python3
"""
Repo-root shim for the parallel clean pipeline.

Implementation lives in ``backend.src.orchestration.parallel_clean``. Prefer:

    python -m backend.src.cli --continuous

when the repository root is on ``PYTHONPATH`` (e.g. cwd is repo root).
"""

import sys
from pathlib import Path

_root = Path(__file__).resolve().parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from backend.src.orchestration.parallel_clean import main_sync

if __name__ == "__main__":
    main_sync()
