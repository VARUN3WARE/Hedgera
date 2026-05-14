"""
Run the default trading pipeline from the repo root:

    cd /path/to/Hedgera && python -m backend.src.cli --continuous

Arguments match the legacy ``parallel_full_pipeline_clean.py`` script
(``--single``, ``--quick``).
"""

import sys
from pathlib import Path

_root = Path(__file__).resolve().parents[3]
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from backend.src.orchestration.parallel_clean import main_sync

if __name__ == "__main__":
    main_sync()
