"""Paths for manual FinRL / explainability dev scripts (run from repo root)."""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURES_CSV = REPO_ROOT / "backend" / "tests" / "fixtures" / "trade_data_3days.csv"
EXPLAINERS_DIR = REPO_ROOT / "backend" / "finrl_integration" / "explainers"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
