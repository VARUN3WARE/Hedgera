#!/usr/bin/env bash
# Local CI: all backend tests including finrl/slow (needs torch, model artifacts).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

export PYTHONPATH=.

if ! python -c "import pytest" 2>/dev/null; then
  python -m pip install --upgrade pip
  pip install -r requirements.txt
  pip install pytest pytest-asyncio
fi

echo "Running full pytest suite..."
pytest --tb=short -q "$@"
