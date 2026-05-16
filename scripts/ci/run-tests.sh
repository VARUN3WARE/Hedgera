#!/usr/bin/env bash
# Local CI: fast backend unit tests (no GitHub Actions required).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

export PYTHONPATH=.

if ! python -c "import pytest" 2>/dev/null; then
  echo "Installing test dependencies..."
  python -m pip install --upgrade pip
  pip install -r requirements.txt
  pip install pytest pytest-asyncio
fi

echo "Running pytest (excluding slow and finrl markers)..."
pytest -m "not slow and not finrl" --tb=short -q "$@"
