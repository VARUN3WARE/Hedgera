"""Shared pytest configuration for backend tests."""

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


@pytest.fixture
def repo_root() -> Path:
    return REPO_ROOT


@pytest.fixture
def trade_data_csv(repo_root: Path) -> Path:
    path = repo_root / "backend" / "tests" / "fixtures" / "trade_data_3days.csv"
    if not path.exists():
        pytest.skip(f"fixture missing: {path}")
    return path


@pytest.fixture
def redis_client():
    """Redis client fixture (placeholder for future integration tests)."""
    return None


@pytest.fixture
def db_session():
    """Database session fixture (placeholder)."""
    return None
