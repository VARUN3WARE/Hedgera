# Aegis Backend

Agentic Trading System Backend

## Setup

**pip (from repo root):**

```bash
pip install -r requirements.txt
pip install -r backend/requirements-dev.txt
```

**uv (from this directory):**

```bash
pip install uv
uv sync
```

Imports use the `backend` package; run the API from the repo root with `python main.py`, or from here with `PYTHONPATH=.. uv run uvicorn src.api.main:app --reload`.

## Structure

- `src/` - Source code
- `config/` - Configuration files
- `tests/` - Tests (`finrl/` for heavy model suites)
- `scripts/` - Operational scripts (`scripts/dev/` for manual FinRL tools)

## Tests

From the repository root:

```bash
pip install pytest pytest-asyncio
pytest -m "not slow and not finrl"
pytest -m finrl
```

## Run API

From the repository root:

```bash
python main.py
```
