# Aegis Backend

Agentic Trading System Backend

## Setup

```bash
# Install dependencies
pip install uv
uv sync

# Run application
uv run uvicorn src.api.main:app --reload
```

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
