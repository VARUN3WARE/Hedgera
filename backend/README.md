# Aegis backend

Python services for the AEGIS trading system: producers, streaming, FinRL, Redis agents, and the unified FastAPI app.

**Full documentation:** [docs/README.md](../docs/README.md) · [Setup](../docs/setup.md) · [API / Redis reference](../docs/api.md)

## Install

From the **repository root**:

```bash
pip install -r requirements.txt
pip install pytest pytest-asyncio   # optional, for tests
```

With **uv** (from this directory):

```bash
uv sync
```

## Run

| Task | Command (from repo root) |
|------|---------------------------|
| API | `python main.py` |
| Pipeline | `python -m backend.src.cli --continuous` |
| Tests | `./scripts/ci/run-tests.sh` |

## Layout

- `src/api/` — FastAPI application
- `src/auth/` — User models, JWT, Beanie
- `src/orchestration/` — Pipeline runners
- `src/streaming/` — Engine factory
- `src/agents_redis/` — LLM agents and execution
- `src/services/` — FinRL, MongoDB sync, explainability
- `tests/` — pytest suites
- `finrl_integration/` — PPO model assets
