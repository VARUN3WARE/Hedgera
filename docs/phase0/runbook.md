# Runbook — supported entrypoints (Phase 0)

All commands assume repository root as current working directory unless noted.

## Configuration

- Copy or maintain a `.env` at the repo root (required by several pipelines).
- Pipeline scripts load it before importing `backend.*` (see `parallel_full_pipeline_clean.py`).
- **Streaming engine:** `STREAMING_ENGINE=redis` (default) or `pathway`. Enhanced/CLI pipelines use this setting; `pipeline_main` and `pipeline_runner` still select Pathway explicitly.

## Tests

```bash
pytest -m "not slow and not finrl"    # default CI set
pytest -m finrl                       # FinRL suites under backend/tests/finrl/
```

Legacy `unit_tests/` paths are documented in [`unit_tests/README.md`](../../unit_tests/README.md).

## REST API (AEGIS monitoring and data)

**Canonical app:** `backend.src.api.main:app`

```bash
python main.py
```

- Uses `uvicorn` with host/port from `backend.config.settings` (`settings.api_host`, `settings.api_port`).
- **Docker:** `docker-compose` service `aegis-api` runs `python main.py` (see repository `docker-compose.yml`).

## Trading pipeline

**Preferred:** module CLI (from repo root, same flags as the legacy script):

```bash
python -m backend.src.cli --continuous
python -m backend.src.cli --single --quick
```

**Shim (backward compatible):** `parallel_full_pipeline_clean.py` delegates to the same implementation.

**Canonical for production-style compose:** `python -m backend.src.cli --continuous` (see `docker-compose.yml` `aegis-pipeline`).

## Docker stack

```bash
docker compose up -d
```

Typical services: Redis, MongoDB, `aegis-pipeline`, `aegis-api`, `aegis-frontend`. See `docker-compose.yml` for commands and ports.

## Frontend

```bash
cd frontend && pnpm install && pnpm dev
```

- BFF routes under `frontend/app/api/` may call a separate auth backend URL via `BACKEND_URL`; see [decisions.md](decisions.md).

## Python dependencies

- **pip / Docker:** repo root `requirements.txt` includes `backend/requirements.txt`.
- **uv / hatch:** prefer `backend/pyproject.toml` for local development (`uv sync` from `backend/` per `backend/README.md`).
