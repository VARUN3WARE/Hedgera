# Runbook — supported entrypoints (Phase 0)

All commands assume repository root as current working directory unless noted.

## Configuration

- Copy or maintain a `.env` at the repo root (required by several pipelines).
- Pipeline scripts load it before importing `backend.*` (see `parallel_full_pipeline_clean.py`).

## REST API (AEGIS monitoring and data)

**Canonical app:** `backend.src.api.main:app`

```bash
python main.py
```

- Uses `uvicorn` with host/port from `backend.config.settings` (`settings.api_host`, `settings.api_port`).
- **Docker:** `docker-compose` service `aegis-api` runs `python main.py` (see repository `docker-compose.yml`).

## Trading pipeline

**Canonical for production-style compose:** `parallel_full_pipeline_clean.py`

```bash
python parallel_full_pipeline_clean.py --continuous
# or quick single run (also Docker image default CMD)
python parallel_full_pipeline_clean.py --single --quick
```

- Other root scripts exist for experiments and legacy flows; see [pipeline-matrix.md](pipeline-matrix.md).

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
