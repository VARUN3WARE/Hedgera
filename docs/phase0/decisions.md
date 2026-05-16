# Deferred decisions (historical notes)

## HTTP surface (resolved)

User auth (Beanie + JWT) is served by the same FastAPI app as trading/monitoring routes:

| Location | Role |
|----------|------|
| `backend/src/api/main.py` | Unified API: Redis routes + `/api/login`, signup, onboarding, etc. |
| `backend/src/auth/` | User models, Beanie init, JWT, Alpaca key verification |

The legacy `backend/app/` package was removed. Set `BACKEND_URL` for the Next.js BFF (see `docker-compose.yml` `aegis-frontend`).

## Test layout

See `unit_tests/README.md` and `pytest.ini` — FinRL suites live under `backend/tests/finrl/`.

## Pipeline entrypoints

Prefer `python -m backend.src.cli` from the repo root; `parallel_full_pipeline_clean.py` remains a thin shim.
