# Deferred decisions (Phase 0 — no implementation)

These items are recorded for Phase 1+ so Phase 0 stays documentation-only and low risk.

## HTTP surface: two FastAPI applications

| Location | Role |
|----------|------|
| `backend/src/api/main.py` | AEGIS API: health, FinRL snapshot, Redis-backed market routes — **used by `main.py` and Docker `aegis-api`** |
| `backend/app/main.py` | TradeAI-style API: Beanie/Motor, `/api/login`, onboarding-style routes |

**Gap:** Next.js BFF under `frontend/app/api/auth/` forwards to `${BACKEND_URL}/api/login`, which matches `backend/app`, not `backend/src/api`.

**Options (pick in a later phase):**

1. Merge auth routers into `backend/src/api` (single process, single port).
2. Run auth as a separate service and set `BACKEND_URL` in the frontend stack to that service.

Phase 0 does not change runtime behavior.

## Test layout

`backend/tests/` (pytest) vs repo root `unit_tests/` — consolidation is a later phase with CI updates.

## Pipeline consolidation

Replacing multiple root scripts with `python -m …` CLI is deferred to Phase 2.
