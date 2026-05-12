# Phase 0 — Inventory and guardrails

Small, focused documents. Each file has one responsibility so later phases can update them independently.

| Document | Purpose |
|----------|---------|
| [runbook.md](runbook.md) | Supported commands for API, pipeline, and Docker |
| [pipeline-matrix.md](pipeline-matrix.md) | Which root scripts use which orchestration and streaming engine |
| [decisions.md](decisions.md) | Open choices reserved for later phases (no behavior change here) |

## Principles for follow-up refactors

- Prefer incremental moves (shims, then delete) over big-bang renames.
- One behavioral change per PR where possible.
- Keep orchestration, streaming, and HTTP layers separated when merging code.
