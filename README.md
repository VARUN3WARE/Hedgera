# AEGIS Trading System

Automated trading stack combining **FinRL** (PPO), **multi-agent** analysis (news, social, market, SEC), and **real-time** market data over **Redis** and **MongoDB**. A **Next.js** dashboard talks to a unified **FastAPI** backend.

## Documentation

| Topic | Guide |
|-------|--------|
| Install & configure | [docs/setup.md](docs/setup.md) |
| Run the system | [docs/quickstart.md](docs/quickstart.md) |
| Architecture | [docs/architecture.md](docs/architecture.md) |
| All topics (index) | [docs/README.md](docs/README.md) |

## Prerequisites

- Python 3.11+
- Redis 7+, MongoDB 7+
- API keys: Alpaca (paper), OpenAI; optional NewsAPI / SEC

See [docs/setup.md](docs/setup.md) for full requirements and `.env` variables.

## Quick start

From the **repository root** (with `.env` configured):

```bash
pip install -r requirements.txt

# Optional: historical bars in MongoDB (first-time)
python historical_data.py

# Trading pipeline (recommended)
python -m backend.src.cli --continuous

# API + auth (separate terminal)
python main.py

# Frontend (separate terminal)
cd frontend && pnpm install && pnpm dev
```

Fast local tests:

```bash
./scripts/ci/run-tests.sh
```

Details: [docs/quickstart.md](docs/quickstart.md) · [docs/testing.md](docs/testing.md)

## Docker

```bash
docker compose up -d
```

Services: Redis, MongoDB, `aegis-pipeline`, `aegis-api` (port 8000), `aegis-frontend` (port 3000). See [docs/docker.md](docs/docker.md).

## Repository layout

```
Hedgera/
├── backend/          # Python: API, agents, FinRL, orchestration
├── frontend/         # Next.js dashboard
├── docs/             # Detailed documentation
├── scripts/ci/       # Local test runner (no GitHub Actions yet)
├── main.py           # Starts FastAPI (backend.src.api)
└── requirements.txt  # Delegates to backend/requirements.txt
```

Full tree: [docs/project-structure.md](docs/project-structure.md)

## License

See repository license file if present.
