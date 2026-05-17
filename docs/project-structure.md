# Project structure

```
Hedgera/
├── backend/
│   ├── config/                 # settings, logging
│   ├── src/
│   │   ├── api/                # FastAPI app + routes (trading + auth)
│   │   ├── auth/               # Beanie users, JWT
│   │   ├── agents_redis/       # News, social, market, SEC, debate, execution
│   │   ├── cli/                # python -m backend.src.cli
│   │   ├── consumers/
│   │   ├── engine/             # Redis manual streaming engine
│   │   ├── orchestration/    # Pipelines (parallel_clean, enhanced, main)
│   │   ├── pathway_engine/     # Pathway streaming engine
│   │   ├── producers/          # Price, news, social
│   │   ├── services/           # FinRL, MongoDB sync, explainability
│   │   ├── streaming/          # Engine factory (redis vs pathway)
│   │   └── utils/
│   ├── finrl_integration/      # PPO model, explainers
│   ├── scripts/                # Ops + dev utilities
│   └── tests/                  # pytest tree
├── frontend/                   # Next.js app (BFF under app/api/)
├── docs/                       # Documentation (this folder)
├── scripts/ci/                 # Local test runners
├── agent_logs/                 # Pipeline run output
├── main.py                     # Uvicorn → backend.src.api
├── requirements.txt            # → backend/requirements.txt
├── parallel_full_pipeline_clean.py  # Shim → orchestration.parallel_clean
├── historical_data.py
├── docker-compose.yml
└── README.md                   # Short overview + links
```

Optional / legacy at repo root: `full_pipeline_enhanced.py`, `full_pipeline.py`, `monitor_pipeline_detailed.py`, `dashboard_bridge.py`, `run_pipeline_pathway.py`.
