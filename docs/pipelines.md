# Pipelines

## Recommended entrypoint

| Command | Role |
|---------|------|
| `python -m backend.src.cli --continuous` | **Default** — parallel clean pipeline (Docker `aegis-pipeline`) |
| `python -m backend.src.cli --single --quick` | One quick cycle |
| `parallel_full_pipeline_clean.py` | Thin shim to the same implementation |

Implementation: [`backend/src/orchestration/parallel_clean.py`](../backend/src/orchestration/parallel_clean.py) (starts `AegisPipelineEnhanced` in the background, then runs agents in parallel).

## Streaming engine

Controlled by `STREAMING_ENGINE` in `.env`:

- `redis` (default) — manual Redis Streams engine (`StreamingEngine`)
- `pathway` — Pathway-based engine

Factory: `backend.src.streaming.create_streaming_engine`. Classic orchestrators `pipeline_main` / `pipeline_runner` still request Pathway explicitly.

## Parallel pipeline (production-style)

**Features:**

- `asyncio.gather` for four agents per ticker
- Pathway aggregation for consensus metrics
- FinRL vs validator reconciliation and Alpaca execution

**Workflow:**

1. Enhanced pipeline collects prices and runs FinRL in the background.
2. Wait for first `finrl-decisions` stream output.
3. Fetch market, news, and social data for selected tickers.
4. Run agents in parallel per ticker; debate and reconcile.
5. Execute approved trades via the decision agent.

**Usage:**

```bash
python -m backend.src.cli --continuous
python -m backend.src.cli --single
python -m backend.src.cli --single --quick
```

## Enhanced pipeline (legacy script)

**File:** `full_pipeline_enhanced.py`

MongoDB sync, fine-tuning every two hours, trigger-based FinRL runs. Still supported for experiments.

```bash
python full_pipeline_enhanced.py
python full_pipeline_enhanced.py --single
python full_pipeline_enhanced.py --quick --single
```

## Other entrypoints

| Script | Notes |
|--------|--------|
| `full_pipeline.py` | Wraps `AegisPipeline` + Pathway engine |
| `run_pipeline_pathway.py` | Pathway engine smoke test |
| `new_enhanced_pipeline.py` | Custom orchestration without shared `AegisPipeline*` class |

Inventory metadata: `backend/src/development/pipeline_catalog.py`.

## Log layout

```
agent_logs/
└── parallel_YYYYMMDD_HHMMSS/
    ├── pipeline.log
    └── cycle_01_YYYYMMDD_HHMMSS/
        ├── 00_SUMMARY.json
        ├── 01_finrl_output.json
        ├── 03_news_data.json
        ├── 05_agent_{TICKER}.json
        ├── 06_reconciliation.json
        └── 07_trades.json
```

Enhanced runs use `agent_logs/enhanced_*` with a similar cycle structure.
