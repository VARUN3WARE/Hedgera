# Pipeline and engine matrix (Phase 0)

Static inventory as of Phase 0. Use this when choosing which script to run or deprecate.

Machine-readable mirror (frozen dataclasses, no runtime imports from pipelines): [`backend/src/development/pipeline_catalog.py`](../../backend/src/development/pipeline_catalog.py).

## Streaming engines (orchestration layer)

Selection is centralized in `backend.src.streaming.create_streaming_engine` (`STREAMING_ENGINE` env, default `redis`).

| Class / module | Engine | Role |
|----------------|--------|------|
| `backend.src.orchestration.pipeline_main.AegisPipeline` | Pathway (explicit) | Pathway-based stream processing |
| `backend.src.orchestration.pipeline_runner` | Pathway (explicit) | Alternate runner around same engine |
| `backend.src.orchestration.pipeline_enhanced.AegisPipelineEnhanced` | From `STREAMING_ENGINE` (default redis) | Enhanced pipeline with FinRL fine-tuning path |

## Root entrypoint scripts

| Script | Orchestration / driver | Streaming | Notes |
|--------|------------------------|-----------|--------|
| `parallel_full_pipeline_clean.py` | `AegisPipelineEnhanced` + local parallel orchestration | Redis `StreamingEngine` | **Thin shim** → `backend.src.orchestration.parallel_clean`; **Docker / preferred CLI:** `python -m backend.src.cli` |
| `python -m backend.src.cli` | Same as shim | Redis `StreamingEngine` | Same flags (`--continuous`, `--single`, `--quick`); compose `aegis-pipeline` default |
| `parallel_full_pipeline.py` | `ParallelFullAegisPipeline` → `AegisPipelineEnhanced` | Redis `StreamingEngine` | Older parallel variant |
| `full_pipeline_enhanced.py` | `FullAegisPipelineEnhanced` → `AegisPipelineEnhanced` | Redis `StreamingEngine` | Full logging variant |
| `full_pipeline.py` | `FullAegisPipeline` → `AegisPipeline` + `MasterStateConsumer` | Pathway `PathwayStreamingEngine` | Wraps classic pipeline |
| `new_enhanced_pipeline.py` | Custom `EnhancedPipeline` (no `AegisPipeline*` class) | None in orchestrator (agents/producers only) | Different workflow shape |
| `run_pipeline_pathway.py` | Standalone | Instantiates `PathwayStreamingEngine` directly | Pathway smoke / dev |
| `main.py` | N/A | N/A | **API only** — not a trading pipeline |

## Monitoring and tests (sample)

| Path | Uses |
|------|------|
| `monitoring/run_1hour_test.py`, `monitoring/test_pipeline.py` | `AegisPipeline` → Pathway engine |
| `monitoring/test_pipeline.py` | Also `FinRLIntegratedService` |
| `unit_tests/*.py` | Various `backend.src.services.*` (FinRL / explainability) |

## How to update this doc

After moving or deleting scripts, adjust this table in the same PR as the code change.
