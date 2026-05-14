"""Read-only catalog of root-level pipeline scripts.

Open/closed: add new ``PipelineScriptInfo`` rows when entrypoints change rather
than scattering comments across scripts. Phase 0 does not import this module
from runtime code.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class StreamingKind(str, Enum):
    """Which streaming implementation the orchestration uses."""

    PATHWAY = "pathway"
    REDIS_MANUAL = "redis_manual"
    NONE = "none"


@dataclass(frozen=True)
class PipelineScriptInfo:
    """One repo-root Python driver related to the trading pipeline."""

    path: str
    orchestration: str
    streaming: StreamingKind
    docker_default: bool
    description: str


PIPELINE_SCRIPTS: tuple[PipelineScriptInfo, ...] = (
    PipelineScriptInfo(
        path="parallel_full_pipeline_clean.py",
        orchestration="AegisPipelineEnhanced + script-local parallel flow",
        streaming=StreamingKind.REDIS_MANUAL,
        docker_default=True,
        description="Shim to backend.src.orchestration.parallel_clean; prefer: python -m backend.src.cli",
    ),
    PipelineScriptInfo(
        path="parallel_full_pipeline.py",
        orchestration="ParallelFullAegisPipeline → AegisPipelineEnhanced",
        streaming=StreamingKind.REDIS_MANUAL,
        docker_default=False,
        description="Parallel variant with extended logging",
    ),
    PipelineScriptInfo(
        path="full_pipeline_enhanced.py",
        orchestration="FullAegisPipelineEnhanced → AegisPipelineEnhanced",
        streaming=StreamingKind.REDIS_MANUAL,
        docker_default=False,
        description="Full enhanced logging wrapper",
    ),
    PipelineScriptInfo(
        path="full_pipeline.py",
        orchestration="FullAegisPipeline → AegisPipeline + MasterStateConsumer",
        streaming=StreamingKind.PATHWAY,
        docker_default=False,
        description="Classic pipeline using PathwayStreamingEngine",
    ),
    PipelineScriptInfo(
        path="new_enhanced_pipeline.py",
        orchestration="EnhancedPipeline (custom, not AegisPipeline*)",
        streaming=StreamingKind.NONE,
        docker_default=False,
        description="Agents/producers path without shared orchestration class",
    ),
    PipelineScriptInfo(
        path="run_pipeline_pathway.py",
        orchestration="Standalone script",
        streaming=StreamingKind.PATHWAY,
        docker_default=False,
        description="Direct PathwayStreamingEngine instantiation / smoke",
    ),
    PipelineScriptInfo(
        path="main.py",
        orchestration="N/A",
        streaming=StreamingKind.NONE,
        docker_default=False,
        description="Launches FastAPI (backend.src.api.main), not a trading pipeline",
    ),
)


ORCHESTRATION_ENGINES: tuple[tuple[str, str, StreamingKind], ...] = (
    (
        "backend.src.orchestration.pipeline_main.AegisPipeline",
        "PathwayStreamingEngine",
        StreamingKind.PATHWAY,
    ),
    (
        "backend.src.orchestration.pipeline_runner.PipelineOrchestrator",
        "PathwayStreamingEngine",
        StreamingKind.PATHWAY,
    ),
    (
        "backend.src.orchestration.pipeline_enhanced.AegisPipelineEnhanced",
        "StreamingEngine",
        StreamingKind.REDIS_MANUAL,
    ),
)
