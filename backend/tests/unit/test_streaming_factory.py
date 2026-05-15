"""Streaming engine factory and kind normalization."""

import pytest

from backend.src.engine.streaming_engine import StreamingEngine
from backend.src.streaming.factory import create_streaming_engine
from backend.src.streaming.kind import StreamingEngineKind, normalize_engine_kind


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("redis", StreamingEngineKind.REDIS),
        ("REDIS", StreamingEngineKind.REDIS),
        ("redis_manual", StreamingEngineKind.REDIS),
        ("manual", StreamingEngineKind.REDIS),
        ("pathway", StreamingEngineKind.PATHWAY),
        ("PATHWAY", StreamingEngineKind.PATHWAY),
    ],
)
def test_normalize_engine_kind(raw, expected):
    assert normalize_engine_kind(raw) is expected


def test_normalize_invalid_kind():
    with pytest.raises(ValueError, match="Unknown streaming engine"):
        normalize_engine_kind("kafka")


def test_create_redis_engine():
    engine = create_streaming_engine(StreamingEngineKind.REDIS)
    assert isinstance(engine, StreamingEngine)


def test_create_pathway_engine():
    pytest.importorskip("pathway")
    from backend.src.pathway_engine.streaming_engine_pathway import PathwayStreamingEngine

    engine = create_streaming_engine(StreamingEngineKind.PATHWAY)
    assert isinstance(engine, PathwayStreamingEngine)
