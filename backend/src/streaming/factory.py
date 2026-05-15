"""Construct the configured streaming engine implementation."""

import logging
from typing import Optional, Union

from backend.config.settings import settings
from backend.src.engine.streaming_engine import StreamingEngine
from backend.src.pathway_engine.streaming_engine_pathway import PathwayStreamingEngine
from backend.src.streaming.kind import StreamingEngineKind, normalize_engine_kind

logger = logging.getLogger(__name__)


def create_streaming_engine(
    kind: Union[str, StreamingEngineKind, None] = None,
    *,
    redis_host: Optional[str] = None,
    redis_port: Optional[int] = None,
    redis_password: Optional[str] = None,
):
    """
    Return a streaming engine instance.

    Args:
        kind: ``redis`` (manual Redis Streams) or ``pathway``. Defaults to
            ``settings.streaming_engine`` when omitted.
        redis_host: Used by the Pathway engine; Redis engine reads from settings.
        redis_port: Used by the Pathway engine.
        redis_password: Used by the Pathway engine.
    """
    resolved = normalize_engine_kind(kind)
    host = redis_host if redis_host is not None else settings.redis_host
    port = redis_port if redis_port is not None else settings.redis_port
    password = (
        redis_password
        if redis_password is not None
        else (settings.redis_password or "")
    )

    if resolved is StreamingEngineKind.REDIS:
        logger.info("Streaming engine: Redis manual (StreamingEngine)")
        return StreamingEngine()

    logger.info("Streaming engine: Pathway (PathwayStreamingEngine)")
    return PathwayStreamingEngine(
        redis_host=host,
        redis_port=port,
        redis_password=password,
    )
