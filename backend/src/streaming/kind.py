from enum import Enum
from typing import Union


class StreamingEngineKind(str, Enum):
    """Which streaming implementation to run."""

    REDIS = "redis"
    PATHWAY = "pathway"


_ALIASES = {
    "redis": StreamingEngineKind.REDIS,
    "redis_manual": StreamingEngineKind.REDIS,
    "manual": StreamingEngineKind.REDIS,
    "legacy": StreamingEngineKind.REDIS,
    "pathway": StreamingEngineKind.PATHWAY,
}


def normalize_engine_kind(value: Union[str, StreamingEngineKind, None]) -> StreamingEngineKind:
    if value is None:
        from backend.config.settings import settings

        return normalize_engine_kind(settings.streaming_engine)
    if isinstance(value, StreamingEngineKind):
        return value
    key = str(value).strip().lower()
    if key not in _ALIASES:
        raise ValueError(
            f"Unknown streaming engine {value!r}; use 'redis' or 'pathway' "
            f"(aliases: redis_manual, manual, legacy)"
        )
    return _ALIASES[key]
