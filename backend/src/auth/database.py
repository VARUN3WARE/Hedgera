import logging
from typing import Optional

from beanie import init_beanie
from motor.motor_asyncio import AsyncIOMotorClient

from backend.config.settings import settings
from backend.src.auth.models.agent import AgentDecision
from backend.src.auth.models.user import User

logger = logging.getLogger(__name__)

_motor_client: Optional[AsyncIOMotorClient] = None
_auth_db_ready: bool = False


def is_auth_db_ready() -> bool:
    return _auth_db_ready


async def init_auth_db() -> None:
    global _motor_client, _auth_db_ready
    _auth_db_ready = False
    url = settings.beanie_mongodb_url
    if not url:
        logger.warning(
            "Auth DB not initialized: set MONGODB_URI or MONGODB_URL for signup/login."
        )
        return
    _motor_client = AsyncIOMotorClient(url)
    database = _motor_client[settings.beanie_database_name]
    await init_beanie(database=database, document_models=[User, AgentDecision])
    _auth_db_ready = True
    logger.info("Beanie auth DB ready: %s", settings.beanie_database_name)


def close_auth_db() -> None:
    global _motor_client, _auth_db_ready
    _auth_db_ready = False
    if _motor_client is not None:
        _motor_client.close()
        _motor_client = None
