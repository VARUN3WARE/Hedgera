from motor.motor_asyncio import AsyncIOMotorClient
from beanie import init_beanie
from app.config import settings
from app.models.user import User
from app.models.agent import AgentDecision

async def init_db():
    client = AsyncIOMotorClient(settings.MONGODB_URL)
    database = client[settings.DB_NAME]
    
    # Initialize Beanie with the Document classes
    await init_beanie(database, document_models=[User, AgentDecision])