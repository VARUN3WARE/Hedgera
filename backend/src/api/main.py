"""FastAPI Main Application"""
import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

try:
    from redis import asyncio as aioredis
except ImportError:
    import redis.asyncio as aioredis

from backend.config.settings import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global Redis client
redis_client: Optional[aioredis.Redis] = None


# Pydantic models
class HealthResponse(BaseModel):
    status: str
    redis_connected: bool
    components: Dict[str, str]


class FinRLDecision(BaseModel):
    timestamp: str
    selected_tickers: List[str]
    actions: Dict[str, Any]
    total_analyzed: int


class MarketState(BaseModel):
    ticker: str
    timestamp: str
    price_data: Dict[str, float]
    indicators: Dict[str, Any]


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle management."""
    global redis_client
    
    # Startup
    logger.info("🚀 Starting FastAPI application...")
    try:
        redis_client = await aioredis.from_url(
            f"redis://{settings.redis_host}:{settings.redis_port}",
            decode_responses=True
        )
        await redis_client.ping()
        logger.info("✅ Connected to Redis")
    except Exception as e:
        logger.error(f"❌ Redis connection failed: {e}")
    
    yield
    
    # Shutdown
    logger.info("🛑 Shutting down FastAPI application...")
    if redis_client:
        await redis_client.close()


app = FastAPI(
    title="AEGIS Trading System API",
    description="Real-time trading system with FinRL integration",
    version="1.0.0",
    lifespan=lifespan
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    return {
        "message": "AEGIS Trading System API",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "finrl_decisions": "/api/finrl/latest",
            "market_state": "/api/market/state",
            "tickers": "/api/tickers"
        }
    }


@app.get("/health", response_model=HealthResponse)
async def health():
    """Health check endpoint."""
    redis_ok = False
    
    if redis_client:
        try:
            await redis_client.ping()
            redis_ok = True
        except:
            pass
    
    return {
        "status": "healthy" if redis_ok else "degraded",
        "redis_connected": redis_ok,
        "components": {
            "api": "running",
            "redis": "connected" if redis_ok else "disconnected"
        }
    }


@app.get("/api/tickers", response_model=List[str])
async def get_tickers():
    """Get list of tracked tickers."""
    return settings.symbols_list


@app.get("/api/finrl/latest", response_model=Optional[FinRLDecision])
async def get_latest_finrl_decision():
    """Get latest FinRL decision."""
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis not available")
    
    try:
        data = await redis_client.get('finrl:latest')
        if not data:
            return None
        
        decision = json.loads(data)
        return decision
    
    except Exception as e:
        logger.error(f"Error fetching FinRL decision: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/market/state")
async def get_market_state(ticker: Optional[str] = None):
    """Get latest market state for ticker(s)."""
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis not available")
    
    try:
        # Read latest from processed stream
        messages = await redis_client.xrevrange(
            'processed:master-state',
            count=30 if not ticker else 100
        )
        
        if not messages:
            return {"message": "No data available"}
        
        states = []
        for message_id, fields in messages:
            try:
                data = json.loads(fields.get('data', '{}'))
                if ticker and data.get('metadata', {}).get('ticker') != ticker:
                    continue
                states.append(data)
                if ticker:
                    break  # Found the ticker
            except:
                continue
        
        if ticker:
            return states[0] if states else {"message": f"No data for {ticker}"}
        
        return {"count": len(states), "states": states[:10]}  # Return first 10
    
    except Exception as e:
        logger.error(f"Error fetching market state: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/streams/info")
async def get_streams_info():
    """Get information about Redis streams."""
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis not available")
    
    try:
        streams = [
            "raw:price-updates",
            "raw:news-articles",
            "raw:social",
            "processed:master-state",
            "finrl-decisions"
        ]
        
        info = {}
        for stream in streams:
            try:
                length = await redis_client.xlen(stream)
                info[stream] = {"length": length}
            except:
                info[stream] = {"length": 0, "exists": False}
        
        return info
    
    except Exception as e:
        logger.error(f"Error fetching stream info: {e}")
        raise HTTPException(status_code=500, detail=str(e))

