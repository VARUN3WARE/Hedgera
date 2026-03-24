"""Agent routes"""
from fastapi import APIRouter

router = APIRouter(prefix="/agents", tags=["agents"])

@router.get("/status")
async def get_agents_status():
    return {"agents": []}

@router.post("/start")
async def start_agents():
    return {"message": "Agents started"}
