"""Producer control routes"""
from fastapi import APIRouter

router = APIRouter(prefix="/producers", tags=["producers"])

@router.post("/start")
async def start_producers():
    return {"message": "Producers started"}

@router.post("/stop")
async def stop_producers():
    return {"message": "Producers stopped"}
