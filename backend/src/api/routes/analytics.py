"""Analytics routes"""
from fastapi import APIRouter

router = APIRouter(prefix="/analytics", tags=["analytics"])

@router.get("/performance")
async def get_performance():
    return {"performance": {}}

@router.get("/risk")
async def get_risk():
    return {"risk": {}}
