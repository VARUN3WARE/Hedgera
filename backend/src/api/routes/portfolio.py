"""Portfolio routes"""
from fastapi import APIRouter

router = APIRouter(prefix="/portfolio", tags=["portfolio"])

@router.get("/")
async def get_portfolio():
    return {"portfolio": {}}

@router.get("/positions")
async def get_positions():
    return {"positions": []}
