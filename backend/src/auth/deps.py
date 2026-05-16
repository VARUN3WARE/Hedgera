from fastapi import HTTPException

from backend.src.auth.database import is_auth_db_ready


async def require_auth_db() -> None:
    if not is_auth_db_ready():
        raise HTTPException(
            status_code=503,
            detail="User auth database not configured (set MONGODB_URI or MONGODB_URL).",
        )
