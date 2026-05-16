from datetime import timedelta

from fastapi import APIRouter, Depends, Header, HTTPException

from backend.config.settings import settings
from backend.src.auth.alpaca_keys import verify_alpaca_keys
from backend.src.auth.deps import require_auth_db
from backend.src.auth.jwt_service import create_access_token, decode_access_token
from backend.src.auth.models.user import (
    BrokerageInfo,
    BrokerageUpdate,
    User,
    UserCreate,
    UserLogin,
    UserSettingsUpdate,
)
from backend.src.auth.passwords import get_password_hash, verify_password

router = APIRouter()


async def get_current_user(authorization: str = Header(None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing token")
    try:
        scheme, token = authorization.split()
        if scheme.lower() != "bearer":
            raise HTTPException(status_code=401, detail="Invalid scheme")
        payload = decode_access_token(token)
        if payload is None:
            raise HTTPException(status_code=401, detail="Invalid token")
        email = payload.get("sub")
        user = await User.find_one(User.email == email)
        if not user:
            raise HTTPException(status_code=401, detail="User not found")
        return user
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(
            status_code=401, detail="Could not validate credentials"
        ) from None


@router.post(
    "/signup", response_model=dict, status_code=201, dependencies=[Depends(require_auth_db)]
)
async def signup(user_in: UserCreate):
    if await User.find_one(User.email == user_in.email):
        raise HTTPException(status_code=400, detail="Email already registered")
    new_user = User(
        name=user_in.name,
        email=user_in.email,
        hashed_password=get_password_hash(user_in.password),
        is_onboarded=False,
    )
    await new_user.insert()
    access_token = create_access_token(data={"sub": new_user.email})
    return {
        "message": "User created successfully",
        "token": access_token,
        "user": {
            "id": str(new_user.id),
            "name": new_user.name,
            "email": new_user.email,
            "isOnboarded": False,
        },
    }


@router.post("/login", dependencies=[Depends(require_auth_db)])
async def login(user_in: UserLogin):
    user = await User.find_one(User.email == user_in.email)
    if not user or not verify_password(user_in.password, user.hashed_password):
        raise HTTPException(status_code=400, detail="Incorrect email or password")
    access_token = create_access_token(
        data={"sub": user.email},
        expires_delta=timedelta(minutes=settings.access_token_expire_minutes),
    )
    is_onboarded = user.is_onboarded or (user.brokerage_info is not None)
    return {
        "token": access_token,
        "user": {
            "id": str(user.id),
            "name": user.name,
            "email": user.email,
            "isOnboarded": is_onboarded,
            "target_profit": user.target_profit,
            "max_risk": user.max_risk,
        },
    }


@router.get("/me", response_model=dict, dependencies=[Depends(require_auth_db)])
async def get_me(current_user: User = Depends(get_current_user)):
    is_onboarded = current_user.is_onboarded or (current_user.brokerage_info is not None)
    return {
        "user": {
            "id": str(current_user.id),
            "name": current_user.name,
            "email": current_user.email,
            "brokerage_info": current_user.brokerage_info,
            "isOnboarded": is_onboarded,
            "target_profit": current_user.target_profit,
            "max_risk": current_user.max_risk,
        }
    }


@router.put("/settings", dependencies=[Depends(require_auth_db)])
async def update_settings(
    body: UserSettingsUpdate,
    current_user: User = Depends(get_current_user),
):
    current_user.target_profit = body.target_profit
    current_user.max_risk = body.max_risk
    await current_user.save()
    return {
        "message": "Settings updated successfully",
        "user": {
            "target_profit": current_user.target_profit,
            "max_risk": current_user.max_risk,
        },
    }


@router.post("/onboarding", dependencies=[Depends(require_auth_db)])
async def onboarding(
    data: BrokerageUpdate,
    current_user: User = Depends(get_current_user),
):
    is_valid, result = await verify_alpaca_keys(
        data.brokerage_api_key, data.brokerage_secret_key
    )
    if not is_valid:
        raise HTTPException(
            status_code=400, detail=f"Alpaca verification failed: {result}"
        )
    current_user.brokerage_info = BrokerageInfo(
        account_nickname=data.account_nickname,
        brokerage_api_key=data.brokerage_api_key,
        brokerage_secret_key=data.brokerage_secret_key,
    )
    current_user.is_onboarded = True
    await current_user.save()
    return {
        "message": "Brokerage info verified & saved",
        "alpaca_account": result,
        "isOnboarded": True,
    }


@router.post("/logout")
async def logout():
    return {"message": "Logged out"}
