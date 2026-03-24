from typing import Optional
from beanie import Document, Indexed
from pydantic import BaseModel, EmailStr, Field

class BrokerageInfo(BaseModel):
    account_nickname: Optional[str] = None
    brokerage_api_key: Optional[str] = None
    brokerage_secret_key: Optional[str] = None

class User(Document):
    name: str
    email: Indexed(EmailStr, unique=True)
    hashed_password: str
    brokerage_info: Optional[BrokerageInfo] = None
    is_onboarded: bool = False
    
    target_profit: float = 15.0  # Default 15%
    max_risk: float = 5.0        # Default 5%

    class Settings:
        name = "users"

# Pydantic Schemas
class UserCreate(BaseModel):
    name: str
    email: EmailStr
    password: str

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class UserResponse(BaseModel):
    id: str
    name: str
    email: EmailStr
    brokerage_info: Optional[BrokerageInfo] = None
    is_onboarded: bool
    target_profit: float
    max_risk: float

class BrokerageUpdate(BaseModel):
    account_nickname: str
    brokerage_api_key: str
    brokerage_secret_key: str

class UserSettingsUpdate(BaseModel):
    target_profit: float
    max_risk: float