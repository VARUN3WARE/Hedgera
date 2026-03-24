from typing import List, Optional, Dict, Any
from beanie import Document
from pydantic import BaseModel
from datetime import datetime

# --- Sub-models for the JSON structure ---

class AgentOutput(BaseModel):
    agent: str
    output: str

class Summary(BaseModel):
    ticker: str
    timestamp: datetime
    agents: List[AgentOutput]

class DebateLog(BaseModel):
    timestamp: datetime
    role: str # BULL or BEAR
    statement_num: int
    content: str

class Validation(BaseModel):
    positivePoints: List[str]
    negativePoints: List[str]
    summary: str

class DebateOutcome(BaseModel):
    ticker: str
    timestamp: datetime
    debate_log: List[DebateLog]
    validation: Validation
    
    # New fields requested
    decision_taken: str # "buy", "sell", "hold"
    shares: Optional[float] = 0
    price_at_decision: float
    confidence_score: float

# --- Main Document Model ---

class AgentDecision(Document):
    user_id: str  # Link to the user who owns this decision (optional, if multi-tenant)
    summary: Summary
    debate_outcome: DebateOutcome
    created_at: datetime = datetime.utcnow()

    class Settings:
        name = "agent_decisions"