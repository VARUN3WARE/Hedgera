from datetime import datetime
from typing import List, Optional

from beanie import Document
from pydantic import BaseModel


class AgentOutput(BaseModel):
    agent: str
    output: str


class Summary(BaseModel):
    ticker: str
    timestamp: datetime
    agents: List[AgentOutput]


class DebateLog(BaseModel):
    timestamp: datetime
    role: str
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
    decision_taken: str
    shares: Optional[float] = 0
    price_at_decision: float
    confidence_score: float


class AgentDecision(Document):
    user_id: str
    summary: Summary
    debate_outcome: DebateOutcome
    created_at: datetime = datetime.utcnow()

    class Settings:
        name = "agent_decisions"
