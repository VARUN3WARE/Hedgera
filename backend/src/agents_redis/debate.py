import json
import os
import random
from datetime import datetime
from dotenv import load_dotenv
from openai import OpenAI
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage
from typing import List, Dict, Optional, Any
from langchain_core.callbacks import BaseCallbackHandler
import asyncio
import logging

from .base_agent import BaseRedisAgent

# === LOAD ENVIRONMENT VARIABLES ===
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")

if not api_key:
    # Don't raise here; allow agent to run and log warnings
    logging.getLogger(__name__).warning("OPENAI_API_KEY not found in environment; LLM calls will likely fail")

# === CONFIGURATION ===
TICKER = os.getenv("DEBATE_TICKER", "AAPL")
ANALYST_REPORTS_FILE = os.path.join(os.path.dirname(__file__), "final_reports.json")
MAX_STATEMENTS_PER_SIDE = int(os.getenv("DEBATE_MAX_STATEMENTS", "3"))
MODEL = os.getenv("DEBATE_MODEL", "gpt-4.1-nano-2025-04-14")
SEED = 42
random.seed(SEED)

logger = logging.getLogger(__name__)


# === HELPER FUNCTIONS ===


def load_analyst_reports():
    """Load the analyst reports from JSON file (Fallback mode)."""
    logger.info("LOADING ANALYST REPORTS (FROM FILE)")

    if not os.path.exists(ANALYST_REPORTS_FILE):
        logger.warning("Report file not found. Using empty data.")
        return "", "", "", ""

    with open(ANALYST_REPORTS_FILE, "r") as f:
        data = json.load(f)

    agents = {a["agent"]: a["output"] for a in data.get("agents", [])}
    market = agents.get("market_analyst", "")
    social = agents.get("social_media_analyst", "")
    news = agents.get("news_analyst", "")
    fundamentals = agents.get("sec_report_analyst", "")

    return market, social, news, fundamentals


def get_recent_context(debate_log):
    """Format debate log into a readable string for prompts."""
    if not debate_log:
        return "No previous debate yet."
    return "\n".join([f"{m['role']}: {m['content']}" for m in debate_log])


# === AGENT FUNCTIONS (Updated to return Dicts) ===


def bull_statement(reports, debate_log, statement_num, llm: ChatOpenAI, callbacks: List[BaseCallbackHandler] = None):
    """Generate Bull Analyst statement."""
    market, social, news, fundamentals = reports
    context = get_recent_context(debate_log)

    prompt = f"""
    You are the Senior Bullish Equity Strategist advocating a pro-investment case for {TICKER}. 
    This is statement #{statement_num} of {MAX_STATEMENTS_PER_SIDE}.

    Your mission:
    1. Build a high-conviction bullish thesis using insights from Market, Social, News, and Fundamentals.
    2. Identify asymmetric upside opportunities and forward-looking catalysts.
    3. Directly address and neutralize the most recent Bear argument using data-driven counterpoints.
    4. Highlight momentum accelerators, competitive advantages, and any improving KPIs.

    Context you must use:
    {context}

    Available Information:
    - Market Indicators: {market}
    - Social Sentiment: {social}
    - News Cycle: {news}
    - SEC & Fundamentals: {fundamentals}

    Output requirements:
    - Tone: institutional-grade, confident, data-driven.
    - Style: concise but compelling (max 6-7 lines).
    - No generic statements — each line must contain a concrete insight, evidence, or rebuttal.
    """

    messages = [
        SystemMessage(content="You are a Bull Analyst advocating for investing."),
        HumanMessage(content=prompt),
    ]

    response = llm.invoke(messages, config={"callbacks": callbacks} if callbacks else None)

    return {
        "timestamp": datetime.now().isoformat(),
        "role": "BULL",
        "statement_num": statement_num,
        "content": response.content.strip(),
    }


def bear_statement(reports, debate_log, statement_num, llm: ChatOpenAI, callbacks: List[BaseCallbackHandler] = None):
    """Generate Bear Analyst statement."""
    market, social, news, fundamentals = reports
    context = get_recent_context(debate_log)

    prompt = f"""
    You are the Senior Bearish Risk Analyst presenting the cautionary thesis for {TICKER}. 
    This is statement #{statement_num} of {MAX_STATEMENTS_PER_SIDE}.

    Your mission:
    1. Highlight downside risks, structural challenges, and deterioration signals across all reports.
    2. Emphasize valuation concerns, macro pressures, execution risks, or weakening KPIs.
    3. Directly counter the Bull’s last argument with sharp, evidence-based rebuttals.
    4. Identify risk catalysts that could trigger drawdowns, volatility spikes, or estimate revisions.

    Context you must use:
    {context}

    Available Information:
    - Market Indicators: {market}
    - Social Sentiment: {social}
    - News Cycle: {news}
    - SEC & Fundamentals: {fundamentals}

    Output requirements:
    - Tone: analytical, skeptical, and professionally cautious.
    - Style: precise, risk-focused insights only (max 6–7 lines).
    - No vague warnings — every point must reference specific data or patterns.
    """

    messages = [
        SystemMessage(content="You are a Bear Analyst warning against investing."),
        HumanMessage(content=prompt),
    ]

    response = llm.invoke(messages, config={"callbacks": callbacks} if callbacks else None)

    return {
        "timestamp": datetime.now().isoformat(),
        "role": "BEAR",
        "statement_num": statement_num,
        "content": response.content.strip(),
    }


def validate_and_decide(reports, debate_log, llm: ChatOpenAI, callbacks: List[BaseCallbackHandler] = None):
    """Validator: Summarizes debate and outputs specific JSON format."""
    market, social, news, fundamentals = reports
    debate_text = get_recent_context(debate_log)

    prompt = f"""
    You are the **Chief Investment Officer (CIO)** of a Multi-Strategy Hedge Fund.
    You have just witnessed a heated debate between your Growth Manager (Bull) and Risk Manager (Bear) regarding {TICKER}.

    ### YOUR TASK:
    Make the final trading decision based **only** on the evidence presented.
    Do not just summarize what they said; evaluate **who had the stronger data**.

    ### DATA REVIEW:
    - Market Technicals: {market}
    - Fundamentals: {fundamentals}
    - News/Sentiment: {news}\n{social}

    ### DEBATE TRANSCRIPT:
    {debate_text}

    ### STRICT OUTPUT FORMAT (JSON ONLY):
    You must return a JSON object with these exact keys:
    {{
      "positivePoints": ["List 3-4 specific data points favoring the Bull case"],
      "negativePoints": ["List 3-4 specific data points favoring the Bear case"],
      "summary": "A professional verdict paragraph. Weigh the evidence. Did the Bull's growth thesis outweigh the Bear's valuation concerns?",
      "final_recommendation": {{
          "decision": "BUY / SELL / HOLD",
          "conviction": "High / Medium / Low"
      }}
    }}

    Do not output Markdown code blocks. Return raw JSON.
    """

    messages = [
        SystemMessage(content="You are a decisive Chief Investment Officer. Output strictly JSON."),
        HumanMessage(content=prompt),
    ]

    structured_llm = llm
    try:
        response = structured_llm.invoke(messages, config={"callbacks": callbacks} if callbacks else None)
        return json.loads(response.content.strip())
    except Exception as e:
        logger.error(f"JSON Validation Error: {e}")
        return {"positivePoints": ["Error"], "negativePoints": ["Error"], "summary": "Error"}


def run_debate(final_reports_data=None, logs_storage: List[Dict] = None):
    logger.info(f"HYBRID A2A BULL VS BEAR DEBATE: {TICKER}")
    llm = ChatOpenAI(model=MODEL, temperature=0.3)

    # Optional callback handler import
    callbacks = []
    try:
        from src.utils.logger import AgentCallbackHandler
        callbacks = [AgentCallbackHandler("Debate Agent", logs_storage)] if logs_storage is not None else []
    except ImportError:
        # Not fatal - continue without callbacks
        logger.debug("AgentCallbackHandler not available, continuing without callbacks")
    except Exception as e:
        logger.warning(f"Failed to initialize callback handler: {e}")
        pass

    # 1. LOAD DATA (From passed data or file)
    if final_reports_data:
        agents = {a["agent"]: a["output"] for a in final_reports_data.get("agents", [])}
        market = agents.get("market_analyst", "No Data")
        social = agents.get("social_media_analyst", "No Data")
        news = agents.get("news_analyst", "No Data")
        fundamentals = agents.get("sec_report_analyst", "No Data")
        reports = (market, social, news, fundamentals)
        logger.info("Loaded reports directly from pipeline.")
    else:
        reports = load_analyst_reports()

    debate_log = []

    # 2. RUN DEBATE LOOP
    for i in range(1, MAX_STATEMENTS_PER_SIDE + 1):
        first_role = "BULL" if random.random() > 0.5 else "BEAR"

        if first_role == "BULL":
            bull_msg = bull_statement(reports, debate_log, i, llm, callbacks)
            debate_log.append(bull_msg)
            logger.info(f"BULL: {bull_msg['content']}")

            bear_msg = bear_statement(reports, debate_log, i, llm, callbacks)
            debate_log.append(bear_msg)
            logger.info(f"BEAR: {bear_msg['content']}")
        else:
            bear_msg = bear_statement(reports, debate_log, i, llm, callbacks)
            debate_log.append(bear_msg)
            logger.info(f"BEAR: {bear_msg['content']}")

            bull_msg = bull_statement(reports, debate_log, i, llm, callbacks)
            debate_log.append(bull_msg)
            logger.info(f"BULL: {bull_msg['content']}")

    # 3. VALIDATE & DECIDE
    logger.info("VALIDATOR ANALYZING FINAL DEBATE...")
    validation_json = validate_and_decide(reports, debate_log, llm, callbacks)
    logger.info(json.dumps(validation_json, indent=2))

    # 4. RETURN RESULT
    return {
        "ticker": TICKER,
        "timestamp": datetime.now().isoformat(),
        "debate_log": debate_log,
        "validation": validation_json,
    }


class DebateAgentRedis(BaseRedisAgent):
    """Agent that listens for debate triggers and runs the structured debate.

    Expected message format on the trigger stream (JSON in field 'data'):
      {"tickers": ["AAPL"], "final_reports": { ... optional ... }}

    If `final_reports` is not provided, the agent will attempt to load
    `final_reports.json` from the agents_redis folder.
    """

    def __init__(
        self,
        redis_url: str,
        stream_key: str = "trigger:debate",
        consumer_group: str = "debate_agent_group",
        consumer_name: str = "debate_agent_1",
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(redis_url, stream_key, consumer_group, consumer_name, logger)
        self.latest_results: Dict[str, Dict] = {}

    async def process_message(self, message_id: str, data: Dict[str, Any]):
        try:
            raw = json.loads(data.get("data", "{}"))
            final_reports = raw.get("final_reports")
            tickers = raw.get("tickers", [TICKER])

            if not final_reports:
                # Attempt to load file produced by sec_report agent / others
                try:
                    with open(ANALYST_REPORTS_FILE, "r") as f:
                        final_reports = json.load(f)
                except Exception as e:
                    self.logger.error(f"Failed to load final reports file: {e}")
                    final_reports = None

            loop = asyncio.get_event_loop()
            # Run the CPU / network-bound synchronous debate in an executor
            result = await loop.run_in_executor(None, run_debate, final_reports, None)

            # Persist result in-memory
            key = result.get("ticker", ",".join(tickers))
            self.latest_results[key] = result

            # Publish result back to Redis for other services to consume
            try:
                if self.redis_client:
                    await self.redis_client.xadd(
                        "debate:results",
                        {"data": json.dumps(result)}
                    )
            except Exception as e:
                self.logger.warning(f"Failed to publish debate result to Redis: {e}")

            self.logger.info(f"Debate finished for {key}")

        except Exception as e:
            self.logger.error(f"Error running debate: {e}", exc_info=True)


if __name__ == "__main__":
    import asyncio
    from src.core.redis_client import RedisStreamClient

    logging.basicConfig(level=logging.INFO)

    async def main():
        agent = DebateAgentRedis(redis_url=os.getenv("REDIS_URL", "redis://localhost:6379"))
        try:
            await agent.run()
        except KeyboardInterrupt:
            await agent.stop()
        finally:
            await RedisStreamClient.close()

    asyncio.run(main())
