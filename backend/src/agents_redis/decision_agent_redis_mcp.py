"""
Decision Agent (Redis-Compatible) with actual MCP Integration

This agent uses MCP server (via HTTP) for Alpaca operations:
1. Receives ranked stock recommendations from the pipeline
2. Fetches current portfolio via MCP HTTP calls
3. Uses LLM (GPT-4) to determine trade quantities
4. Executes trades via MCP HTTP calls
5. Logs results to Redis and JSON

The MCP server must be running on the specified URL for this agent to work.
"""

import os
import json
import logging
import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime
from dotenv import load_dotenv

import httpx
from openai import OpenAI

logger = logging.getLogger(__name__)


class DecisionAgentRedis:
    """
    Decision-making agent for automated portfolio management.
    
    Uses MCP server for Alpaca integration.
    """
    
    def __init__(
        self,
        openai_api_key: str,
        mcp_server_url: str = "http://localhost:8000",
        redis_url: str = "redis://localhost:6379"
    ):
        """
        Initialize Decision Agent with actual MCP integration.
        
        Args:
            openai_api_key: OpenAI API key for LLM decisions
            mcp_server_url: URL of the MCP server for Alpaca operations (e.g., http://localhost:8000)
            redis_url: Redis connection URL
            
        Raises:
            ValueError: If MCP server is not accessible
        """
        load_dotenv()
        
        # Initialize OpenAI
        self.openai_api_key = openai_api_key
        self.openai_client = OpenAI(api_key=openai_api_key)
        
        # MCP HTTP client setup
        self.mcp_server_url = mcp_server_url.rstrip('/')
        self.mcp_client = None
        
        self.redis_url = redis_url
        
        logger.info("✅ Decision Agent (MCP HTTP) initialized")
        logger.info(f"   MCP Server: {self.mcp_server_url}")
        logger.info(f"   LLM: GPT-4")
    
    async def _ensure_mcp_client(self):
        """Ensure MCP HTTP client is initialized and server is accessible."""
        if self.mcp_client is None:
            try:
                self.mcp_client = httpx.AsyncClient(base_url=self.mcp_server_url, timeout=30.0)
                
                # Health check
                response = await self.mcp_client.get("/health")
                response.raise_for_status()
                logger.info(f"✅ Connected to MCP server at {self.mcp_server_url}")
                
            except Exception as e:
                logger.error(f"❌ Failed to connect to MCP server: {e}")
                raise RuntimeError(
                    f"MCP server is not accessible at {self.mcp_server_url}. "
                    f"Make sure it's running: python -m src.mcp_server.http_server"
                )
    
    async def fetch_portfolio(self) -> Dict[str, Any]:
        """
        Fetch current portfolio state via MCP HTTP server.
        
        Returns:
            Dict with cash, buying_power, positions, and equity
        """
        try:
            await self._ensure_mcp_client()
            
            response = await self.mcp_client.post("/tools/get_portfolio", json={})
            response.raise_for_status()
            
            result = response.json()
            portfolio = result.get("data", {})
            
            logger.info(f"✅ Portfolio fetched via MCP: ${portfolio['cash']:.2f} cash, "
                       f"{len(portfolio['positions'])} positions")
            
            if portfolio['positions']:
                logger.info(f"   Current Holdings:")
                for pos in portfolio['positions']:
                    pl_pct = pos['unrealized_plpc'] * 100
                    logger.info(f"      {pos['symbol']}: {pos['qty']:.0f} shares @ ${pos['current_price']:.2f} "
                               f"(P/L: {pl_pct:+.2f}%)")
            else:
                logger.info(f"   No current holdings")
            
            return portfolio
            
        except Exception as e:
            logger.error(f"❌ Failed to fetch portfolio via MCP: {e}", exc_info=True)
            logger.error(f"💡 Make sure MCP server is running at {self.mcp_server_url}")
            return {
                "cash": 0,
                "buying_power": 0,
                "portfolio_value": 0,
                "equity": 0,
                "positions": []
            }
    
    async def get_current_price(self, ticker: str) -> Optional[float]:
        """Get current market price via MCP HTTP server."""
        try:
            await self._ensure_mcp_client()
            
            response = await self.mcp_client.post("/tools/get_price", json={"ticker": ticker})
            response.raise_for_status()
            
            result = response.json()
            price = result.get("data")
            logger.debug(f"📊 Price for {ticker}: ${price:.2f} (via MCP)")
            return price
                
        except Exception as e:
            logger.error(f"❌ Could not get price for {ticker} via MCP: {e}")
            logger.error(f"💡 Make sure MCP server is running at {self.mcp_server_url}")
            return None
    
    def llm_decision_making(
        self,
        portfolio: Dict[str, Any],
        recommendations: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Use LLM to determine trade decisions based on portfolio and recommendations.
        
        Args:
            portfolio: Current portfolio state from Alpaca
            recommendations: List of stock recommendations from pipeline
            
        Returns:
            List of trade decisions with action, quantity, and reasoning
        """
        prompt = f"""
You are an expert portfolio manager making FINAL trading decisions based on algorithmic recommendations.

CURRENT PORTFOLIO:
{json.dumps(portfolio, indent=2)}

STOCK RECOMMENDATIONS (from pipeline):
{json.dumps(recommendations, indent=2)}

IMPORTANT CONTEXT:
- These stocks are PRE-APPROVED (FinRL and Validator both agreed on BUY/SELL)
- 'finrl_shares' is the quantity suggested by FinRL's quantitative model
- 'validator_confidence' is the confidence from qualitative agent analysis (0-1)
- 'aligned=true' means both systems agreed on the action
- You can see current positions in the portfolio (symbol, qty, current_price, P/L)

YOUR TASK:
Determine the FINAL quantity to trade for each stock based on:
1. FinRL's suggested quantity (finrl_shares)
2. Validator's confidence level (validator_confidence)
3. Current portfolio state (cash available, existing positions)
4. Position sizing and diversification

TRADING RULES:
1. For BUY orders:
   - Start with FinRL's suggested quantity (finrl_shares)
   - Adjust based on validator confidence:
     * High confidence (>0.9): Use 100% of finrl_shares
     * Medium confidence (0.7-0.9): Use 70-80% of finrl_shares
     * Lower confidence (0.6-0.7): Use 50-60% of finrl_shares or skip
   - If stock is ALREADY HELD: Consider if you want to add to position or skip
   
2. For SELL orders:
   - ONLY sell if stock is currently held in portfolio
   - Check current qty in positions before deciding
   - Can sell partial or full position based on confidence
   
3. Risk Management:
   - Maximum 10% of available cash per BUY
   - Maximum $10,000 per trade
   - Don't exceed 20% of total portfolio value in any single stock
   - Consider existing positions when calculating limits
   
4. Portfolio Awareness:
   - Review current holdings before trading
   - Avoid over-concentration in one stock
   - Consider P/L of existing positions when deciding to add/sell

OUTPUT FORMAT (JSON only, no markdown):
[
    {{
        "ticker": "AAPL",
        "action": "BUY",
        "quantity": 10,
        "confidence": 0.95,
        "estimated_cost": 2770.00,
        "reason": "Validator confidence 95%, FinRL suggested 10 shares, using full amount based on strong alignment"
    }}
]

Generate trading decisions NOW:
"""

        try:
            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a professional portfolio manager. Output ONLY valid JSON, no markdown formatting."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
                max_tokens=2000
            )
            
            response_text = response.choices[0].message.content.strip()
            
            # Remove markdown code blocks if present
            if response_text.startswith("```"):
                response_text = response_text.split("```")[1]
                if response_text.startswith("json"):
                    response_text = response_text[4:]
                response_text = response_text.strip()
            
            decisions = json.loads(response_text)
            
            logger.info(f"✅ LLM generated {len(decisions)} trade decisions")
            
            return decisions
            
        except json.JSONDecodeError as e:
            logger.error(f"❌ Failed to parse LLM response: {e}")
            logger.error(f"Response was: {response_text[:500]}")
            return []
        except Exception as e:
            logger.error(f"❌ LLM decision making failed: {e}")
            return []
    
    async def execute_trade(self, trade: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a single trade via MCP HTTP server.
        
        Args:
            trade: Trade decision with ticker, action, quantity
            
        Returns:
            Trade result with status, order_id, and details
        """
        ticker = trade['ticker']
        action = trade['action'].upper()
        quantity = int(trade['quantity'])
        
        if action == 'HOLD':
            return {
                "ticker": ticker,
                "action": "HOLD",
                "status": "skipped",
                "message": "No action taken"
            }
        
        try:
            # Get current price for validation
            current_price = await self.get_current_price(ticker)
            if not current_price:
                return {
                    "ticker": ticker,
                    "action": action,
                    "status": "failed",
                    "message": "Could not get current price"
                }
            
            await self._ensure_mcp_client()
            
            # Make HTTP call to MCP server
            response = await self.mcp_client.post(
                "/tools/submit_order",
                json={
                    "ticker": ticker,
                    "side": action.lower(),
                    "quantity": quantity
                }
            )
            response.raise_for_status()
            
            order_result = response.json().get("data", {})
            
            result = {
                "ticker": ticker,
                "action": action,
                "quantity": quantity,
                "status": order_result.get("status", "submitted"),
                "order_id": order_result.get("order_id"),
                "estimated_price": current_price,
                "estimated_value": current_price * quantity,
                "timestamp": datetime.now().isoformat(),
                "message": f"{action} order submitted successfully"
            }
            
            logger.info(f"✅ {action} {quantity} shares of {ticker} @ ${current_price:.2f} (via MCP)")
            return result
            
        except Exception as e:
            logger.error(f"❌ Failed to execute {action} for {ticker} via MCP: {e}")
            logger.error(f"💡 Make sure MCP server is running at {self.mcp_server_url}")
            return {
                "ticker": ticker,
                "action": action,
                "status": "failed",
                "message": f"MCP error: {str(e)}"
            }
    
    async def execute_all_trades(
        self,
        trade_decisions: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Execute all trade decisions via MCP HTTP bulk orders.
        
        Args:
            trade_decisions: List of trade decisions from LLM
            
        Returns:
            List of trade results
        """
        # Filter out HOLD decisions
        active_trades = [t for t in trade_decisions if t['action'] != 'HOLD']
        hold_trades = [t for t in trade_decisions if t['action'] == 'HOLD']
        
        results = []
        
        # Log HOLD decisions
        for trade in hold_trades:
            logger.info(f"⏸️  HOLD {trade['ticker']} - {trade.get('reason', 'No reason')}")
            results.append({
                "ticker": trade['ticker'],
                "action": "HOLD",
                "status": "skipped"
            })
        
        # Execute active trades
        if active_trades:
            if len(active_trades) > 1:
                # Use bulk orders via MCP HTTP
                try:
                    await self._ensure_mcp_client()
                    bulk_orders = [
                        {"ticker": t['ticker'], "side": t['action'], "quantity": int(t['quantity'])}
                        for t in active_trades
                    ]
                    
                    response = await self.mcp_client.post(
                        "/tools/submit_bulk_orders",
                        json={"orders": bulk_orders}
                    )
                    response.raise_for_status()
                    
                    bulk_results = response.json().get("data", [])
                    results.extend(bulk_results)
                    
                except Exception as e:
                    logger.error(f"❌ Bulk order via MCP failed: {e}")
                    logger.error(f"💡 Make sure MCP server is running at {self.mcp_server_url}")
                    # Execute one by one as fallback
                    for trade in active_trades:
                        result = await self.execute_trade(trade)
                        results.append(result)
            else:
                # Execute single trade
                for trade in active_trades:
                    result = await self.execute_trade(trade)
                    results.append(result)
        
        # Summary
        successful = len([r for r in results if r['status'] == 'submitted'])
        failed = len([r for r in results if r['status'] == 'failed'])
        skipped = len([r for r in results if r['status'] == 'skipped'])
        
        logger.info(f"📊 Trade Execution Summary:")
        logger.info(f"   ✅ Successful: {successful}")
        logger.info(f"   ❌ Failed: {failed}")
        logger.info(f"   ⏸️  Skipped: {skipped}")
        
        return results
    
    async def make_decisions_and_execute(
        self,
        pipeline_output: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Main method: Take pipeline output, make decisions, execute trades.
        
        Args:
            pipeline_output: Output from the complete pipeline
            
        Returns:
            Complete decision and execution report
        """
        logger.info("=" * 80)
        logger.info("💼 DECISION AGENT: Making Portfolio Decisions")
        logger.info("=" * 80)
        
        # Extract recommendations
        recommendations = pipeline_output.get('approved_stocks', [])
        
        if not recommendations:
            logger.warning("⚠️  No approved stocks to trade")
            return {
                "timestamp": datetime.now().isoformat(),
                "status": "no_recommendations",
                "message": "No approved stocks from pipeline"
            }
        
        logger.info(f"📊 Received {len(recommendations)} recommendations")
        
        # Fetch current portfolio
        portfolio = await self.fetch_portfolio()
        
        # Get LLM decisions
        trade_decisions = self.llm_decision_making(portfolio, recommendations)
        
        if not trade_decisions:
            logger.warning("⚠️  LLM did not generate any trade decisions")
            return {
                "timestamp": datetime.now().isoformat(),
                "status": "no_decisions",
                "message": "LLM did not generate trade decisions"
            }
        
        # Execute trades
        trade_results = await self.execute_all_trades(trade_decisions)
        
        # Fetch updated portfolio
        updated_portfolio = await self.fetch_portfolio()
        
        # Prepare final report
        report = {
            "timestamp": datetime.now().isoformat(),
            "status": "completed",
            "initial_portfolio": portfolio,
            "recommendations": recommendations,
            "trade_decisions": trade_decisions,
            "trade_results": trade_results,
            "final_portfolio": updated_portfolio,
            "summary": {
                "total_recommendations": len(recommendations),
                "total_decisions": len(trade_decisions),
                "trades_executed": len([r for r in trade_results if r['status'] == 'submitted']),
                "trades_failed": len([r for r in trade_results if r['status'] == 'failed']),
                "trades_skipped": len([r for r in trade_results if r['status'] == 'skipped'])
            }
        }
        
        logger.info("=" * 80)
        logger.info("✅ Decision Agent Complete")
        logger.info("=" * 80)
        
        return report
    
    def save_report(
        self,
        report: Dict[str, Any],
        output_dir: str = "reports/trades"
    ) -> str:
        """Save decision report to JSON file."""
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = os.path.join(output_dir, f"trade_report_{timestamp}.json")
        
        with open(filepath, 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"💾 Trade report saved: {filepath}")
        
        return filepath


# Standalone execution for testing
async def main():
    """Test the decision agent with sample pipeline output."""
    from pathlib import Path
    
    # Load most recent pipeline output
    reports_dir = Path(__file__).parent.parent.parent / "reports"
    complete_test_files = sorted(reports_dir.glob("complete_test_*.json"))
    
    if not complete_test_files:
        print("❌ No pipeline output found in reports/")
        return
    
    latest_output = complete_test_files[-1]
    
    print(f"📊 Loading pipeline output: {latest_output.name}")
    
    with open(latest_output, 'r') as f:
        pipeline_output = json.load(f)
    
    # Initialize decision agent
    openai_api_key = os.getenv("OPENAI_API_KEY")
    mcp_server_url = os.getenv("MCP_SERVER_URL", "http://localhost:8000")
    
    agent = DecisionAgentRedis(openai_api_key=openai_api_key, mcp_server_url=mcp_server_url)
    
    try:
        # Make decisions and execute trades
        report = await agent.make_decisions_and_execute(pipeline_output)
        
        # Save report
        filepath = agent.save_report(report)
        
        print(f"\n✅ Trade execution complete!")
        print(f"📄 Report: {filepath}")
    finally:
        # Clean up HTTP client
        if agent.mcp_client:
            await agent.mcp_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
