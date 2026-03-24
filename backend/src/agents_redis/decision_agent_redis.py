"""
Decision Agent (Redis-Compatible) - Final Portfolio Management

This agent:
1. Receives ranked stock recommendations from the pipeline
2. Fetches current portfolio from Alpaca Paper Trading
3. Uses LLM (GPT-4) to determine trade quantities based on:
   - Current holdings
   - Available cash
   - Confidence scores
   - Risk management rules
4. Executes trades on Alpaca
5. Logs results to Redis and JSON

Integrates with the optimized pipeline output.
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from dotenv import load_dotenv

from alpaca_trade_api import REST
from openai import OpenAI

logger = logging.getLogger(__name__)


class DecisionAgentRedis:
    """
    Decision-making agent for automated portfolio management.
    
    Takes pipeline output and executes trades based on:
    - Current portfolio state
    - Stock recommendations with confidence scores
    - Risk management rules
    - LLM reasoning
    """
    
    def __init__(
        self,
        openai_api_key: str,
        alpaca_api_key: Optional[str] = None,
        alpaca_secret_key: Optional[str] = None,
        alpaca_base_url: str = "https://paper-api.alpaca.markets",
        redis_url: str = "redis://localhost:6379"
    ):
        """
        Initialize Decision Agent.
        
        Args:
            openai_api_key: OpenAI API key for LLM decisions
            alpaca_api_key: Alpaca API key (defaults to env var)
            alpaca_secret_key: Alpaca secret key (defaults to env var)
            alpaca_base_url: Alpaca API base URL (paper/live trading)
            redis_url: Redis connection URL
        """
        load_dotenv()
        
        # Initialize OpenAI
        self.openai_api_key = openai_api_key
        self.client = OpenAI(api_key=openai_api_key)
        
        # Initialize Alpaca for trading (use paper trading URL)
        self.alpaca_api_key = alpaca_api_key or os.getenv("ALPACA_API_KEY")
        self.alpaca_secret_key = alpaca_secret_key or os.getenv("ALPACA_SECRET_KEY")
        # Use ALPACA_PAPER_BASE_URL for trading, fallback to parameter or default
        self.alpaca_base_url = os.getenv("ALPACA_PAPER_BASE_URL") or alpaca_base_url
        
        if not self.alpaca_api_key or not self.alpaca_secret_key:
            raise ValueError("❌ Alpaca API credentials not found!")
        
        self.alpaca = REST(
            self.alpaca_api_key,
            self.alpaca_secret_key,
            self.alpaca_base_url
        )
        
        self.redis_url = redis_url
        
        logger.info("✅ Decision Agent initialized")
        logger.info(f"   Alpaca: {self.alpaca_base_url}")
        logger.info(f"   LLM: GPT-4")
    
    def fetch_portfolio(self) -> Dict[str, Any]:
        """
        Fetch current portfolio state from Alpaca using direct REST API v2.
        
        Returns:
            Dict with cash, buying_power, positions, and equity
        """
        try:
            import requests
            
            # Use v2 API directly (more stable than v1)
            headers = {
                "APCA-API-KEY-ID": self.alpaca_api_key,
                "APCA-API-SECRET-KEY": self.alpaca_secret_key
            }
            
            # Fetch account info
            account_response = requests.get(
                f"{self.alpaca_base_url}/v2/account",
                headers=headers
            )
            account_response.raise_for_status()
            account = account_response.json()
            
            # Fetch positions
            positions_response = requests.get(
                f"{self.alpaca_base_url}/v2/positions",
                headers=headers
            )
            positions_response.raise_for_status()
            positions = positions_response.json()

            portfolio = {
                "cash": float(account.get('cash', 0)),
                "buying_power": float(account.get('buying_power', account.get('cash', 0))),
                "portfolio_value": float(account.get('portfolio_value', account.get('cash', 0))),
                "equity": float(account.get('equity', account.get('cash', 0))),
                "positions": [
                    {
                        "symbol": pos.get('symbol'),
                        "qty": float(pos.get('qty', 0)),
                        "market_value": float(pos.get('market_value', 0)),
                        "avg_entry_price": float(pos.get('avg_entry_price', 0)),
                        "current_price": float(pos.get('current_price', pos.get('avg_entry_price', 0))),
                        "unrealized_pl": float(pos.get('unrealized_pl', 0)),
                        "unrealized_plpc": float(pos.get('unrealized_plpc', 0))
                    }
                    for pos in positions
                ]
            }
            
            logger.info(f"✅ Portfolio fetched: ${portfolio['cash']:.2f} cash, "
                       f"{len(portfolio['positions'])} positions")
            
            # Log each position for visibility
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
            logger.error(f"❌ Failed to fetch portfolio: {e}", exc_info=True)
            logger.error(f"   Check Alpaca credentials in .env file")
            # Return empty portfolio but with proper structure
            return {
                "cash": 0,
                "buying_power": 0,
                "portfolio_value": 0,
                "equity": 0,
                "positions": []
            }
    
    def get_current_price(self, ticker: str) -> Optional[float]:
        """Get current market price for a ticker using yfinance as reliable fallback."""
        try:
            # Try yfinance first (most reliable for current prices)
            import yfinance as yf
            
            stock = yf.Ticker(ticker)
            info = stock.info
            
            # Try to get current price from different fields
            price = info.get('regularMarketPrice') or info.get('currentPrice') or info.get('previousClose')
            
            if price and price > 0:
                logger.debug(f"📊 Price for {ticker}: ${price:.2f} (via yfinance)")
                return float(price)
            
            # Fallback: get last close from history
            hist = stock.history(period='1d', interval='1m')
            if not hist.empty:
                price = hist['Close'].iloc[-1]
                logger.debug(f"📊 Price for {ticker}: ${price:.2f} (via yfinance history)")
                return float(price)
                
        except ImportError:
            logger.warning(f"⚠️  yfinance not installed, trying Alpaca methods for {ticker}")
        except Exception as e:
            logger.debug(f"⚠️  yfinance failed for {ticker}: {e}, trying Alpaca methods")
        
        # Fallback to Alpaca methods if yfinance fails
        try:
            # Try get_last_trade (most common in older alpaca-trade-api versions)
            if hasattr(self.alpaca, 'get_last_trade'):
                quote = self.alpaca.get_last_trade(ticker)
                if hasattr(quote, 'price'):
                    return float(quote.price)
            
            # Try get_last_quote
            if hasattr(self.alpaca, 'get_last_quote'):
                quote = self.alpaca.get_last_quote(ticker)
                if hasattr(quote, 'askprice') and hasattr(quote, 'bidprice'):
                    return float((quote.askprice + quote.bidprice) / 2)
                elif hasattr(quote, 'askprice'):
                    return float(quote.askprice)
            
            # Try get_barset (older SDK versions)
            if hasattr(self.alpaca, 'get_barset'):
                bars = self.alpaca.get_barset([ticker], 'minute', limit=1)
                if bars and ticker in bars and len(bars[ticker]) > 0:
                    return float(bars[ticker][0].c)
            
            logger.warning(f"⚠️  No supported Alpaca method available for {ticker}")
            return None
            
        except Exception as e:
            logger.warning(f"⚠️  Could not get price for {ticker}: {e}")
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
            response = self.client.chat.completions.create(
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
    
    def execute_trade(self, trade: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a single trade on Alpaca.
        
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
            current_price = self.get_current_price(ticker)
            if not current_price:
                return {
                    "ticker": ticker,
                    "action": action,
                    "status": "failed",
                    "message": "Could not get current price"
                }
            
            # Submit order to Alpaca
            order = self.alpaca.submit_order(
                symbol=ticker,
                qty=quantity,
                side='buy' if action == 'BUY' else 'sell',
                type='market',
                time_in_force='day'
            )
            
            result = {
                "ticker": ticker,
                "action": action,
                "quantity": quantity,
                "status": "submitted",
                "order_id": order.id,
                "estimated_price": current_price,
                "estimated_value": current_price * quantity,
                "timestamp": datetime.now().isoformat(),
                "message": f"{action} order submitted successfully"
            }
            
            logger.info(f"✅ {action} {quantity} shares of {ticker} @ ${current_price:.2f}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Failed to execute {action} for {ticker}: {e}")
            return {
                "ticker": ticker,
                "action": action,
                "status": "failed",
                "message": str(e)
            }
    
    def execute_all_trades(
        self,
        trade_decisions: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Execute all trade decisions.
        
        Args:
            trade_decisions: List of trade decisions from LLM
            
        Returns:
            List of trade results
        """
        results = []
        
        for trade in trade_decisions:
            if trade['action'] == 'HOLD':
                logger.info(f"⏸️  HOLD {trade['ticker']} - {trade.get('reason', 'No reason')}")
                results.append({
                    "ticker": trade['ticker'],
                    "action": "HOLD",
                    "status": "skipped"
                })
                continue
            
            result = self.execute_trade(trade)
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
    
    def make_decisions_and_execute(
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
        portfolio = self.fetch_portfolio()
        
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
        trade_results = self.execute_all_trades(trade_decisions)
        
        # Fetch updated portfolio
        updated_portfolio = self.fetch_portfolio()
        
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
    
    agent = DecisionAgentRedis(openai_api_key=openai_api_key)
    
    # Make decisions and execute trades
    report = agent.make_decisions_and_execute(pipeline_output)
    
    # Save report
    filepath = agent.save_report(report)
    
    print(f"\n✅ Trade execution complete!")
    print(f"📄 Report: {filepath}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
