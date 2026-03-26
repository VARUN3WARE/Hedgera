"""
Enhanced AEGIS Trading Pipeline

Complete workflow:
1. FinRL gives BUY/SELL signals with ticker list
2. Generate fake market/technical data
3. Fetch real news from NewsAPI + synthetic social media data
4. Run all 4 agents (News, Social, Market, SEC)
5. Run debate & validation for each ticker
"""

import asyncio
import json
import logging
import os
import redis
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

# Import AEGIS components
from backend.config.settings import settings
from backend.src.producers.news_producer_impl import NewsProducer
from backend.src.producers.social_producer_impl import SocialProducer
from backend.src.consumers.master_state_consumer import MasterStateConsumer
from backend.src.agents_redis.news_analyst_redis import NewsAnalystRedis
from backend.src.agents_redis.social_analyst_redis import SocialAnalystRedis
from backend.src.agents_redis.market_analyst_redis import MarketAnalystRedis
from backend.src.agents_redis.sec_report_analyst_redis import SecReportAnalystRedis
from backend.src.agents_redis.debate import run_debate
from backend.src.agents_redis.decision_agent_redis_mcp import DecisionAgentRedis


class EnhancedPipeline:
    """Complete AEGIS pipeline orchestrator"""
    
    def __init__(self, test_tickers: List[str] = None):
        """
        Initialize pipeline with test tickers or use FinRL output
        
        Args:
            test_tickers: Optional list of tickers for testing (e.g., ['AAPL', 'MSFT'])
        """
        # Setup logging directory
        self.base_log_dir = Path("agent_logs")
        self.base_log_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.cycle_log_dir = self.base_log_dir / timestamp
        self.cycle_log_dir.mkdir(exist_ok=True)
        
        # Setup logger
        self.logger = self._setup_logger()
        
        # FinRL output (fake for now, replace with actual FinRL later)
        self.finrl_output = {
            "buy": {"AAPL": 25, "MSFT": 15},
            "sell": {"NVDA": 30},
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # Use test tickers if provided, otherwise extract from FinRL
        if test_tickers:
            self.selected_tickers = test_tickers
        else:
            self.selected_tickers = list(self.finrl_output["buy"].keys())
        
        self.logger.info(f"Pipeline initialized with tickers: {self.selected_tickers}")
        
        # Redis client
        self.redis_client = None
    
    def _setup_logger(self):
        """Setup logging for this pipeline run"""
        logger = logging.getLogger("EnhancedPipeline")
        logger.setLevel(logging.INFO)
        
        # Remove existing handlers
        logger.handlers = []
        
        # File handler
        fh = logging.FileHandler(self.cycle_log_dir / "pipeline.log")
        fh.setLevel(logging.INFO)
        
        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        
        logger.addHandler(fh)
        logger.addHandler(ch)
        
        return logger
    
    def _save_json_log(self, filename: str, data: Any):
        """Save data to JSON file in cycle log directory"""
        filepath = self.cycle_log_dir / filename
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        self.logger.info(f"Saved {filename}")
    
    async def setup_redis(self):
        """Setup Redis connection"""
        try:
            self.redis_client = redis.Redis(
                host=settings.redis_host,
                port=settings.redis_port,
                db=0,
                decode_responses=True
            )
            self.redis_client.ping()
            self.logger.info("✅ Redis connected")
        except Exception as e:
            self.logger.error(f"❌ Redis connection failed: {e}")
            raise
    
    async def step1_log_finrl_output(self):
        """Step 1: Log FinRL output"""
        self.logger.info("=" * 60)
        self.logger.info("STEP 1: FinRL Output")
        self.logger.info("=" * 60)
        
        self.logger.info(f"Buy signals: {self.finrl_output['buy']}")
        self.logger.info(f"Sell signals: {self.finrl_output['sell']}")
        self.logger.info(f"Selected tickers for processing: {self.selected_tickers}")
        
        # Save to log
        self._save_json_log("01_finrl_output.json", self.finrl_output)
    
    async def step2_produce_fake_market_data(self):
        """Step 2: Produce fake market/technical data for selected tickers"""
        self.logger.info("=" * 60)
        self.logger.info("STEP 2: Generating Fake Market Data")
        self.logger.info("=" * 60)
        
        import random
        
        fake_market_data = {}
        
        for ticker in self.selected_tickers:
            base_price = random.uniform(100, 300)
            fake_market_data[ticker] = {
                'open': round(base_price * 0.99, 2),
                'high': round(base_price * 1.02, 2),
                'low': round(base_price * 0.98, 2),
                'close': round(base_price, 2),
                'volume': random.randint(500000, 5000000),
                'macd': round(random.uniform(-2, 2), 2),
                'boll_ub': round(base_price * 1.05, 2),
                'boll_lb': round(base_price * 0.95, 2),
                'rsi_30': round(random.uniform(30, 70), 1),
                'cci_30': round(random.uniform(-100, 100), 1),
                'dx_30': round(random.uniform(10, 40), 1),
                'close_30_sma': round(base_price * 0.99, 2),
                'close_60_sma': round(base_price * 0.98, 2),
                'VIXY': round(random.uniform(15, 25), 1)
            }
        
        # Save to Redis under a key for market data
        for ticker, data in fake_market_data.items():
            key = f"market_data:{ticker}"
            self.redis_client.set(key, json.dumps(data))
            self.logger.info(f"Saved market data to Redis: {key}")
        
        self._save_json_log("02_fake_market_data.json", fake_market_data)
        return fake_market_data
    
    async def step3_fetch_news_social(self):
        """Step 3: Fetch news and social data for selected tickers"""
        self.logger.info("=" * 60)
        self.logger.info("STEP 3: Fetching News & Social Data")
        self.logger.info("=" * 60)
        
        # Initialize producers
        news_producer = NewsProducer()
        social_producer = SocialProducer()
        
        # Set active symbols
        news_producer.set_active_symbols(self.selected_tickers)
        social_producer.set_active_symbols(self.selected_tickers)
        
        # Initialize
        await news_producer.initialize()
        await social_producer.initialize()
        
        # Fetch and publish data for each ticker (multiple posts per ticker)
        news_per_ticker = 2  # Generate 2 news articles per ticker
        posts_per_ticker = 3  # Generate 3 social posts per ticker
        
        for ticker in self.selected_tickers:
            self.logger.info(f"Fetching data for {ticker}...")
            
            # Fetch multiple news articles per ticker
            for i in range(news_per_ticker):
                news_result = await news_producer.fetch_data()
                if news_result:
                    # Publish to Redis stream using internal method
                    await news_producer._publish(news_result)
            
            # Fetch multiple social posts per ticker
            for i in range(posts_per_ticker):
                social_result = await social_producer.fetch_data()
                if social_result:
                    # Publish to Redis stream using internal method
                    await social_producer._publish(social_result)
        
        # Cleanup producers
        await news_producer.cleanup()
        await social_producer.cleanup()
        
        # Now use consumer to fetch all data from Redis
        self.logger.info("Using consumer to fetch data from Redis...")
        consumer = MasterStateConsumer(
            redis_url=f"redis://{settings.redis_host}:{settings.redis_port}/0",
            logger=self.logger
        )
        
        await consumer.connect()
        
        # Consume all data for selected tickers
        consumed_data = await consumer.consume_all_for_tickers(self.selected_tickers)
        
        await consumer.disconnect()
        
        # Extract news and social data
        news_data = {ticker: data["news"] for ticker, data in consumed_data.items()}
        social_data = {ticker: data["social"] for ticker, data in consumed_data.items()}
        
        # Save logs
        self._save_json_log("03_news_data.json", news_data)
        self._save_json_log("04_social_data.json", social_data)
        
        return news_data, social_data
    
    async def step4_run_agents(self, fake_market_data: Dict, news_data: Dict, social_data: Dict):
        """Step 4: Run all agents on each ticker"""
        self.logger.info("=" * 60)
        self.logger.info("STEP 4: Running Agents")
        self.logger.info("=" * 60)
        
        all_agent_results = {}
        
        # Get OpenAI API key
        openai_api_key = os.getenv("OPENAI_API_KEY")
        if not openai_api_key:
            self.logger.warning("⚠️ OPENAI_API_KEY not set, agents may fail")
        
        redis_url = f"redis://{settings.redis_host}:{settings.redis_port}/0"
        
        for ticker in self.selected_tickers:
            self.logger.info(f"\n{'='*40}")
            self.logger.info(f"Processing ticker: {ticker}")
            self.logger.info(f"{'='*40}")
            
            ticker_results = {
                "ticker": ticker,
                "timestamp": datetime.now().isoformat()
            }
            
            # Get ticker-specific data
            ticker_news = news_data.get(ticker, [])
            ticker_social = social_data.get(ticker, [])
            ticker_market = fake_market_data.get(ticker, {})
            
            # 1. News Analyst
            try:
                self.logger.info(f"[{ticker}] Running News Analyst...")
                news_agent = NewsAnalystRedis(
                    redis_url=redis_url,
                    stream_key="raw:news-articles",
                    openai_api_key=openai_api_key,
                    logger=self.logger
                )
                
                await news_agent.connect()
                
                # Populate buffer with news from consumer
                if ticker_news:
                    news_agent.news_buffer[ticker] = ticker_news
                    self.logger.info(f"[{ticker}] Loaded {len(ticker_news)} news items from consumer")
                else:
                    # Create dummy news if no data
                    news_agent.news_buffer[ticker] = [
                        {
                            "ticker": ticker,
                            "title": f"{ticker} shows strong performance",
                            "description": f"Latest analysis indicates {ticker} continues to perform well",
                            "sentiment": 0.5,
                            "timestamp": datetime.now().isoformat()
                        }
                    ]
                    self.logger.info(f"[{ticker}] Using dummy news data")
                
                news_analysis = await news_agent.analyze_news_sentiment(ticker)
                await news_agent.disconnect()
                
                ticker_results["news_analysis"] = news_analysis
                self.logger.info(f"[{ticker}] News analysis complete")
                
            except Exception as e:
                self.logger.error(f"[{ticker}] News analyst error: {e}")
                ticker_results["news_analysis"] = {"error": str(e)}
            
            # 2. Social Analyst
            try:
                self.logger.info(f"[{ticker}] Running Social Analyst...")
                social_agent = SocialAnalystRedis(
                    redis_url=redis_url,
                    stream_key="raw:social",
                    openai_api_key=openai_api_key,
                    logger=self.logger
                )
                
                await social_agent.connect()
                
                # Populate buffer with social data from consumer
                if ticker_social:
                    social_agent.social_buffer[ticker] = ticker_social
                    self.logger.info(f"[{ticker}] Loaded {len(ticker_social)} social items from consumer")
                else:
                    # Create dummy social data if no data
                    social_agent.social_buffer[ticker] = [
                        {
                            "ticker": ticker,
                            "text": f"Investors are optimistic about {ticker}",
                            "sentiment": 0.5,
                            "likes": 150,
                            "comments": 75,
                            "shares": 30,
                            "timestamp": datetime.now().isoformat()
                        }
                    ]
                    self.logger.info(f"[{ticker}] Using dummy social data")
                
                social_analysis = await social_agent.analyze_social_sentiment(ticker)
                await social_agent.disconnect()
                
                ticker_results["social_analysis"] = social_analysis
                self.logger.info(f"[{ticker}] Social analysis complete")
                
            except Exception as e:
                self.logger.error(f"[{ticker}] Social analyst error: {e}")
                ticker_results["social_analysis"] = {"error": str(e)}
            
            # 3. Market Analyst (using fake technical data)
            try:
                self.logger.info(f"[{ticker}] Running Market Analyst...")
                market_agent = MarketAnalystRedis(
                    redis_url=redis_url,
                    stream_key="price_stream",
                    openai_api_key=openai_api_key,
                    logger=self.logger
                )
                
                await market_agent.connect()
                
                # Populate with fake market data
                market_agent.price_buffer[ticker] = [ticker_market]
                self.logger.info(f"[{ticker}] Loaded market data from fake producer")
                
                market_analysis = await market_agent.analyze_market_data(ticker)
                await market_agent.disconnect()
                
                ticker_results["market_analysis"] = market_analysis
                self.logger.info(f"[{ticker}] Market analysis complete")
                
            except Exception as e:
                self.logger.error(f"[{ticker}] Market analyst error: {e}")
                ticker_results["market_analysis"] = {"error": str(e)}
            
            # 4. SEC Analyst
            try:
                self.logger.info(f"[{ticker}] Running SEC Analyst...")
                sec_agent = SecReportAnalystRedis(
                    redis_url=redis_url,
                    stream_key="sec_stream",
                    openai_api_key=openai_api_key,
                    logger=self.logger
                )
                
                await sec_agent.connect()
                
                # Fetch and analyze SEC data
                sec_analysis = await sec_agent.fetch_and_analyze_ticker(ticker)
                await sec_agent.disconnect()
                
                ticker_results["sec_analysis"] = sec_analysis
                self.logger.info(f"[{ticker}] SEC analysis complete")
                
            except Exception as e:
                self.logger.error(f"[{ticker}] SEC analyst error: {e}")
                ticker_results["sec_analysis"] = {"error": str(e)}
            
            # Save ticker results
            all_agent_results[ticker] = ticker_results
            self._save_json_log(f"05_agent_results_{ticker}.json", ticker_results)
        
        # Save combined results
        self._save_json_log("06_all_agent_results.json", all_agent_results)
        
        return all_agent_results
    
    async def step5_run_debate(self, agent_results: Dict):
        """Step 5: Run debate for each ticker"""
        self.logger.info("=" * 60)
        self.logger.info("STEP 5: Running Debate & Validation")
        self.logger.info("=" * 60)
        
        debate_results = {}
        
        for ticker in self.selected_tickers:
            self.logger.info(f"\n{'='*40}")
            self.logger.info(f"Debate for: {ticker}")
            self.logger.info(f"{'='*40}")
            
            try:
                # Prepare data for debate
                ticker_data = agent_results.get(ticker, {})
                
                # Format as expected by debate
                final_reports_data = {
                    "ticker": ticker,
                    "agents": [
                        {
                            "agent": "market_analyst",
                            "output": json.dumps(ticker_data.get("market_analysis", {}))
                        },
                        {
                            "agent": "social_media_analyst",
                            "output": json.dumps(ticker_data.get("social_analysis", {}))
                        },
                        {
                            "agent": "news_analyst",
                            "output": json.dumps(ticker_data.get("news_analysis", {}))
                        },
                        {
                            "agent": "sec_report_analyst",
                            "output": json.dumps(ticker_data.get("sec_analysis", {}))
                        }
                    ]
                }
                
                # Run debate
                debate_result = run_debate(final_reports_data=final_reports_data)
                debate_results[ticker] = debate_result
                
                # Extract final recommendation from validation
                final_rec = debate_result.get('validation', {}).get('final_recommendation', {})
                self.logger.info(f"[{ticker}] Debate complete")
                self.logger.info(f"[{ticker}] Decision: {final_rec.get('decision', 'N/A')} (Conviction: {final_rec.get('conviction', 'N/A')})")
                
            except Exception as e:
                self.logger.error(f"[{ticker}] Debate error: {e}")
                debate_results[ticker] = {"error": str(e)}
        
        # Save debate results
        self._save_json_log("07_debate_results.json", debate_results)
        
        return debate_results
    
    async def step6_reconcile_and_decide(self, debate_results: Dict):
        """
        Step 6: Reconcile FinRL output with Validator decisions
        
        Only accept tickers where BOTH FinRL and Validator agree on the action.
        If they contradict, reject that ticker.
        
        Returns approved stocks with aligned recommendations.
        """
        self.logger.info("=" * 60)
        self.logger.info("STEP 6: Reconciling FinRL & Validator Decisions")
        self.logger.info("=" * 60)
        
        approved_stocks = []
        rejected_stocks = []
        
        # Get all tickers from FinRL (both BUY and SELL)
        finrl_buy_tickers = set(self.finrl_output.get("buy", {}).keys())
        finrl_sell_tickers = set(self.finrl_output.get("sell", {}).keys())
        
        for ticker in self.selected_tickers:
            self.logger.info(f"\n{'='*40}")
            self.logger.info(f"Reconciling: {ticker}")
            self.logger.info(f"{'='*40}")
            
            # Get FinRL action and shares
            finrl_action = None
            finrl_shares = 0
            
            if ticker in finrl_buy_tickers:
                finrl_action = "BUY"
                finrl_shares = self.finrl_output["buy"][ticker]
            elif ticker in finrl_sell_tickers:
                finrl_action = "SELL"
                finrl_shares = self.finrl_output["sell"][ticker]
            else:
                finrl_action = "HOLD"
                finrl_shares = 0
            
            # Get Validator decision
            debate_result = debate_results.get(ticker, {})
            validator_decision = debate_result.get('validation', {}).get('final_recommendation', {})
            validator_action = validator_decision.get('decision', 'HOLD')
            validator_confidence = validator_decision.get('conviction', 0)
            
            # Normalize actions for comparison
            finrl_action_normalized = finrl_action.upper()
            validator_action_normalized = validator_action.upper()
            
            self.logger.info(f"[{ticker}] FinRL: {finrl_action_normalized} ({finrl_shares} shares)")
            self.logger.info(f"[{ticker}] Validator: {validator_action_normalized} (Conviction: {validator_confidence})")
            
            # Check if they align
            aligned = finrl_action_normalized == validator_action_normalized
            
            if aligned and finrl_action_normalized != "HOLD":
                # Both agree on BUY or SELL - APPROVED
                approved_stocks.append({
                    "ticker": ticker,
                    "action": finrl_action_normalized,
                    "finrl_shares": finrl_shares,
                    "validator_confidence": validator_confidence,
                    "aligned": True,
                    "reason": f"FinRL and Validator both recommend {finrl_action_normalized}"
                })
                self.logger.info(f"[{ticker}] ✅ APPROVED: Both systems agree on {finrl_action_normalized}")
                
            elif not aligned:
                # Contradiction - REJECTED
                rejected_stocks.append({
                    "ticker": ticker,
                    "finrl_action": finrl_action_normalized,
                    "validator_action": validator_action_normalized,
                    "aligned": False,
                    "reason": f"Contradiction: FinRL says {finrl_action_normalized}, Validator says {validator_action_normalized}"
                })
                self.logger.warning(f"[{ticker}] ❌ REJECTED: Contradiction detected")
                
            else:
                # Both say HOLD - REJECTED (no action needed)
                rejected_stocks.append({
                    "ticker": ticker,
                    "finrl_action": finrl_action_normalized,
                    "validator_action": validator_action_normalized,
                    "aligned": True,
                    "reason": "Both systems recommend HOLD (no action)"
                })
                self.logger.info(f"[{ticker}] ⏸️  REJECTED: Both recommend HOLD")
        
        # Log summary
        self.logger.info("\n" + "=" * 60)
        self.logger.info("RECONCILIATION SUMMARY")
        self.logger.info("=" * 60)
        self.logger.info(f"✅ Approved: {len(approved_stocks)}")
        self.logger.info(f"❌ Rejected: {len(rejected_stocks)}")
        
        if approved_stocks:
            self.logger.info("\nApproved Stocks:")
            for stock in approved_stocks:
                self.logger.info(f"  • {stock['ticker']}: {stock['action']} "
                               f"({stock['finrl_shares']} shares, "
                               f"confidence: {stock['validator_confidence']})")
        
        if rejected_stocks:
            self.logger.info("\nRejected Stocks:")
            for stock in rejected_stocks:
                self.logger.info(f"  • {stock['ticker']}: {stock['reason']}")
        
        # Save reconciliation results
        reconciliation_results = {
            "timestamp": datetime.now().isoformat(),
            "approved_stocks": approved_stocks,
            "rejected_stocks": rejected_stocks,
            "summary": {
                "total_tickers": len(self.selected_tickers),
                "approved_count": len(approved_stocks),
                "rejected_count": len(rejected_stocks)
            }
        }
        self._save_json_log("08_reconciliation.json", reconciliation_results)
        
        return reconciliation_results
    
    async def step7_execute_trades(self, reconciliation_results: Dict):
        """
        Step 7: Execute trades for approved stocks using Decision Agent MCP
        
        Args:
            reconciliation_results: Results from step 6 with approved stocks
            
        Returns:
            Trade execution results
        """
        self.logger.info("=" * 60)
        self.logger.info("STEP 7: Executing Trades via Decision Agent MCP")
        self.logger.info("=" * 60)
        
        approved_stocks = reconciliation_results.get("approved_stocks", [])
        
        if not approved_stocks:
            self.logger.warning("⚠️  No approved stocks to trade")
            return {
                "timestamp": datetime.now().isoformat(),
                "status": "no_trades",
                "message": "No approved stocks from reconciliation"
            }
        
        try:
            # Initialize Decision Agent with MCP
            openai_api_key = os.getenv("OPENAI_API_KEY")
            
            decision_agent = DecisionAgentRedis(
                openai_api_key=openai_api_key,
                mcp_server_url="http://localhost:8000",
                redis_url=f"redis://{settings.redis_host}:{settings.redis_port}/0"
            )
            
            # Format approved stocks for decision agent
            pipeline_output = {
                "approved_stocks": approved_stocks,
                "timestamp": datetime.now().isoformat()
            }
            
            # Make decisions and execute trades
            trade_report = await decision_agent.make_decisions_and_execute(pipeline_output)
            
            # Save trade report
            self._save_json_log("09_trade_execution.json", trade_report)
            
            # Log summary
            summary = trade_report.get("summary", {})
            self.logger.info("\n" + "=" * 60)
            self.logger.info("TRADE EXECUTION SUMMARY")
            self.logger.info("=" * 60)
            self.logger.info(f"Total Recommendations: {summary.get('total_recommendations', 0)}")
            self.logger.info(f"Total Decisions: {summary.get('total_decisions', 0)}")
            self.logger.info(f"✅ Trades Executed: {summary.get('trades_executed', 0)}")
            self.logger.info(f"❌ Trades Failed: {summary.get('trades_failed', 0)}")
            self.logger.info(f"⏸️  Trades Skipped: {summary.get('trades_skipped', 0)}")
            
            return trade_report
            
        except Exception as e:
            self.logger.error(f"❌ Trade execution failed: {e}", exc_info=True)
            error_report = {
                "timestamp": datetime.now().isoformat(),
                "status": "failed",
                "error": str(e),
                "message": "Trade execution encountered an error"
            }
            self._save_json_log("09_trade_execution_error.json", error_report)
            return error_report
    
    async def run(self):
        """Run the complete pipeline"""
        try:
            self.logger.info("=" * 60)
            self.logger.info("ENHANCED AEGIS PIPELINE STARTED")
            self.logger.info("=" * 60)
            self.logger.info(f"Log directory: {self.cycle_log_dir}")
            
            # Setup
            await self.setup_redis()
            
            # Step 1: Log FinRL output
            await self.step1_log_finrl_output()
            
            # Step 2: Generate fake market data
            fake_market_data = await self.step2_produce_fake_market_data()
            
            # Step 3: Fetch news and social
            news_data, social_data = await self.step3_fetch_news_social()
            
            # Step 4: Run all agents
            agent_results = await self.step4_run_agents(fake_market_data, news_data, social_data)
            
            # Step 5: Run debate
            debate_results = await self.step5_run_debate(agent_results)
            
            # Step 6: Reconcile FinRL & Validator decisions
            reconciliation_results = await self.step6_reconcile_and_decide(debate_results)
            
            # Step 7: Execute trades for approved stocks
            trade_results = await self.step7_execute_trades(reconciliation_results)
            
            # Final summary
            self.logger.info("=" * 60)
            self.logger.info("PIPELINE COMPLETED SUCCESSFULLY")
            self.logger.info("=" * 60)
            self.logger.info(f"Processed {len(self.selected_tickers)} tickers")
            self.logger.info(f"All logs saved to: {self.cycle_log_dir}")
            
            # Create summary
            summary = {
                "pipeline_run": datetime.now().isoformat(),
                "tickers_processed": self.selected_tickers,
                "finrl_output": self.finrl_output,
                "debate_decisions": {
                    ticker: result.get("validation", {}).get("final_recommendation", {})
                    for ticker, result in debate_results.items()
                },
                "reconciliation": {
                    "approved_stocks": reconciliation_results.get("approved_stocks", []),
                    "rejected_stocks": reconciliation_results.get("rejected_stocks", []),
                    "summary": reconciliation_results.get("summary", {})
                },
                "trade_execution": {
                    "status": trade_results.get("status", "unknown"),
                    "summary": trade_results.get("summary", {})
                },
                "log_directory": str(self.cycle_log_dir)
            }
            self._save_json_log("00_SUMMARY.json", summary)
            
            print("\n" + "=" * 60)
            print("✅ PIPELINE RUN COMPLETE")
            print("=" * 60)
            print(f"📁 Logs: {self.cycle_log_dir}")
            print(f"📊 Tickers: {', '.join(self.selected_tickers)}")
            print(f"✅ Approved: {len(reconciliation_results.get('approved_stocks', []))}")
            print(f"❌ Rejected: {len(reconciliation_results.get('rejected_stocks', []))}")
            if trade_results.get("summary"):
                print(f"💼 Trades Executed: {trade_results['summary'].get('trades_executed', 0)}")
            print("=" * 60)
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}", exc_info=True)
            raise


async def main():
    """Main entry point"""
    # Use 2-3 tickers for testing
    test_tickers = ["AAPL", "MSFT"]
    
    pipeline = EnhancedPipeline(test_tickers=test_tickers)
    await pipeline.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Pipeline stopped by user")
    except Exception as e:
        print(f"\n❌ Pipeline error: {e}")
        import traceback
        traceback.print_exc()
