#!/usr/bin/env python3
"""
Parallel AEGIS Trading Pipeline - Optimized Version

Clean implementation using asyncio.gather for parallel agent processing.
Removes redundant code and maintains only critical logging points.
"""

import asyncio
import json
import logging
import os
import redis
import redis.asyncio
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List
import sys

# ════════════════════════════════════════════════════════════════
# CRITICAL: Load .env file BEFORE importing backend modules
# ════════════════════════════════════════════════════════════════
from dotenv import load_dotenv

project_root = Path(__file__).parent
env_path = project_root / '.env'

if not env_path.exists():
    print(f"❌ ERROR: .env file not found at {env_path}")
    print(f"   Current directory: {os.getcwd()}")
    sys.exit(1)

load_dotenv(dotenv_path=env_path)
print(f"✅ Loaded .env from: {env_path}")

# Verify OpenAI API key is loaded
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    print("❌ ERROR: OPENAI_API_KEY not found in .env file!")
    print("   Please check your .env file contains:")
    print("   OPENAI_API_KEY=sk-...")
    sys.exit(1)

print(f"✅ OpenAI API Key loaded: {OPENAI_API_KEY[:20]}...{OPENAI_API_KEY[-4:]}")
# ════════════════════════════════════════════════════════════════

sys.path.insert(0, str(Path(__file__).parent))

from backend.config.settings import settings
from backend.src.orchestration.pipeline_enhanced import AegisPipelineEnhanced
from backend.src.producers.news_producer_impl import NewsProducer
from backend.src.producers.social_producer_impl import SocialProducer
from backend.src.agents_redis.news_analyst_redis import NewsAnalystRedis
from backend.src.agents_redis.social_analyst_redis import SocialAnalystRedis
from backend.src.agents_redis.market_analyst_redis import MarketAnalystRedis
from backend.src.agents_redis.sec_report_analyst_redis import SecReportAnalystRedis
from backend.src.agents_redis.debate import run_debate
from backend.src.agents_redis.decision_agent_redis_mcp import DecisionAgentRedis
from backend.src.pathway_integration.aggregator import PathwayAggregator


async def run_ticker_agents(
    ticker: str,
    redis_url: str,
    openai_api_key: str,
    news_data: List[Dict],
    social_data: List[Dict],
    market_data: Dict,
    logger: logging.Logger
) -> Dict[str, Any]:
    """
    Run all 4 agents in parallel for a single ticker.
    
    Returns:
        Complete analysis with debate results
    """
    try:
        # Create agent tasks
        async def run_agent(agent_class, stream_key, buffer_key, data):
            try:
                agent = agent_class(
                    redis_url=redis_url,
                    stream_key=stream_key,
                    openai_api_key=openai_api_key,
                    logger=logger
                )
                await agent.connect()
                
                # Set buffer data
                if buffer_key == "news_buffer":
                    agent.news_buffer[ticker] = data or [{"ticker": ticker, "title": f"{ticker} news", "sentiment": 0.5}]
                    result = await agent.analyze_news_sentiment(ticker)
                elif buffer_key == "social_buffer":
                    agent.social_buffer[ticker] = data or [{"ticker": ticker, "text": f"{ticker} social", "sentiment": 0.5}]
                    result = await agent.analyze_social_sentiment(ticker)
                elif buffer_key == "price_buffer":
                    if data:
                        agent.price_buffer[ticker] = [data]
                    result = await agent.analyze_market_data(ticker)
                else:  # SEC
                    result = await agent.fetch_and_analyze_ticker(ticker)
                
                await agent.disconnect()
                return result
            except Exception as e:
                logger.error(f"[{ticker}] {agent_class.__name__} failed: {e}")
                return {"ticker": ticker, "error": str(e)}
        
        # Run all agents in parallel
        news_result, social_result, market_result, sec_result = await asyncio.gather(
            run_agent(NewsAnalystRedis, "raw:news-articles", "news_buffer", news_data),
            run_agent(SocialAnalystRedis, "raw:social", "social_buffer", social_data),
            run_agent(MarketAnalystRedis, "price_stream", "price_buffer", market_data),
            run_agent(SecReportAnalystRedis, "sec_stream", None, None)
        )
        
        # Run debate
        final_reports = {
            "ticker": ticker,
            "agents": [
                {"agent": "market_analyst", "output": json.dumps(market_result)},
                {"agent": "social_media_analyst", "output": json.dumps(social_result)},
                {"agent": "news_analyst", "output": json.dumps(news_result)},
                {"agent": "sec_report_analyst", "output": json.dumps(sec_result)}
            ]
        }
        
        loop = asyncio.get_event_loop()
        debate_result = await loop.run_in_executor(None, run_debate, final_reports)
        
        decision = debate_result.get('validation', {}).get('final_recommendation', {}).get('decision', 'N/A')
        logger.info(f"[{ticker}] ✅ Complete - Decision: {decision}")
        
        return {
            "ticker": ticker,
            "timestamp": datetime.now().isoformat(),
            "news_analysis": news_result,
            "social_analysis": social_result,
            "market_analysis": market_result,
            "sec_analysis": sec_result,
            "debate_result": debate_result
        }
        
    except Exception as e:
        logger.error(f"[{ticker}] ❌ Failed: {e}")
        return {"ticker": ticker, "error": str(e), "timestamp": datetime.now().isoformat()}


class ParallelPipeline:
    """Parallel AEGIS Pipeline - Clean implementation"""
    
    def __init__(self, wait_minutes: int = 60, continuous: bool = True, quick_mode: bool = False):
        # Setup logging
        self.base_log_dir = Path("agent_logs")
        self.base_log_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.session_log_dir = self.base_log_dir / f"parallel_{timestamp}"
        self.session_log_dir.mkdir(exist_ok=True)
        
        self.logger = self._setup_logger()
        self.cycle_count = 0
        self.cycle_log_dir = self.session_log_dir
        
        # Configuration
        self.wait_minutes = wait_minutes
        self.continuous = continuous
        self.quick_mode = quick_mode
        
        # State
        self.redis_client = None
        self.enhanced_pipeline = None
        self.enhanced_pipeline_task = None
        self.finrl_output = None
        self.selected_tickers = []
        self.last_finrl_timestamp = None
        
        # Connection details
        self.redis_url = f"redis://{settings.redis_host}:{settings.redis_port}/0"
        self.openai_api_key = OPENAI_API_KEY  # Use the verified global variable
        
        if not self.openai_api_key:
            self.logger.error("❌ CRITICAL: OpenAI API key not available!")
            raise ValueError("OPENAI_API_KEY missing")
        
        self.logger.info("🚀 Parallel Pipeline initialized")
        self.logger.info(f"   OpenAI API Key: {self.openai_api_key[:20]}...")
        self.logger.info(f"   Wait: {wait_minutes}min | Continuous: {continuous} | Quick: {quick_mode}")
    
    def _setup_logger(self):
        logger = logging.getLogger("ParallelPipeline")
        logger.setLevel(logging.INFO)
        logger.handlers = []
        
        fh = logging.FileHandler(self.session_log_dir / "pipeline.log")
        ch = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        logger.addHandler(fh)
        logger.addHandler(ch)
        
        return logger
    
    def _save_json(self, filename: str, data: Any):
        """Save JSON log"""
        with open(self.cycle_log_dir / filename, 'w') as f:
            json.dump(data, f, indent=2, default=str)
    
    async def setup(self):
        """Initialize Redis and enhanced pipeline"""
        # Redis
        self.redis_client = redis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            decode_responses=True
        )
        self.redis_client.ping()
        self.logger.info("✅ Redis connected")
        
        # Enhanced pipeline (background)
        self.enhanced_pipeline = AegisPipelineEnhanced(quick_mode=self.quick_mode)
        self.enhanced_pipeline_task = asyncio.create_task(self.enhanced_pipeline.start_all())
        self.logger.info("✅ Enhanced pipeline started")
    
    async def wait_for_finrl(self):
        """Wait for first FinRL output"""
        self.logger.info(f"⏳ Waiting {self.wait_minutes + 5}min for FinRL...")
        
        wait_seconds = self.wait_minutes * 60 + 300
        start_time = datetime.now()
        
        check_count = 0
        while (datetime.now() - start_time).total_seconds() < wait_seconds:
            messages = self.redis_client.xrevrange('finrl-decisions', count=1)
            if messages:
                msg_id, msg_data = messages[0]
                timestamp = msg_data.get('timestamp', '')
                if timestamp and timestamp > start_time.isoformat():
                    self.logger.info(f"✅ FinRL output detected at {timestamp}")
                    return True
            
            check_count += 1
            if check_count % 10 == 0:  # Log every 5 minutes
                elapsed = (datetime.now() - start_time).total_seconds()
                remaining = wait_seconds - elapsed
                self.logger.info(f"   Still waiting... {remaining/60:.1f}min remaining")
            
            await asyncio.sleep(30)
        
        self.logger.warning("⚠️  Wait timeout - FinRL may not have produced output yet")
        self.logger.warning("   The system will use fallback dummy data")
        return True
    
    async def fetch_finrl(self):
        """Fetch FinRL decisions from Redis"""
        messages = self.redis_client.xrevrange('finrl-decisions', count=1)
        
        if not messages:
            self.logger.warning("⚠️ No FinRL output in Redis stream")
            self.logger.warning("   This means FinRL service hasn't run yet")
            self.logger.warning("   Using FALLBACK dummy data (all tickers BUY 10)")
            return self._fallback_finrl()
        
        msg_id, msg_data = messages[0]
        timestamp = msg_data.get('timestamp', datetime.now().isoformat())
        
        if timestamp == self.last_finrl_timestamp:
            self.logger.info("ℹ️ No new FinRL output")
            return False
        
        self.last_finrl_timestamp = timestamp
        
        # Parse the data field which contains JSON
        data_json = msg_data.get('data', '{}')
        try:
            data = json.loads(data_json)
            self.selected_tickers = data.get('selected_tickers', [])
            buy_decisions = data.get('buy_decisions', {})
            sell_decisions = data.get('sell_decisions', {})
        except:
            # Fallback to direct fields (old format)
            self.selected_tickers = json.loads(msg_data.get('selected_tickers', '[]'))
            buy_decisions = json.loads(msg_data.get('buy_decisions', '{}'))
            sell_decisions = json.loads(msg_data.get('sell_decisions', '{}'))
        
        if not self.selected_tickers:
            self.logger.warning("⚠️ FinRL output has no selected tickers")
            return self._fallback_finrl()
        
        self.finrl_output = {
            "buy": buy_decisions,
            "sell": sell_decisions,
            "timestamp": timestamp,
            "selected_tickers": self.selected_tickers
        }
        
        self.logger.info(f"✅ FinRL output received:")
        self.logger.info(f"   Timestamp: {timestamp}")
        self.logger.info(f"   Selected: {len(self.selected_tickers)} tickers")
        self.logger.info(f"   Buy signals: {len(buy_decisions)} tickers")
        self.logger.info(f"   Sell signals: {len(sell_decisions)} tickers")
        self._save_json("01_finrl_output.json", self.finrl_output)
        return True
    
    def _fallback_finrl(self):
        """
        Fallback to processed stream when FinRL hasn't run yet.
        
        WARNING: This creates DUMMY BUY signals (all tickers BUY 10 shares).
        This is NOT real FinRL output and should only be used for testing!
        """
        self.logger.warning("=" * 80)
        self.logger.warning("⚠️  USING FALLBACK DUMMY DATA - NOT REAL FINRL OUTPUT!")
        self.logger.warning("=" * 80)
        
        messages = self.redis_client.xrevrange('processed:price', count=500)
        if not messages:
            self.logger.error("❌ No data in Redis processed:price stream either!")
            return False
        
        tickers = set()
        for _, msg_data in messages:
            try:
                data = json.loads(msg_data.get('data', '{}'))
                ticker = data.get('metadata', {}).get('ticker')
                if ticker and ticker != 'VIXY':
                    tickers.add(ticker)
            except:
                pass
        
        self.selected_tickers = sorted(list(tickers))[:10]
        self.finrl_output = {
            "buy": {t: 10 for t in self.selected_tickers},
            "sell": {},
            "timestamp": datetime.now().isoformat(),
            "selected_tickers": self.selected_tickers,
            "_is_dummy_data": True,  # Flag to indicate this is not real FinRL
            "_note": "DUMMY FALLBACK DATA - All tickers set to BUY 10 shares"
        }
        
        self.logger.warning(f"⚠️  Created DUMMY buy signals for {len(self.selected_tickers)} tickers")
        self.logger.warning(f"   Tickers: {self.selected_tickers}")
        self.logger.warning(f"   All set to: BUY 10 shares (NOT real FinRL predictions!)")
        self.logger.warning("=" * 80)
        
        self._save_json("01_finrl_output.json", self.finrl_output)
        return True
    
    async def fetch_market_data(self):
        """Fetch processed market data"""
        messages = self.redis_client.xrevrange('processed:price', count=500)
        
        ticker_map = {}
        for _, msg_data in messages:
            try:
                data = json.loads(msg_data.get('data', '{}'))
                ticker = data.get('metadata', {}).get('ticker')
                if ticker and ticker != 'VIXY' and ticker not in ticker_map:
                    ticker_map[ticker] = data
            except:
                pass
        
        market_data = {}
        for ticker in self.selected_tickers:
            data = ticker_map.get(ticker)
            if data:
                price = data.get('price_data', {})
                ma = data.get('moving_averages', {})
                mom = data.get('momentum_indicators', {})
                trend = data.get('trend_indicators', {})
                vol = data.get('volatility_indicators', {})
                
                market_data[ticker] = {
                    'open': float(price.get('open', 0)),
                    'high': float(price.get('high', 0)),
                    'low': float(price.get('low', 0)),
                    'close': float(price.get('close', 0)),
                    'volume': int(price.get('volume', 0)),
                    'macd': float(mom.get('macd', {}).get('macd_line', 0)),
                    'boll_ub': float(vol.get('boll_ub', 0)),
                    'boll_lb': float(vol.get('boll_lb', 0)),
                    'rsi_30': float(mom.get('rsi_30', 0)),
                    'cci_30': float(mom.get('cci_30', 0)),
                    'dx_30': float(trend.get('dx_30', 0)),
                    'close_30_sma': float(ma.get('close_30_sma', 0)),
                    'close_60_sma': float(ma.get('close_60_sma', 0)),
                    'VIXY': 0.0
                }
        
        self.logger.info(f"✅ Market data: {len(market_data)} tickers")
        self._save_json("02_market_data.json", market_data)
        return market_data
    
    async def fetch_news_social(self):
        """Fetch news and social data"""
        self.logger.info(f"📰 Fetching news/social for {len(self.selected_tickers)} tickers...")
        
        # Initialize producers
        news_producer = NewsProducer()
        social_producer = SocialProducer()
        news_producer.set_active_symbols(self.selected_tickers)
        social_producer.set_active_symbols(self.selected_tickers)
        await news_producer.initialize()
        await social_producer.initialize()
        
        redis_async = await redis.asyncio.from_url(self.redis_url)
        
        # Get starting IDs
        try:
            news_msgs = await redis_async.xrevrange("raw:news-articles", count=1)
            start_news_id = news_msgs[0][0] if news_msgs else None
            if start_news_id and isinstance(start_news_id, bytes):
                start_news_id = start_news_id.decode('utf-8')
            
            social_msgs = await redis_async.xrevrange("raw:social", count=1)
            start_social_id = social_msgs[0][0] if social_msgs else None
            if start_social_id and isinstance(start_social_id, bytes):
                start_social_id = start_social_id.decode('utf-8')
        except:
            start_news_id = start_social_id = None
        
        # Fetch in parallel
        async def fetch_news():
            tasks = []
            for ticker in self.selected_tickers:
                async def fetch_ticker_news(t):
                    if news_producer.api_calls_today >= news_producer.max_calls_per_day:
                        return []
                    try:
                        articles = await news_producer._fetch_news(t)
                        news_producer.api_calls_today += 1
                        return [news_producer._format_article(t, a) for a in (articles or [])[:8]]
                    except:
                        return []
                tasks.append(fetch_ticker_news(ticker))
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Publish
            publish_tasks = []
            for ticker_articles in results:
                if isinstance(ticker_articles, list):
                    for article in ticker_articles:
                        publish_tasks.append(news_producer._publish(article))
            
            if publish_tasks:
                await asyncio.gather(*publish_tasks, return_exceptions=True)
        
        async def fetch_social():
            tasks = [social_producer.fetch_data() for _ in range(len(self.selected_tickers) * 4)]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            publish_tasks = []
            for result in results:
                if result and not isinstance(result, Exception):
                    publish_tasks.append(social_producer._publish(result))
            
            if publish_tasks:
                await asyncio.gather(*publish_tasks, return_exceptions=True)
        
        # Execute
        await asyncio.gather(fetch_news(), fetch_social())
        await news_producer.cleanup()
        await social_producer.cleanup()
        
        # Collect from Redis
        news_data = {t: [] for t in self.selected_tickers}
        social_data = {t: [] for t in self.selected_tickers}
        
        try:
            # News - Use xrevrange to get last 1000 messages (avoids Stream ID issues)
            msgs = await redis_async.xrevrange("raw:news-articles", max="+", min="-", count=1000)
            
            for _, msg_data in msgs:
                try:
                    data_field = msg_data.get(b"data") or msg_data.get("data")
                    if data_field:
                        data_str = data_field.decode('utf-8') if isinstance(data_field, bytes) else data_field
                        data = json.loads(data_str)
                        articles = data.get("batch", [data])
                        for article in articles:
                            ticker = article.get("symbol") or article.get("ticker")
                            if ticker in self.selected_tickers:
                                news_data[ticker].append({
                                    "ticker": ticker,
                                    "title": article.get("headline", article.get("title", "")),
                                    "description": article.get("description", ""),
                                    "timestamp": article.get("timestamp", datetime.now().isoformat()),
                                    "sentiment": article.get("sentiment")
                                })
                except:
                    pass
            
            # Social - Use xrevrange to get last 1000 messages (avoids Stream ID issues)
            msgs = await redis_async.xrevrange("raw:social", max="+", min="-", count=1000)
            
            for _, msg_data in msgs:
                try:
                    data_field = msg_data.get(b"data") or msg_data.get("data")
                    if data_field:
                        data_str = data_field.decode('utf-8') if isinstance(data_field, bytes) else data_field
                        data = json.loads(data_str)
                        ticker = data.get("symbol") or data.get("ticker")
                        if ticker in self.selected_tickers:
                            social_data[ticker].append({
                                "ticker": ticker,
                                "text": data.get("text", ""),
                                "timestamp": data.get("timestamp", datetime.now().isoformat()),
                                "sentiment": data.get("sentiment")
                            })
                except:
                    pass
        finally:
            await redis_async.close()
        
        self.logger.info(f"✅ News/Social fetched")
        self._save_json("03_news_data.json", news_data)
        self._save_json("04_social_data.json", social_data)
        
        return news_data, social_data
    
    async def run_agents(self, market_data, news_data, social_data):
        """Run all agents in parallel"""
        self.logger.info(f"⚡ Processing {len(self.selected_tickers)} tickers in parallel...")
        
        start_time = datetime.now()
        
        tasks = [
            run_ticker_agents(
                ticker=ticker,
                redis_url=self.redis_url,
                openai_api_key=self.openai_api_key,
                news_data=news_data.get(ticker, []),
                social_data=social_data.get(ticker, []),
                market_data=market_data.get(ticker, {}),
                logger=self.logger
            )
            for ticker in self.selected_tickers
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Save results
        agent_results = {}
        for i, result in enumerate(results):
            ticker = self.selected_tickers[i]
            if isinstance(result, Exception):
                self.logger.error(f"[{ticker}] ❌ Exception: {result}")
                agent_results[ticker] = {"ticker": ticker, "error": str(result)}
            else:
                agent_results[ticker] = result
                self._save_json(f"05_agent_{ticker}.json", result)
        
        duration = (datetime.now() - start_time).total_seconds()
        self.logger.info(f"✅ Agents complete in {duration:.2f}s ({duration/len(self.selected_tickers):.2f}s avg)")
        
        return agent_results
    
    async def reconcile(self, agent_results):
        """Reconcile FinRL vs Validator with Pathway aggregation"""
        approved = []
        rejected = []
        
        # ═══════════════════════════════════════════════════════
        # PATHWAY: Aggregate agent consensus using groupby + reduce
        # ═══════════════════════════════════════════════════════
        self.logger.info("⚡ Running Pathway aggregation...")
        pathway_aggregator = PathwayAggregator(self.cycle_log_dir)
        pathway_metrics = pathway_aggregator.aggregate_agent_results(agent_results)
        self._save_json("05b_pathway_metrics.json", pathway_metrics)
        self.logger.info(f"✅ Pathway computed metrics for {len(pathway_metrics)} tickers")
        
        finrl_buy = set(self.finrl_output.get("buy", {}).keys())
        finrl_sell = set(self.finrl_output.get("sell", {}).keys())
        
        for ticker in self.selected_tickers:
            # FinRL action
            if ticker in finrl_buy:
                finrl_action = "BUY"
                finrl_shares = self.finrl_output["buy"][ticker]
            elif ticker in finrl_sell:
                finrl_action = "SELL"
                finrl_shares = self.finrl_output["sell"][ticker]
            else:
                finrl_action = "HOLD"
                finrl_shares = 0
            
            # Validator action
            debate = agent_results.get(ticker, {}).get("debate_result", {})
            validator = debate.get('validation', {}).get('final_recommendation', {})
            validator_action = validator.get('decision', 'HOLD').upper()
            validator_conf = validator.get('conviction', 0)
            
            # Get Pathway metrics for this ticker
            pathway = pathway_metrics.get(ticker, {})
            consensus_score = pathway.get('consensus_score', 0.0)
            sentiment_agreement = pathway.get('sentiment_agreement', 0.0)
            avg_confidence = pathway.get('avg_confidence', 0.0)
            
            aligned = finrl_action == validator_action
            
            # Enhanced approval with Pathway quality gates
            if aligned and finrl_action != "HOLD":
                # Check Pathway consensus thresholds
                if consensus_score >= 60.0 and sentiment_agreement >= 70.0:
                    approved.append({
                        "ticker": ticker,
                        "action": finrl_action,
                        "finrl_shares": finrl_shares,
                        "validator_confidence": validator_conf,
                        "pathway_consensus": consensus_score,
                        "pathway_agreement": sentiment_agreement,
                        "pathway_confidence": avg_confidence,
                        "aligned": True,
                        "reason": f"Strong consensus {finrl_action}"
                    })
                    self.logger.info(
                        f"[{ticker}] ✅ APPROVED - {finrl_action} "
                        f"(Consensus: {consensus_score:.1f}%, Agreement: {sentiment_agreement:.1f}%)"
                    )
                else:
                    rejected.append({
                        "ticker": ticker,
                        "finrl_action": finrl_action,
                        "validator_action": validator_action,
                        "pathway_consensus": consensus_score,
                        "pathway_agreement": sentiment_agreement,
                        "aligned": aligned,
                        "reason": f"Low consensus ({consensus_score:.1f}%) or agreement ({sentiment_agreement:.1f}%)"
                    })
                    self.logger.info(
                        f"[{ticker}] ⚠️  REJECTED - Weak Pathway scores "
                        f"(Consensus: {consensus_score:.1f}%, Agreement: {sentiment_agreement:.1f}%)"
                    )
            else:
                rejected.append({
                    "ticker": ticker,
                    "finrl_action": finrl_action,
                    "validator_action": validator_action,
                    "pathway_consensus": consensus_score,
                    "pathway_agreement": sentiment_agreement,
                    "aligned": aligned,
                    "reason": f"Contradiction" if not aligned else "HOLD"
                })
                self.logger.info(f"[{ticker}] ❌ REJECTED - Not aligned or HOLD")
        
        reconciliation = {
            "timestamp": datetime.now().isoformat(),
            "status": "completed",
            "approved_stocks": approved,
            "rejected_stocks": rejected,
            "pipeline_metrics": {
                "total_processed": len(self.selected_tickers),
                "approved": len(approved),
                "rejected": len(rejected)
            }
        }
        
        self.logger.info(f"✅ Reconciliation: {len(approved)} approved, {len(rejected)} rejected")
        self._save_json("06_reconciliation.json", reconciliation)
        return reconciliation
    
    async def execute_trades(self, reconciliation):
        """Execute trades via DecisionAgent"""
        approved = reconciliation.get("approved_stocks", [])
        
        if not approved:
            self.logger.warning("⚠️ No approved stocks")
            return {"status": "no_trades", "timestamp": datetime.now().isoformat()}
        
        try:
            agent = DecisionAgentRedis(
                openai_api_key=self.openai_api_key,
                mcp_server_url="http://localhost:8000",
                redis_url=self.redis_url
            )
            
            report = await agent.make_decisions_and_execute(reconciliation)
            self._save_json("07_trades.json", report)
            
            summary = report.get("summary", {})
            self.logger.info(f"✅ Trades: {summary.get('trades_executed', 0)} executed, {summary.get('trades_failed', 0)} failed")
            
            return report
        except Exception as e:
            self.logger.error(f"❌ Trade execution failed: {e}")
            return {"status": "failed", "error": str(e), "timestamp": datetime.now().isoformat()}
    
    async def run_cycle(self):
        """Run one complete cycle"""
        self.cycle_count += 1
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.cycle_log_dir = self.session_log_dir / f"cycle_{self.cycle_count:02d}_{timestamp}"
        self.cycle_log_dir.mkdir(exist_ok=True)
        
        self.logger.info("=" * 80)
        self.logger.info(f"CYCLE {self.cycle_count} STARTED")
        self.logger.info("=" * 80)
        
        # Fetch FinRL
        has_new = await self.fetch_finrl()
        if not has_new and self.cycle_count > 1:
            self.logger.info("No new FinRL output, skipping")
            return None
        
        # Fetch data
        market_data = await self.fetch_market_data()
        news_data, social_data = await self.fetch_news_social()
        
        # Run agents
        agent_results = await self.run_agents(market_data, news_data, social_data)
        
        # Reconcile
        reconciliation = await self.reconcile(agent_results)
        
        # Execute trades
        trade_results = await self.execute_trades(reconciliation)
        
        # Summary
        summary = {
            "cycle": self.cycle_count,
            "timestamp": datetime.now().isoformat(),
            "tickers": self.selected_tickers,
            "metrics": reconciliation.get("pipeline_metrics", {}),
            "trade_status": trade_results.get("status", "unknown")
        }
        self._save_json("00_SUMMARY.json", summary)
        
        self.logger.info(f"✅ CYCLE {self.cycle_count} COMPLETED")
        self.logger.info("=" * 80)
        
        return summary
    
    async def run(self):
        """Run complete pipeline"""
        try:
            await self.setup()
            await self.wait_for_finrl()
            await self.run_cycle()
            
            if self.continuous:
                self.logger.info("🔄 Continuous mode - monitoring for new FinRL outputs...")
                while True:
                    await asyncio.sleep(7200)  # 2 hours
                    await self.run_cycle()
            else:
                self.logger.info("✅ Single cycle complete")
                if self.enhanced_pipeline_task:
                    self.enhanced_pipeline_task.cancel()
            
            self.logger.info(f"✅ Pipeline complete - {self.cycle_count} cycles - Logs: {self.session_log_dir}")
            
        except Exception as e:
            self.logger.error(f"❌ Pipeline failed: {e}", exc_info=True)
            raise


async def main():
    """Main entry point"""
    print("""
    ╔═══════════════════════════════════════════════════════════════╗
    ║         PARALLEL AEGIS PIPELINE - CLEAN VERSION               ║
    ╚═══════════════════════════════════════════════════════════════╝
    
    Features:
    ⚡ 4 agents in parallel per ticker (News, Social, Market, SEC)
    ⚡ Multiple tickers processed concurrently
    ⚡ Clean code with critical logging only
    ⚡ Expected 3-4x speedup vs sequential
    
    Press Ctrl+C to stop
    ═══════════════════════════════════════════════════════════════
    """)
    
    wait_minutes = 60
    continuous = True
    quick_mode = False
    
    if "--single" in sys.argv:
        continuous = False
        print("📌 Single cycle mode")
    
    if "--quick" in sys.argv:
        wait_minutes = 5
        quick_mode = True
        print(f"⚡ Quick mode - {wait_minutes}min wait")
    
    pipeline = ParallelPipeline(
        wait_minutes=wait_minutes,
        continuous=continuous,
        quick_mode=quick_mode
    )
    await pipeline.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Stopped by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
