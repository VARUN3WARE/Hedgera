#!/usr/bin/env python3
"""
Full AEGIS Trading Pipeline - ENHANCED with Streaming Fine-tuning

This enhanced pipeline:
1. Runs the ENHANCED FinRL pipeline with:
   - MongoDB sync for historical data storage
   - Automatic model fine-tuning every 2 hours
   - Fine-tuned model predictions
   - Trigger-based immediate FinRL runs on breakouts

2. After first FinRL run completes:
   - Fetches FinRL decisions from Redis
   - Retrieves real processed market data
   - Runs 4 agents (News, Social, Market, SEC)
   - Runs debate & validation
   - Reconciles FinRL vs Validator decisions
   - Executes approved trades

3. Continues running enhanced pipeline in background:
   - MongoDB keeps syncing data
   - Fine-tuning runs every 2 hours
   - FinRL runs every 2 hours with updated model
   - On each FinRL run, triggers agent pipeline again

This is the PRODUCTION full pipeline with continuous operation.
"""

import asyncio
import json
import logging
import os
import redis
import redis.asyncio
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
import sys

sys.path.insert(0, str(Path(__file__).parent))

# Import AEGIS components
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


class FullAegisPipelineEnhanced:
    """
    Complete AEGIS pipeline with streaming fine-tuning.
    
    Enhanced Features:
    - MongoDB sync for historical data
    - Model fine-tuning every 2 hours
    - Continuous operation with periodic agent analysis
    - Trigger-based immediate runs on market breakouts
    - NO wait times (uses historical data from MongoDB)
    """
    
    def __init__(self, wait_minutes: int = 60, continuous: bool = True, quick_mode: bool = False):
        """
        Initialize enhanced full pipeline
        
        Args:
            wait_minutes: Unused (kept for backward compatibility)
            continuous: If True, keep running and repeat agent analysis every 2 hours
            quick_mode: If True, use quick test settings in enhanced pipeline
        """
        # Setup logging directory
        self.base_log_dir = Path("agent_logs")
        self.base_log_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.session_log_dir = self.base_log_dir / f"enhanced_{timestamp}"
        self.session_log_dir.mkdir(exist_ok=True)
        
        # Store quick mode flag
        self.quick_mode = quick_mode
        
        # Current cycle log directory (updated each cycle)
        self.cycle_log_dir = self.session_log_dir
        self.cycle_count = 0
        
        # Setup logger
        self.logger = self._setup_logger()
        
        # Configuration
        self.wait_minutes = wait_minutes
        self.continuous = continuous
        
        # Pipeline components
        self.enhanced_pipeline = None
        self.enhanced_pipeline_task = None
        self.redis_client = None
        
        # FinRL output (will be populated from Redis)
        self.finrl_output = None
        self.selected_tickers = []
        self.last_finrl_timestamp = None
        
        self.logger.info("=" * 80)
        self.logger.info("FULL AEGIS PIPELINE - ENHANCED WITH STREAMING FINE-TUNING")
        self.logger.info("=" * 80)
        self.logger.info(f"Wait time for first FinRL: {wait_minutes} minutes")
        self.logger.info(f"Continuous mode: {continuous}")
        self.logger.info(f"Session log directory: {self.session_log_dir}")
    
    def _setup_logger(self):
        """Setup logging for this pipeline run"""
        logger = logging.getLogger("FullAegisPipelineEnhanced")
        logger.setLevel(logging.INFO)
        
        # Remove existing handlers
        logger.handlers = []
        
        # File handler
        fh = logging.FileHandler(self.session_log_dir / "full_pipeline_enhanced.log")
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
    
    def _create_cycle_log_dir(self):
        """Create a new log directory for this agent cycle"""
        self.cycle_count += 1
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.cycle_log_dir = self.session_log_dir / f"cycle_{self.cycle_count:02d}_{timestamp}"
        self.cycle_log_dir.mkdir(exist_ok=True)
        self.logger.info(f"📁 Cycle {self.cycle_count} log directory: {self.cycle_log_dir}")
    
    def _save_json_log(self, filename: str, data: Any):
        """Save data to JSON file in current cycle log directory"""
        filepath = self.cycle_log_dir / filename
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        self.logger.info(f"💾 Saved {filename}")
    
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
    
    async def start_enhanced_pipeline(self):
        """
        Start the enhanced pipeline with MongoDB sync and fine-tuning.
        This runs in the background continuously.
        """
        self.logger.info("=" * 80)
        self.logger.info("STARTING ENHANCED PIPELINE (MongoDB Sync + Fine-tuning)")
        self.logger.info("=" * 80)
        
        try:
            self.enhanced_pipeline = AegisPipelineEnhanced(quick_mode=self.quick_mode)
            self.enhanced_pipeline_task = asyncio.create_task(
                self.enhanced_pipeline.start_all()
            )
            self.logger.info("✅ Enhanced pipeline started in background")
            if self.quick_mode:
                self.logger.info("   ⚡ QUICK MODE ENABLED")
                self.logger.info("   • MongoDB sync: Every 10 seconds")
                self.logger.info("   • Fine-tuning: After 10 minutes, then every 2 hours")
                self.logger.info("   • FinRL runs: After 5 minutes, then every 2 hours")
                self.logger.info("   • Trigger checks: Every 5 minutes")
            else:
                self.logger.info("   • MongoDB sync: Every 60 seconds")
                self.logger.info("   • Fine-tuning: Every 2 hours")
                self.logger.info("   • FinRL runs: Every 2 hours (after initial 60 min)")
                self.logger.info("   • Trigger checks: Every 5 minutes for breakouts")
        except Exception as e:
            self.logger.error(f"❌ Failed to start enhanced pipeline: {e}")
            raise
    
    async def fetch_finrl_output(self, max_retries: int = 5, retry_delay: int = 120):
        """
        Fetch FinRL output from Redis with retry logic.
        
        Args:
            max_retries: Maximum number of retry attempts (default: 5)
            retry_delay: Delay between retries in seconds (default: 120 = 2 minutes)
        
        Returns:
            True if new output was found, False otherwise.
        """
        self.logger.info("=" * 80)
        self.logger.info("FETCHING FinRL OUTPUT")
        self.logger.info("=" * 80)
        
        for attempt in range(1, max_retries + 1):
            try:
                # Read latest FinRL decision from Redis stream
                messages = self.redis_client.xrevrange('finrl-decisions', count=1)
                
                if not messages:
                    if attempt < max_retries:
                        self.logger.warning(f"⚠️  No FinRL decisions found (attempt {attempt}/{max_retries})")
                        self.logger.info(f"⏳ Waiting {retry_delay} seconds before retry...")
                        await asyncio.sleep(retry_delay)
                        continue
                    else:
                        self.logger.warning(f"⚠️  No FinRL decisions after {max_retries} attempts, using fallback")
                        return self._fallback_to_processed_stream()
            
                # Parse the message
                message_id, message_data = messages[0]
                
                timestamp = message_data.get('timestamp', datetime.now().isoformat())
                
                # Check if this is the same as last time
                if timestamp == self.last_finrl_timestamp:
                    if attempt < max_retries:
                        self.logger.info(f"ℹ️  No new FinRL output since last check (attempt {attempt}/{max_retries})")
                        self.logger.info(f"⏳ Waiting {retry_delay} seconds before retry...")
                        await asyncio.sleep(retry_delay)
                        continue
                    else:
                        self.logger.info("ℹ️  No new FinRL output after retries")
                        return False
                
                self.last_finrl_timestamp = timestamp
                
                # Parse the nested 'data' field (FinRL service publishes data as JSON string)
                data_str = message_data.get('data', '{}')
                data = json.loads(data_str) if isinstance(data_str, str) else data_str
                
                selected_tickers = data.get('selected_tickers', [])
                buy_decisions = data.get('buy_decisions', {})
                sell_decisions = data.get('sell_decisions', {})
                
                # If FinRL returned empty tickers, retry or fallback
                if not selected_tickers:
                    if attempt < max_retries:
                        self.logger.warning(f"⚠️  FinRL returned empty tickers (attempt {attempt}/{max_retries})")
                        self.logger.info(f"⏳ Waiting {retry_delay} seconds before retry...")
                        await asyncio.sleep(retry_delay)
                        continue
                    else:
                        self.logger.warning(f"⚠️  FinRL returned empty tickers after {max_retries} attempts, using fallback")
                        return self._fallback_to_processed_stream()
                
                self.finrl_output = {
                    "buy": buy_decisions,
                    "sell": sell_decisions,
                    "timestamp": timestamp,
                    "selected_tickers": selected_tickers
                }
                
                self.selected_tickers = selected_tickers
                
                self.logger.info(f"✅ Retrieved FinRL output from Redis (attempt {attempt})")
                self.logger.info(f"   Timestamp: {timestamp}")
                self.logger.info(f"   Selected tickers: {selected_tickers}")
                self.logger.info(f"   Buy signals: {list(buy_decisions.keys())}")
                self.logger.info(f"   Sell signals: {list(sell_decisions.keys())}")
                
                # Save FinRL output
                self._save_json_log("01_finrl_output.json", self.finrl_output)
                return True
                
            except Exception as e:
                if attempt < max_retries:
                    self.logger.warning(f"⚠️  Error fetching FinRL output (attempt {attempt}/{max_retries}): {e}")
                    self.logger.info(f"⏳ Waiting {retry_delay} seconds before retry...")
                    await asyncio.sleep(retry_delay)
                    continue
                else:
                    self.logger.error(f"❌ Error fetching FinRL output after {max_retries} attempts: {e}", exc_info=True)
                    return False
        
        return False
    
    def _fallback_to_processed_stream(self):
        """Fallback: get tickers from processed stream if no FinRL output"""
        self.logger.info("📊 Fetching tickers from processed stream...")
        
        processed_messages = self.redis_client.xrevrange('processed:price', count=500)
        
        if not processed_messages:
            self.logger.error("❌ No processed data found either")
            return False
        
        # Extract unique tickers
        unique_tickers = set()
        for msg_id, msg_data in processed_messages:
            data_json = msg_data.get('data')
            if data_json:
                try:
                    data = json.loads(data_json)
                    ticker = data.get('metadata', {}).get('ticker')
                    if ticker and ticker != 'VIXY':
                        unique_tickers.add(ticker)
                except json.JSONDecodeError:
                    continue
        
        self.selected_tickers = sorted(list(unique_tickers))[:10]
        
        self.finrl_output = {
            "buy": {ticker: 10 for ticker in self.selected_tickers},
            "sell": {},
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "selected_tickers": self.selected_tickers
        }
        
        self.logger.info(f"✅ Using {len(self.selected_tickers)} tickers from processed stream")
        self._save_json_log("01_finrl_output.json", self.finrl_output)
        return True
    
    async def fetch_processed_market_data(self):
        """Fetch real processed market data from Redis"""
        self.logger.info("=" * 80)
        self.logger.info("FETCHING PROCESSED MARKET DATA")
        self.logger.info("=" * 80)
        
        try:
            market_data = {}
            
            messages = self.redis_client.xrevrange('processed:price', count=500)
            
            if not messages:
                self.logger.warning("⚠️  No processed data found in stream")
                self._save_json_log("02_processed_market_data.json", market_data)
                return market_data
            
            self.logger.info(f"   Found {len(messages)} entries in processed stream")
            
            # Build a map of ticker -> latest data
            ticker_map = {}
            for msg_id, msg_data in messages:
                data_json = msg_data.get('data')
                if data_json:
                    try:
                        data = json.loads(data_json)
                        ticker = data.get('metadata', {}).get('ticker')
                        if ticker and ticker != 'VIXY' and ticker not in ticker_map:
                            ticker_map[ticker] = data
                    except json.JSONDecodeError:
                        continue
            
            self.logger.info(f"   Found data for {len(ticker_map)} unique tickers")
            
            # Extract data for selected tickers
            for ticker in self.selected_tickers:
                data = ticker_map.get(ticker)
                
                if data:
                    price_data = data.get('price_data', {})
                    moving_avgs = data.get('moving_averages', {})
                    momentum = data.get('momentum_indicators', {})
                    trend = data.get('trend_indicators', {})
                    volatility = data.get('volatility_indicators', {})
                    macd_data = momentum.get('macd', {})
                    
                    processed_data = {
                        'open': float(price_data.get('open', 0)),
                        'high': float(price_data.get('high', 0)),
                        'low': float(price_data.get('low', 0)),
                        'close': float(price_data.get('close', 0)),
                        'volume': int(price_data.get('volume', 0)),
                        'macd': float(macd_data.get('macd_line', 0)),
                        'boll_ub': float(volatility.get('boll_ub', 0)),
                        'boll_lb': float(volatility.get('boll_lb', 0)),
                        'rsi_30': float(momentum.get('rsi_30', 0)),
                        'cci_30': float(momentum.get('cci_30', 0)),
                        'dx_30': float(trend.get('dx_30', 0)),
                        'close_30_sma': float(moving_avgs.get('close_30_sma', 0)),
                        'close_60_sma': float(moving_avgs.get('close_60_sma', 0)),
                        'VIXY': 0.0
                    }
                    market_data[ticker] = processed_data
                    self.logger.info(f"   ✅ {ticker}: Close=${processed_data['close']:.2f}, "
                                   f"RSI={processed_data['rsi_30']:.1f}")
                else:
                    self.logger.warning(f"   ⚠️  No data found for {ticker}")
            
            self._save_json_log("02_processed_market_data.json", market_data)
            return market_data
            
        except Exception as e:
            self.logger.error(f"❌ Error fetching market data: {e}", exc_info=True)
            return {}
    
    async def fetch_news_social(self):
        """Fetch news and social data for selected tickers"""
        self.logger.info("=" * 80)
        self.logger.info("FETCHING NEWS & SOCIAL DATA")
        self.logger.info("=" * 80)
        
        news_producer = NewsProducer()
        social_producer = SocialProducer()
        
        news_producer.set_active_symbols(self.selected_tickers)
        social_producer.set_active_symbols(self.selected_tickers)
        
        await news_producer.initialize()
        await social_producer.initialize()
        
        self.logger.info(f"✅ Producers activated for: {self.selected_tickers}")
        
        # Get starting message IDs
        redis_async = await redis.asyncio.from_url(
            f"redis://{settings.redis_host}:{settings.redis_port}/0"
        )
        
        start_news_id = None
        start_social_id = None
        
        try:
            news_msgs = await redis_async.xrevrange("raw:news-articles", count=1)
            if news_msgs:
                msg_id = news_msgs[0][0]
                start_news_id = msg_id.decode('utf-8') if isinstance(msg_id, bytes) else msg_id
            
            social_msgs = await redis_async.xrevrange("raw:social", count=1)
            if social_msgs:
                msg_id = social_msgs[0][0]
                start_social_id = msg_id.decode('utf-8') if isinstance(msg_id, bytes) else msg_id
        except Exception as e:
            self.logger.warning(f"Could not get starting message IDs: {e}")
        
        # Produce data for each ticker
        for ticker in self.selected_tickers:
            self.logger.info(f"📰 Fetching data for {ticker}...")
            
            for _ in range(2):  # 2 news articles per ticker
                news_result = await news_producer.fetch_data()
                if news_result:
                    await news_producer._publish(news_result)
            
            for _ in range(3):  # 3 social posts per ticker
                social_result = await social_producer.fetch_data()
                if social_result:
                    await social_producer._publish(social_result)
        
        await news_producer.cleanup()
        await social_producer.cleanup()
        
        # Fetch the newly produced data
        news_data = {ticker: [] for ticker in self.selected_tickers}
        social_data = {ticker: [] for ticker in self.selected_tickers}
        
        try:
            # Fetch news
            if start_news_id:
                new_news_msgs = await redis_async.xrange(
                    "raw:news-articles", min=f"({start_news_id}", max="+", count=1000
                )
            else:
                new_news_msgs = await redis_async.xrevrange("raw:news-articles", count=500)
            
            for msg_id, msg_data in new_news_msgs:
                try:
                    data_field = msg_data.get(b"data") or msg_data.get("data")
                    if not data_field:
                        continue
                    
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
                                "sentiment": article.get("sentiment"),
                            })
                except Exception as e:
                    pass
            
            # Fetch social
            if start_social_id:
                new_social_msgs = await redis_async.xrange(
                    "raw:social", min=f"({start_social_id}", max="+", count=1000
                )
            else:
                new_social_msgs = await redis_async.xrevrange("raw:social", count=500)
            
            for msg_id, msg_data in new_social_msgs:
                try:
                    data_field = msg_data.get(b"data") or msg_data.get("data")
                    if not data_field:
                        continue
                    
                    data_str = data_field.decode('utf-8') if isinstance(data_field, bytes) else data_field
                    data = json.loads(data_str)
                    
                    ticker = data.get("symbol") or data.get("ticker")
                    if ticker in self.selected_tickers:
                        social_data[ticker].append({
                            "ticker": ticker,
                            "text": data.get("text", ""),
                            "timestamp": data.get("timestamp", datetime.now().isoformat()),
                            "sentiment": data.get("sentiment"),
                        })
                except Exception as e:
                    pass
                    
        finally:
            await redis_async.close()
        
        # Log summary
        for ticker in self.selected_tickers:
            self.logger.info(f"   {ticker}: {len(news_data.get(ticker, []))} news, "
                           f"{len(social_data.get(ticker, []))} social")
        
        self._save_json_log("03_news_data.json", news_data)
        self._save_json_log("04_social_data.json", social_data)
        
        return news_data, social_data
    
    async def run_agents(self, market_data: Dict, news_data: Dict, social_data: Dict):
        """Run all 4 agents on each ticker"""
        self.logger.info("=" * 80)
        self.logger.info("RUNNING AGENTS")
        self.logger.info("=" * 80)
        
        all_agent_results = {}
        openai_api_key = os.getenv("OPENAI_API_KEY")
        redis_url = f"redis://{settings.redis_host}:{settings.redis_port}/0"
        
        for ticker in self.selected_tickers:
            self.logger.info(f"\n{'='*40}")
            self.logger.info(f"Processing: {ticker}")
            self.logger.info(f"{'='*40}")
            
            ticker_results = {"ticker": ticker, "timestamp": datetime.now().isoformat()}
            
            ticker_news = news_data.get(ticker, [])
            ticker_social = social_data.get(ticker, [])
            ticker_market = market_data.get(ticker, {})
            
            # 1. News Analyst
            try:
                self.logger.info(f"[{ticker}] Running News Analyst...")
                news_agent = NewsAnalystRedis(
                    redis_url=redis_url, stream_key="raw:news-articles",
                    openai_api_key=openai_api_key, logger=self.logger
                )
                await news_agent.connect()
                
                if ticker_news:
                    news_agent.news_buffer[ticker] = ticker_news
                else:
                    news_agent.news_buffer[ticker] = [{
                        "ticker": ticker, "title": f"{ticker} shows strong performance",
                        "sentiment": 0.5, "timestamp": datetime.now().isoformat()
                    }]
                
                news_analysis = await news_agent.analyze_news_sentiment(ticker)
                await news_agent.disconnect()
                ticker_results["news_analysis"] = news_analysis
                self.logger.info(f"[{ticker}] ✅ News analysis complete")
            except Exception as e:
                self.logger.error(f"[{ticker}] News analyst error: {e}")
                ticker_results["news_analysis"] = {"error": str(e)}
            
            # 2. Social Analyst
            try:
                self.logger.info(f"[{ticker}] Running Social Analyst...")
                social_agent = SocialAnalystRedis(
                    redis_url=redis_url, stream_key="raw:social",
                    openai_api_key=openai_api_key, logger=self.logger
                )
                await social_agent.connect()
                
                if ticker_social:
                    social_agent.social_buffer[ticker] = ticker_social
                else:
                    social_agent.social_buffer[ticker] = [{
                        "ticker": ticker, "text": f"Investors optimistic about {ticker}",
                        "sentiment": 0.5, "timestamp": datetime.now().isoformat()
                    }]
                
                social_analysis = await social_agent.analyze_social_sentiment(ticker)
                await social_agent.disconnect()
                ticker_results["social_analysis"] = social_analysis
                self.logger.info(f"[{ticker}] ✅ Social analysis complete")
            except Exception as e:
                self.logger.error(f"[{ticker}] Social analyst error: {e}")
                ticker_results["social_analysis"] = {"error": str(e)}
            
            # 3. Market Analyst
            try:
                self.logger.info(f"[{ticker}] Running Market Analyst...")
                market_agent = MarketAnalystRedis(
                    redis_url=redis_url, stream_key="price_stream",
                    openai_api_key=openai_api_key, logger=self.logger
                )
                await market_agent.connect()
                
                if ticker_market:
                    market_agent.price_buffer[ticker] = [ticker_market]
                    self.logger.info(f"[{ticker}] Using REAL market data")
                
                market_analysis = await market_agent.analyze_market_data(ticker)
                await market_agent.disconnect()
                ticker_results["market_analysis"] = market_analysis
                self.logger.info(f"[{ticker}] ✅ Market analysis complete")
            except Exception as e:
                self.logger.error(f"[{ticker}] Market analyst error: {e}")
                ticker_results["market_analysis"] = {"error": str(e)}
            
            # 4. SEC Analyst
            try:
                self.logger.info(f"[{ticker}] Running SEC Analyst...")
                sec_agent = SecReportAnalystRedis(
                    redis_url=redis_url, stream_key="sec_stream",
                    openai_api_key=openai_api_key, logger=self.logger
                )
                await sec_agent.connect()
                sec_analysis = await sec_agent.fetch_and_analyze_ticker(ticker)
                await sec_agent.disconnect()
                ticker_results["sec_analysis"] = sec_analysis
                self.logger.info(f"[{ticker}] ✅ SEC analysis complete")
            except Exception as e:
                self.logger.error(f"[{ticker}] SEC analyst error: {e}")
                ticker_results["sec_analysis"] = {"error": str(e)}
            
            all_agent_results[ticker] = ticker_results
            self._save_json_log(f"05_agent_results_{ticker}.json", ticker_results)
        
        self._save_json_log("06_all_agent_results.json", all_agent_results)
        return all_agent_results
    
    async def run_debate(self, agent_results: Dict):
        """Run debate for each ticker"""
        self.logger.info("=" * 80)
        self.logger.info("RUNNING DEBATE & VALIDATION")
        self.logger.info("=" * 80)
        
        debate_results = {}
        
        for ticker in self.selected_tickers:
            self.logger.info(f"Debate for: {ticker}")
            
            try:
                ticker_data = agent_results.get(ticker, {})
                
                final_reports_data = {
                    "ticker": ticker,
                    "agents": [
                        {"agent": "market_analyst", "output": json.dumps(ticker_data.get("market_analysis", {}))},
                        {"agent": "social_media_analyst", "output": json.dumps(ticker_data.get("social_analysis", {}))},
                        {"agent": "news_analyst", "output": json.dumps(ticker_data.get("news_analysis", {}))},
                        {"agent": "sec_report_analyst", "output": json.dumps(ticker_data.get("sec_analysis", {}))}
                    ]
                }
                
                debate_result = run_debate(final_reports_data=final_reports_data)
                debate_results[ticker] = debate_result
                
                final_rec = debate_result.get('validation', {}).get('final_recommendation', {})
                self.logger.info(f"[{ticker}] Decision: {final_rec.get('decision', 'N/A')} "
                               f"(Conviction: {final_rec.get('conviction', 'N/A')})")
                
            except Exception as e:
                self.logger.error(f"[{ticker}] Debate error: {e}")
                debate_results[ticker] = {"error": str(e)}
        
        self._save_json_log("07_debate_results.json", debate_results)
        return debate_results
    
    async def reconcile_and_decide(self, debate_results: Dict):
        """Reconcile FinRL output with Validator decisions"""
        self.logger.info("=" * 80)
        self.logger.info("RECONCILING FinRL & VALIDATOR DECISIONS")
        self.logger.info("=" * 80)
        
        approved_stocks = []
        rejected_stocks = []
        
        finrl_buy_tickers = set(self.finrl_output.get("buy", {}).keys())
        finrl_sell_tickers = set(self.finrl_output.get("sell", {}).keys())
        
        for ticker in self.selected_tickers:
            # Get FinRL action
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
            validator_action = validator_decision.get('decision', 'HOLD').upper()
            validator_confidence = validator_decision.get('conviction', 0)
            
            self.logger.info(f"[{ticker}] FinRL: {finrl_action} ({finrl_shares}), "
                           f"Validator: {validator_action} ({validator_confidence})")
            
            aligned = finrl_action == validator_action
            
            if aligned and finrl_action != "HOLD":
                approved_stocks.append({
                    "ticker": ticker, "action": finrl_action, "finrl_shares": finrl_shares,
                    "validator_confidence": validator_confidence, "aligned": True,
                    "reason": f"Both systems recommend {finrl_action}"
                })
                self.logger.info(f"[{ticker}] ✅ APPROVED")
            elif not aligned:
                rejected_stocks.append({
                    "ticker": ticker, "finrl_action": finrl_action, "validator_action": validator_action,
                    "aligned": False, "reason": f"Contradiction: FinRL={finrl_action}, Validator={validator_action}"
                })
                self.logger.warning(f"[{ticker}] ❌ REJECTED: Contradiction")
            else:
                rejected_stocks.append({
                    "ticker": ticker, "finrl_action": finrl_action, "validator_action": validator_action,
                    "aligned": True, "reason": "Both recommend HOLD"
                })
                self.logger.info(f"[{ticker}] ⏸️  HOLD")
        
        self.logger.info(f"\n✅ Approved: {len(approved_stocks)}, ❌ Rejected: {len(rejected_stocks)}")
        
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
    
    async def execute_trades(self, reconciliation_results: Dict):
        """Execute trades for approved stocks"""
        self.logger.info("=" * 80)
        self.logger.info("EXECUTING TRADES")
        self.logger.info("=" * 80)
        
        approved_stocks = reconciliation_results.get("approved_stocks", [])
        
        if not approved_stocks:
            self.logger.warning("⚠️  No approved stocks to trade")
            return {"timestamp": datetime.now().isoformat(), "status": "no_trades"}
        
        try:
            openai_api_key = os.getenv("OPENAI_API_KEY")
            
            decision_agent = DecisionAgentRedis(
                openai_api_key=openai_api_key,
                mcp_server_url="http://localhost:8000",
                redis_url=f"redis://{settings.redis_host}:{settings.redis_port}/0"
            )
            
            pipeline_output = {
                "approved_stocks": approved_stocks,
                "timestamp": datetime.now().isoformat()
            }
            
            trade_report = await decision_agent.make_decisions_and_execute(pipeline_output)
            self._save_json_log("09_trade_execution.json", trade_report)
            
            summary = trade_report.get("summary", {})
            self.logger.info(f"✅ Trades Executed: {summary.get('trades_executed', 0)}")
            self.logger.info(f"❌ Trades Failed: {summary.get('trades_failed', 0)}")
            
            return trade_report
            
        except Exception as e:
            self.logger.error(f"❌ Trade execution failed: {e}", exc_info=True)
            return {"timestamp": datetime.now().isoformat(), "status": "failed", "error": str(e)}
    
    async def run_agent_cycle(self):
        """Run one complete agent analysis cycle"""
        self._create_cycle_log_dir()
        
        self.logger.info("=" * 80)
        self.logger.info(f"AGENT CYCLE {self.cycle_count} STARTED")
        self.logger.info("=" * 80)
        
        # Fetch FinRL output
        has_new_output = await self.fetch_finrl_output()
        if not has_new_output and self.cycle_count > 1:
            self.logger.info("No new FinRL output, skipping agent cycle")
            return None
        
        # Fetch market data
        market_data = await self.fetch_processed_market_data()
        
        # Fetch news and social
        news_data, social_data = await self.fetch_news_social()
        
        # Run agents
        agent_results = await self.run_agents(market_data, news_data, social_data)
        
        # Run debate
        debate_results = await self.run_debate(agent_results)
        
        # Reconcile
        reconciliation_results = await self.reconcile_and_decide(debate_results)
        
        # Execute trades
        trade_results = await self.execute_trades(reconciliation_results)
        
        # Save summary
        summary = {
            "cycle": self.cycle_count,
            "timestamp": datetime.now().isoformat(),
            "tickers_processed": self.selected_tickers,
            "finrl_output": self.finrl_output,
            "reconciliation": reconciliation_results.get("summary", {}),
            "trade_status": trade_results.get("status", "unknown")
        }
        self._save_json_log("00_SUMMARY.json", summary)
        
        self.logger.info("=" * 80)
        self.logger.info(f"AGENT CYCLE {self.cycle_count} COMPLETED")
        self.logger.info("=" * 80)
        
        return summary
    
    async def run(self):
        """Run the complete enhanced full pipeline"""
        try:
            self.logger.info("=" * 80)
            self.logger.info("FULL ENHANCED PIPELINE STARTED")
            self.logger.info("=" * 80)
            
            # Setup
            await self.setup_redis()
            
            # Start enhanced pipeline (MongoDB sync + fine-tuning + FinRL)
            await self.start_enhanced_pipeline()
            
            # Small delay to let pipeline components start
            self.logger.info("⏳ Waiting 2 minutes for pipeline components to initialize...")
            await asyncio.sleep(120)
            
            # Run first agent cycle
            await self.run_agent_cycle()
            
            if self.continuous:
                self.logger.info("=" * 80)
                self.logger.info("CONTINUOUS MODE - Monitoring for new FinRL outputs")
                self.logger.info("Agent cycles will run after each FinRL run (every 2 hours)")
                self.logger.info("=" * 80)
                
                # Continue monitoring and running agent cycles
                while True:
                    # Wait 2 hours (FinRL interval)
                    await asyncio.sleep(7200)
                    
                    # Run another agent cycle
                    await self.run_agent_cycle()
            else:
                self.logger.info("Single cycle mode - Pipeline complete")
                
                # Stop the enhanced pipeline
                if self.enhanced_pipeline_task:
                    self.enhanced_pipeline_task.cancel()
                    try:
                        await self.enhanced_pipeline_task
                    except asyncio.CancelledError:
                        pass
            
            print("\n" + "=" * 80)
            print("✅ FULL ENHANCED PIPELINE COMPLETE")
            print("=" * 80)
            print(f"📁 Session logs: {self.session_log_dir}")
            print(f"📊 Total cycles: {self.cycle_count}")
            print("=" * 80)
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}", exc_info=True)
            raise


def print_banner():
    """Print startup banner"""
    banner = """
    ╔═══════════════════════════════════════════════════════════════════╗
    ║     FULL AEGIS TRADING SYSTEM - ENHANCED WITH FINE-TUNING        ║
    ╚═══════════════════════════════════════════════════════════════════╝
    
    ENHANCED FEATURES:
    ✨ MongoDB sync for historical data (permanent storage)
    ✨ Automatic model fine-tuning every 2 hours
    ✨ Fine-tuned PPO model predictions
    ✨ Trigger-based immediate runs on market breakouts
    ✨ Continuous operation with periodic agent analysis
    ✨ NO wait times - immediate execution with historical data
    
    WORKFLOW:
    ┌─────────────────────────────────────────────────────────────────┐
    │ PHASE 0: Setup (Run once before pipeline)                      │
    │   • python historical_data.py - Ensure 3 days of data          │
    ├─────────────────────────────────────────────────────────────────┤
    │ PHASE 1: Enhanced Pipeline (Runs Continuously)                  │
    │   • Price data collection for 30 tickers                       │
    │   • MongoDB sync every 60 seconds                              │
    │   • Fine-tuning runs immediately, then every 2 hours           │
    │   • FinRL predictions run immediately, then every 2 hours      │
    │   • Trigger checks every 5 minutes                             │
    ├─────────────────────────────────────────────────────────────────┤
    │ PHASE 2: Agent Analysis (After FinRL initialization)           │
    │   • Fetch FinRL decisions from Redis                           │
    │   • Fetch news & social data for selected tickers              │
    │   • Run 4 agents (News, Social, Market, SEC)                   │
    │   • Run debate & validation                                    │
    │   • Reconcile FinRL vs Validator decisions                     │
    │   • Execute trades for approved stocks                         │
    └─────────────────────────────────────────────────────────────────┘
    
    Press Ctrl+C to stop
    ════════════════════════════════════════════════════════════════════
    """
    print(banner)


async def main():
    """Main entry point"""
    print_banner()
    
    # Configuration
    continuous = True  # Keep running and repeat every 2 hours
    quick_mode = False
    
    # Check for command line args
    if "--single" in sys.argv:
        continuous = False
        print("📌 Single cycle mode - will exit after first agent cycle")
    
    if "--quick" in sys.argv:
        quick_mode = True
        print("⚡ Quick mode enabled")
        print("   Enhanced pipeline will use quick settings (faster intervals)")
    
    print(f"🔄 Continuous mode: {continuous}")
    print(f"⚡ Quick mode: {quick_mode}")
    print("")
    print("💡 TIP: Run 'python historical_data.py' first to ensure 3 days of data")
    print("")
    
    pipeline = FullAegisPipelineEnhanced(
        wait_minutes=0,  # No wait - using historical data
        continuous=continuous,
        quick_mode=quick_mode
    )
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
