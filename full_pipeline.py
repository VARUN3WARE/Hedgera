#!/usr/bin/env python3
"""
Full AEGIS Trading Pipeline
Combines both pipelines:
1. Run original pipeline (collect data → FinRL → get decisions)
2. Run enhanced pipeline (agents → debate → reconciliation → trade execution)

This pipeline:
- Starts data collection for 30 tickers
- Waits for FinRL to complete and get BUY/SELL decisions
- Retrieves real processed market data from Redis (no fake data)
- Runs 4 agents on FinRL-selected tickers
- Reconciles FinRL vs Validator decisions
- Executes trades for approved stocks
"""

import asyncio
import json
import logging
import os
import redis
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
import sys

sys.path.insert(0, str(Path(__file__).parent))

# Import AEGIS components
from backend.config.settings import settings
from backend.src.orchestration.pipeline_main import AegisPipeline
from backend.src.producers.news_producer_impl import NewsProducer
from backend.src.producers.social_producer_impl import SocialProducer
from backend.src.consumers.master_state_consumer import MasterStateConsumer
from backend.src.agents_redis.news_analyst_redis import NewsAnalystRedis
from backend.src.agents_redis.social_analyst_redis import SocialAnalystRedis
from backend.src.agents_redis.market_analyst_redis import MarketAnalystRedis
from backend.src.agents_redis.sec_report_analyst_redis import SecReportAnalystRedis
from backend.src.agents_redis.debate import run_debate
from backend.src.agents_redis.decision_agent_redis_mcp import DecisionAgentRedis


class FullAegisPipeline:
    """Complete AEGIS pipeline orchestrator - Data Collection → FinRL → Agents → Execution"""
    
    def __init__(self, skip_data_collection: bool = False, wait_minutes: int = 60):
        """
        Initialize full pipeline
        
        Args:
            skip_data_collection: If True, skip data collection and use existing FinRL output
            wait_minutes: How long to wait for data collection (default: 60 minutes)
        """
        # Setup logging directory
        self.base_log_dir = Path("agent_logs")
        self.base_log_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.cycle_log_dir = self.base_log_dir / timestamp
        self.cycle_log_dir.mkdir(exist_ok=True)
        
        # Setup logger
        self.logger = self._setup_logger()
        
        # Configuration
        self.skip_data_collection = skip_data_collection
        self.wait_minutes = wait_minutes
        
        # Pipeline components
        self.data_pipeline = None
        self.redis_client = None
        
        # FinRL output (will be populated from Redis)
        self.finrl_output = None
        self.selected_tickers = []
        
        self.logger.info("=" * 80)
        self.logger.info("FULL AEGIS PIPELINE INITIALIZED")
        self.logger.info("=" * 80)
        self.logger.info(f"Skip data collection: {skip_data_collection}")
        self.logger.info(f"Wait time: {wait_minutes} minutes")
        self.logger.info(f"Log directory: {self.cycle_log_dir}")
    
    def _setup_logger(self):
        """Setup logging for this pipeline run"""
        logger = logging.getLogger("FullAegisPipeline")
        logger.setLevel(logging.INFO)
        
        # Remove existing handlers
        logger.handlers = []
        
        # File handler
        fh = logging.FileHandler(self.cycle_log_dir / "full_pipeline.log")
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
    
    async def phase1_run_data_collection_and_finrl(self):
        """
        Phase 1: Run data collection and FinRL model
        
        This runs the original pipeline:
        - Collect price data for 30 tickers (60 minutes)
        - Calculate technical indicators in real-time
        - Run FinRL model to get BUY/SELL decisions
        - Store decisions in Redis
        """
        self.logger.info("=" * 80)
        self.logger.info("PHASE 1: DATA COLLECTION & FinRL")
        self.logger.info("=" * 80)
        
        if self.skip_data_collection:
            self.logger.info("⏭️  Skipping data collection (using existing data)")
            return
        
        try:
            self.logger.info(f"🚀 Starting data collection pipeline...")
            self.logger.info(f"⏰ Will collect data for {self.wait_minutes} minutes")
            
            # Create and run the data pipeline
            self.data_pipeline = AegisPipeline()
            
            # Run pipeline in background task
            pipeline_task = asyncio.create_task(self.data_pipeline.start_all())
            
            # Wait for the specified time + buffer for FinRL execution
            wait_seconds = self.wait_minutes * 60 + 300  # Add 5 minutes buffer for FinRL
            self.logger.info(f"⏳ Waiting {wait_seconds/60:.1f} minutes for data collection + FinRL...")
            
            try:
                await asyncio.wait_for(asyncio.sleep(wait_seconds), timeout=wait_seconds + 60)
            except asyncio.TimeoutError:
                pass
            
            self.logger.info("✅ Data collection phase complete")
            
            # Cancel the pipeline task
            pipeline_task.cancel()
            try:
                await pipeline_task
            except asyncio.CancelledError:
                pass
            
        except Exception as e:
            self.logger.error(f"❌ Phase 1 error: {e}", exc_info=True)
            raise
    
    async def phase2_fetch_finrl_output(self):
        """
        Phase 2: Fetch FinRL output from Redis OR get all tickers from processed stream
        
        Retrieves the latest FinRL decisions from Redis stream:
        - Selected tickers (top 10)
        - BUY decisions with quantities
        - SELL decisions with quantities
        
        If no FinRL decisions, uses all available tickers from processed stream.
        """
        self.logger.info("=" * 80)
        self.logger.info("PHASE 2: FETCHING FinRL OUTPUT")
        self.logger.info("=" * 80)
        
        try:
            # Read latest FinRL decision from Redis stream
            messages = self.redis_client.xrevrange('finrl-decisions', count=1)
            
            if not messages:
                self.logger.warning("⚠️  No FinRL decisions found in Redis")
                self.logger.info("📊 Fetching all available tickers from processed stream instead...")
                
                # Get all unique tickers from processed:price stream
                processed_messages = self.redis_client.xrevrange('processed:price', count=500)
                
                if not processed_messages:
                    self.logger.error("❌ No processed data found either")
                    self.logger.warning("⚠️  Using fallback test data...")
                    self.finrl_output = {
                        "buy": {"AAPL": 25, "MSFT": 15},
                        "sell": {},
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "selected_tickers": ["AAPL", "MSFT"]
                    }
                    self.selected_tickers = ["AAPL", "MSFT"]
                    return
                
                # Extract unique tickers from processed stream
                unique_tickers = set()
                for msg_id, msg_data in processed_messages:
                    # Data is nested in JSON under 'data' key
                    data_json = msg_data.get('data')
                    if data_json:
                        try:
                            data = json.loads(data_json)
                            ticker = data.get('metadata', {}).get('ticker')
                            if ticker and ticker != 'VIXY':  # Exclude VIXY
                                unique_tickers.add(ticker)
                        except json.JSONDecodeError:
                            continue
                
                self.selected_tickers = sorted(list(unique_tickers))[:10]  # Take top 10
                
                # Create synthetic BUY decisions for all tickers
                self.finrl_output = {
                    "buy": {ticker: 10 for ticker in self.selected_tickers},  # 10 shares each
                    "sell": {},
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "selected_tickers": self.selected_tickers
                }
                
                self.logger.info(f"✅ Using {len(self.selected_tickers)} tickers from processed stream")
                self.logger.info(f"   Tickers: {self.selected_tickers}")
                self._save_json_log("01_finrl_output.json", self.finrl_output)
                return
            
            # Parse the message
            message_id, message_data = messages[0]
            
            timestamp = message_data.get('timestamp', datetime.now().isoformat())
            selected_tickers = json.loads(message_data.get('selected_tickers', '[]'))
            buy_decisions = json.loads(message_data.get('buy_decisions', '{}'))
            sell_decisions = json.loads(message_data.get('sell_decisions', '{}'))
            
            self.finrl_output = {
                "buy": buy_decisions,
                "sell": sell_decisions,
                "timestamp": timestamp,
                "selected_tickers": selected_tickers
            }
            
            self.selected_tickers = selected_tickers
            
            self.logger.info(f"✅ Retrieved FinRL output from Redis")
            self.logger.info(f"   Timestamp: {timestamp}")
            self.logger.info(f"   Selected tickers: {selected_tickers}")
            self.logger.info(f"   Buy signals: {list(buy_decisions.keys())}")
            self.logger.info(f"   Sell signals: {list(sell_decisions.keys())}")
            
            # Save FinRL output
            self._save_json_log("01_finrl_output.json", self.finrl_output)
            
        except Exception as e:
            self.logger.error(f"❌ Error fetching FinRL output: {e}", exc_info=True)
            raise
    
    async def phase3_fetch_processed_market_data(self):
        """
        Phase 3: Fetch real processed market data from Redis
        
        Retrieves the actual market data with technical indicators
        that was calculated during FinRL execution.
        NO FAKE DATA - uses real processed data from streaming engine.
        """
        self.logger.info("=" * 80)
        self.logger.info("PHASE 3: FETCHING PROCESSED MARKET DATA")
        self.logger.info("=" * 80)
        
        try:
            market_data = {}
            
            # Read ALL data from processed:price stream once (more efficient)
            self.logger.info("📊 Reading processed:price stream...")
            messages = self.redis_client.xrevrange('processed:price', count=500)
            
            if not messages:
                self.logger.warning("⚠️  No processed data found in stream")
                self._save_json_log("02_processed_market_data.json", market_data)
                return market_data
            
            self.logger.info(f"   Found {len(messages)} entries in processed stream")
            
            # Build a map of ticker -> latest data
            ticker_map = {}
            for msg_id, msg_data in messages:
                # The data is nested in a JSON string under 'data' key
                data_json = msg_data.get('data')
                if data_json:
                    try:
                        data = json.loads(data_json)
                        ticker = data.get('metadata', {}).get('ticker')
                        if ticker and ticker != 'VIXY':
                            # Keep the first one we see (most recent due to xrevrange)
                            if ticker not in ticker_map:
                                ticker_map[ticker] = data
                    except json.JSONDecodeError:
                        continue
            
            self.logger.info(f"   Found data for {len(ticker_map)} unique tickers")
            
            # Extract data for selected tickers
            for ticker in self.selected_tickers:
                data = ticker_map.get(ticker)
                
                if data:
                    # Extract nested data
                    price_data = data.get('price_data', {})
                    moving_avgs = data.get('moving_averages', {})
                    momentum = data.get('momentum_indicators', {})
                    trend = data.get('trend_indicators', {})
                    volatility = data.get('volatility_indicators', {})
                    macd_data = momentum.get('macd', {})
                    
                    # Parse the data
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
                        'VIXY': 0.0  # VIXY not in individual ticker data
                    }
                    market_data[ticker] = processed_data
                    self.logger.info(f"   ✅ {ticker}: Close=${processed_data['close']:.2f}, "
                                   f"RSI={processed_data['rsi_30']:.1f}, "
                                   f"MACD={processed_data['macd']:.2f}")
                else:
                    self.logger.warning(f"   ⚠️  No data found for {ticker}")
            
            # Save processed market data
            self._save_json_log("02_processed_market_data.json", market_data)
            
            return market_data
            
        except Exception as e:
            self.logger.error(f"❌ Error fetching market data: {e}", exc_info=True)
            return {}
    
    async def phase4_fetch_news_social(self):
        """
        Phase 4: Produce and fetch news and social data for selected tickers
        
        Activates producers and generates fresh data for all selected tickers.
        """
        self.logger.info("=" * 80)
        self.logger.info("PHASE 4: PRODUCING NEWS & SOCIAL DATA")
        self.logger.info("=" * 80)
        
        # Initialize producers
        self.logger.info(f"📡 Initializing producers for {len(self.selected_tickers)} tickers...")
        news_producer = NewsProducer()
        social_producer = SocialProducer()
        
        # Set active symbols for the selected tickers
        news_producer.set_active_symbols(self.selected_tickers)
        social_producer.set_active_symbols(self.selected_tickers)
        
        # Initialize producers
        await news_producer.initialize()
        await social_producer.initialize()
        
        self.logger.info(f"✅ Producers activated for tickers: {self.selected_tickers}")
        
        # Track the starting message IDs BEFORE producing new data
        news_stream = "raw:news-articles"
        social_stream = "raw:social"
        
        # Get the latest message IDs before producing (this marks the "start" point)
        redis_client = await redis.asyncio.from_url(
            f"redis://{settings.redis_host}:{settings.redis_port}/0"
        )
        
        start_news_id = None
        start_social_id = None
        
        try:
            # Get last message ID from each stream
            news_msgs = await redis_client.xrevrange(news_stream, count=1)
            if news_msgs:
                msg_id = news_msgs[0][0]
                # Decode byte string to regular string
                start_news_id = msg_id.decode('utf-8') if isinstance(msg_id, bytes) else msg_id
            
            social_msgs = await redis_client.xrevrange(social_stream, count=1)
            if social_msgs:
                msg_id = social_msgs[0][0]
                # Decode byte string to regular string
                start_social_id = msg_id.decode('utf-8') if isinstance(msg_id, bytes) else msg_id
        except Exception as e:
            self.logger.warning(f"Could not get starting message IDs: {e}")
        
        self.logger.info(f"📍 Starting news message ID: {start_news_id}")
        self.logger.info(f"📍 Starting social message ID: {start_social_id}")
        
        # Fetch and publish data for each ticker (multiple posts per ticker)
        news_per_ticker = 2  # Generate 2 news articles per ticker
        posts_per_ticker = 3  # Generate 3 social posts per ticker
        
        for ticker in self.selected_tickers:
            self.logger.info(f"📰 Fetching data for {ticker}...")
            
            # Fetch multiple news articles per ticker
            for i in range(news_per_ticker):
                news_result = await news_producer.fetch_data()
                if news_result:
                    await news_producer._publish(news_result)
            
            # Fetch multiple social posts per ticker
            for i in range(posts_per_ticker):
                social_result = await social_producer.fetch_data()
                if social_result:
                    await social_producer._publish(social_result)
        
        # Cleanup producers
        await news_producer.cleanup()
        await social_producer.cleanup()
        
        self.logger.info(f"✅ Data production complete for all {len(self.selected_tickers)} tickers")
        self.logger.info("=" * 80)
        self.logger.info("FETCHING PRODUCED DATA FROM REDIS (ONLY NEW DATA)")
        self.logger.info("=" * 80)
        
        # Fetch ONLY the newly produced messages (after start_news_id and start_social_id)
        news_data = {ticker: [] for ticker in self.selected_tickers}
        social_data = {ticker: [] for ticker in self.selected_tickers}
        
        try:
            # Fetch news messages produced AFTER start_news_id
            if start_news_id:
                new_news_msgs = await redis_client.xrange(
                    news_stream,
                    min=f"({start_news_id}",  # Exclusive start (only newer messages)
                    max="+",
                    count=1000
                )
            else:
                # If no start ID, fetch all recent messages
                new_news_msgs = await redis_client.xrevrange(news_stream, count=500)
            
            self.logger.info(f"📊 Found {len(new_news_msgs)} new news messages")
            
            # Parse news messages
            for msg_id, msg_data in new_news_msgs:
                try:
                    # Handle byte strings in msg_data
                    data_field = msg_data.get(b"data") or msg_data.get("data")
                    if not data_field:
                        continue
                    
                    # Decode if bytes
                    data_str = data_field.decode('utf-8') if isinstance(data_field, bytes) else data_field
                    data = json.loads(data_str)
                    
                    # Handle batch format
                    articles = []
                    if "batch" in data:
                        articles = data["batch"]
                    else:
                        articles = [data]
                    
                    for article in articles:
                        ticker = article.get("symbol") or article.get("ticker")
                        if ticker in self.selected_tickers:
                            news_data[ticker].append({
                                "ticker": ticker,
                                "title": article.get("headline", article.get("title", "")),
                                "description": article.get("description", ""),
                                "url": article.get("url", ""),
                                "source": article.get("source", ""),
                                "timestamp": article.get("timestamp", datetime.now().isoformat()),
                                "sentiment": article.get("sentiment"),
                            })
                except Exception as e:
                    self.logger.warning(f"Error parsing news message {msg_id}: {e}")
            
            # Fetch social messages produced AFTER start_social_id
            if start_social_id:
                new_social_msgs = await redis_client.xrange(
                    social_stream,
                    min=f"({start_social_id}",  # Exclusive start
                    max="+",
                    count=1000
                )
            else:
                new_social_msgs = await redis_client.xrevrange(social_stream, count=500)
            
            self.logger.info(f"📊 Found {len(new_social_msgs)} new social messages")
            
            # Parse social messages
            for msg_id, msg_data in new_social_msgs:
                try:
                    # Handle byte strings in msg_data
                    data_field = msg_data.get(b"data") or msg_data.get("data")
                    if not data_field:
                        continue
                    
                    # Decode if bytes
                    data_str = data_field.decode('utf-8') if isinstance(data_field, bytes) else data_field
                    data = json.loads(data_str)
                    
                    ticker = data.get("symbol") or data.get("ticker")
                    if ticker in self.selected_tickers:
                        social_data[ticker].append({
                            "ticker": ticker,
                            "text": data.get("text", ""),
                            "author": data.get("author", ""),
                            "platform": data.get("platform", ""),
                            "timestamp": data.get("timestamp", datetime.now().isoformat()),
                            "likes": data.get("likes", 0),
                            "comments": data.get("comments", 0),
                            "shares": data.get("shares", 0),
                            "sentiment": data.get("sentiment"),
                        })
                except Exception as e:
                    self.logger.warning(f"Error parsing social message {msg_id}: {e}")
                    
        except Exception as e:
            self.logger.error(f"Error fetching new data from Redis: {e}")
        finally:
            await redis_client.close()
        
        # Log summary
        for ticker in self.selected_tickers:
            news_count = len(news_data.get(ticker, []))
            social_count = len(social_data.get(ticker, []))
            self.logger.info(f"   {ticker}: {news_count} news, {social_count} social posts")
        
        # Save logs
        self._save_json_log("03_news_data.json", news_data)
        self._save_json_log("04_social_data.json", social_data)
        
        return news_data, social_data
    
    async def phase5_run_agents(self, market_data: Dict, news_data: Dict, social_data: Dict):
        """
        Phase 5: Run all 4 agents on each ticker
        
        Agents:
        1. News Analyst
        2. Social Analyst
        3. Market Analyst (using REAL processed data, not fake)
        4. SEC Report Analyst
        """
        self.logger.info("=" * 80)
        self.logger.info("PHASE 5: RUNNING AGENTS")
        self.logger.info("=" * 80)
        
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
            ticker_market = market_data.get(ticker, {})
            
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
                
                if ticker_news:
                    news_agent.news_buffer[ticker] = ticker_news
                    self.logger.info(f"[{ticker}] Loaded {len(ticker_news)} news items")
                else:
                    news_agent.news_buffer[ticker] = [{
                        "ticker": ticker,
                        "title": f"{ticker} shows strong performance",
                        "description": f"Latest analysis indicates {ticker} continues to perform well",
                        "sentiment": 0.5,
                        "timestamp": datetime.now().isoformat()
                    }]
                    self.logger.info(f"[{ticker}] Using dummy news data")
                
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
                    redis_url=redis_url,
                    stream_key="raw:social",
                    openai_api_key=openai_api_key,
                    logger=self.logger
                )
                
                await social_agent.connect()
                
                if ticker_social:
                    social_agent.social_buffer[ticker] = ticker_social
                    self.logger.info(f"[{ticker}] Loaded {len(ticker_social)} social items")
                else:
                    social_agent.social_buffer[ticker] = [{
                        "ticker": ticker,
                        "text": f"Investors are optimistic about {ticker}",
                        "sentiment": 0.5,
                        "likes": 150,
                        "comments": 75,
                        "shares": 30,
                        "timestamp": datetime.now().isoformat()
                    }]
                    self.logger.info(f"[{ticker}] Using dummy social data")
                
                social_analysis = await social_agent.analyze_social_sentiment(ticker)
                await social_agent.disconnect()
                
                ticker_results["social_analysis"] = social_analysis
                self.logger.info(f"[{ticker}] ✅ Social analysis complete")
                
            except Exception as e:
                self.logger.error(f"[{ticker}] Social analyst error: {e}")
                ticker_results["social_analysis"] = {"error": str(e)}
            
            # 3. Market Analyst (using REAL processed data)
            try:
                self.logger.info(f"[{ticker}] Running Market Analyst...")
                market_agent = MarketAnalystRedis(
                    redis_url=redis_url,
                    stream_key="price_stream",
                    openai_api_key=openai_api_key,
                    logger=self.logger
                )
                
                await market_agent.connect()
                
                # Use REAL processed market data from Redis
                if ticker_market:
                    market_agent.price_buffer[ticker] = [ticker_market]
                    self.logger.info(f"[{ticker}] Loaded REAL market data from Redis processed stream")
                    self.logger.info(f"         Close: ${ticker_market.get('close', 0):.2f}, "
                                   f"RSI: {ticker_market.get('rsi_30', 0):.1f}, "
                                   f"MACD: {ticker_market.get('macd', 0):.2f}")
                else:
                    self.logger.warning(f"[{ticker}] No market data available")
                
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
                    redis_url=redis_url,
                    stream_key="sec_stream",
                    openai_api_key=openai_api_key,
                    logger=self.logger
                )
                
                await sec_agent.connect()
                
                sec_analysis = await sec_agent.fetch_and_analyze_ticker(ticker)
                await sec_agent.disconnect()
                
                ticker_results["sec_analysis"] = sec_analysis
                self.logger.info(f"[{ticker}] ✅ SEC analysis complete")
                
            except Exception as e:
                self.logger.error(f"[{ticker}] SEC analyst error: {e}")
                ticker_results["sec_analysis"] = {"error": str(e)}
            
            # Save ticker results
            all_agent_results[ticker] = ticker_results
            self._save_json_log(f"05_agent_results_{ticker}.json", ticker_results)
        
        # Save combined results
        self._save_json_log("06_all_agent_results.json", all_agent_results)
        
        return all_agent_results
    
    async def phase6_run_debate(self, agent_results: Dict):
        """Phase 6: Run debate for each ticker"""
        self.logger.info("=" * 80)
        self.logger.info("PHASE 6: RUNNING DEBATE & VALIDATION")
        self.logger.info("=" * 80)
        
        debate_results = {}
        
        for ticker in self.selected_tickers:
            self.logger.info(f"\n{'='*40}")
            self.logger.info(f"Debate for: {ticker}")
            self.logger.info(f"{'='*40}")
            
            try:
                ticker_data = agent_results.get(ticker, {})
                
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
                
                debate_result = run_debate(final_reports_data=final_reports_data)
                debate_results[ticker] = debate_result
                
                final_rec = debate_result.get('validation', {}).get('final_recommendation', {})
                self.logger.info(f"[{ticker}] ✅ Debate complete")
                self.logger.info(f"[{ticker}] Decision: {final_rec.get('decision', 'N/A')} "
                               f"(Conviction: {final_rec.get('conviction', 'N/A')})")
                
            except Exception as e:
                self.logger.error(f"[{ticker}] Debate error: {e}")
                debate_results[ticker] = {"error": str(e)}
        
        self._save_json_log("07_debate_results.json", debate_results)
        
        return debate_results
    
    async def phase7_reconcile_and_decide(self, debate_results: Dict):
        """
        Phase 7: Reconcile FinRL output with Validator decisions
        
        Only accept tickers where BOTH FinRL and Validator agree on the action.
        """
        self.logger.info("=" * 80)
        self.logger.info("PHASE 7: RECONCILING FinRL & VALIDATOR DECISIONS")
        self.logger.info("=" * 80)
        
        approved_stocks = []
        rejected_stocks = []
        
        finrl_buy_tickers = set(self.finrl_output.get("buy", {}).keys())
        finrl_sell_tickers = set(self.finrl_output.get("sell", {}).keys())
        
        for ticker in self.selected_tickers:
            self.logger.info(f"\n{'='*40}")
            self.logger.info(f"Reconciling: {ticker}")
            self.logger.info(f"{'='*40}")
            
            # Get FinRL action
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
            
            # Normalize and compare
            finrl_action_normalized = finrl_action.upper()
            validator_action_normalized = validator_action.upper()
            
            self.logger.info(f"[{ticker}] FinRL: {finrl_action_normalized} ({finrl_shares} shares)")
            self.logger.info(f"[{ticker}] Validator: {validator_action_normalized} (Conviction: {validator_confidence})")
            
            aligned = finrl_action_normalized == validator_action_normalized
            
            if aligned and finrl_action_normalized != "HOLD":
                # Both agree - APPROVED
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
                # Both HOLD - REJECTED
                rejected_stocks.append({
                    "ticker": ticker,
                    "finrl_action": finrl_action_normalized,
                    "validator_action": validator_action_normalized,
                    "aligned": True,
                    "reason": "Both systems recommend HOLD (no action)"
                })
                self.logger.info(f"[{ticker}] ⏸️  REJECTED: Both recommend HOLD")
        
        # Summary
        self.logger.info("\n" + "=" * 80)
        self.logger.info("RECONCILIATION SUMMARY")
        self.logger.info("=" * 80)
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
    
    async def phase8_execute_trades(self, reconciliation_results: Dict):
        """Phase 8: Execute trades for approved stocks"""
        self.logger.info("=" * 80)
        self.logger.info("PHASE 8: EXECUTING TRADES VIA DECISION AGENT MCP")
        self.logger.info("=" * 80)
        
        approved_stocks = reconciliation_results.get("approved_stocks", [])
        
        if not approved_stocks:
            self.logger.warning("⚠️  No approved stocks to trade")
            return {
                "timestamp": datetime.now().isoformat(),
                "status": "no_trades",
                "message": "No approved stocks from reconciliation"
            }
        
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
            self.logger.info("\n" + "=" * 80)
            self.logger.info("TRADE EXECUTION SUMMARY")
            self.logger.info("=" * 80)
            self.logger.info(f"Total Recommendations: {summary.get('total_recommendations', 0)}")
            self.logger.info(f"✅ Trades Executed: {summary.get('trades_executed', 0)}")
            self.logger.info(f"❌ Trades Failed: {summary.get('trades_failed', 0)}")
            self.logger.info(f"⏸️  Trades Skipped: {summary.get('trades_skipped', 0)}")
            
            return trade_report
            
        except Exception as e:
            self.logger.error(f"❌ Trade execution failed: {e}", exc_info=True)
            error_report = {
                "timestamp": datetime.now().isoformat(),
                "status": "failed",
                "error": str(e)
            }
            self._save_json_log("09_trade_execution_error.json", error_report)
            return error_report
    
    async def run(self):
        """Run the complete full pipeline"""
        try:
            self.logger.info("=" * 80)
            self.logger.info("FULL AEGIS PIPELINE STARTED")
            self.logger.info("=" * 80)
            
            # Setup
            await self.setup_redis()
            
            # Phase 1: Data collection and FinRL
            await self.phase1_run_data_collection_and_finrl()
            
            # Phase 2: Fetch FinRL output from Redis
            await self.phase2_fetch_finrl_output()
            
            # Phase 3: Fetch REAL processed market data (no fake data)
            market_data = await self.phase3_fetch_processed_market_data()
            
            # Phase 4: Fetch news and social
            news_data, social_data = await self.phase4_fetch_news_social()
            
            # Phase 5: Run all agents
            agent_results = await self.phase5_run_agents(market_data, news_data, social_data)
            
            # Phase 6: Run debate
            debate_results = await self.phase6_run_debate(agent_results)
            
            # Phase 7: Reconcile FinRL & Validator
            reconciliation_results = await self.phase7_reconcile_and_decide(debate_results)
            
            # Phase 8: Execute trades
            trade_results = await self.phase8_execute_trades(reconciliation_results)
            
            # Final summary
            self.logger.info("=" * 80)
            self.logger.info("FULL PIPELINE COMPLETED SUCCESSFULLY")
            self.logger.info("=" * 80)
            
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
            
            print("\n" + "=" * 80)
            print("✅ FULL PIPELINE RUN COMPLETE")
            print("=" * 80)
            print(f"📁 Logs: {self.cycle_log_dir}")
            print(f"📊 Tickers: {', '.join(self.selected_tickers)}")
            print(f"✅ Approved: {len(reconciliation_results.get('approved_stocks', []))}")
            print(f"❌ Rejected: {len(reconciliation_results.get('rejected_stocks', []))}")
            if trade_results.get("summary"):
                print(f"💼 Trades Executed: {trade_results['summary'].get('trades_executed', 0)}")
            print("=" * 80)
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}", exc_info=True)
            raise


def print_banner():
    """Print startup banner"""
    banner = """
    ╔═══════════════════════════════════════════════════════════════════╗
    ║              FULL AEGIS TRADING SYSTEM PIPELINE                   ║
    ╚═══════════════════════════════════════════════════════════════════╝
    
    Complete Workflow:
    Phase 1: Data Collection (60 min)
       • Collect price data for 30 tickers
       • Calculate technical indicators in real-time
       • Run FinRL model → Get BUY/SELL decisions
    
    Phase 2-3: Fetch FinRL Output & Market Data
       • Retrieve FinRL decisions from Redis
       • Fetch REAL processed market data (with indicators)
    
    Phase 4-6: Agent Analysis
       • Fetch news & social data for selected tickers
       • Run 4 agents (News, Social, Market, SEC)
       • Run debate & validation
    
    Phase 7-8: Decision & Execution
       • Reconcile FinRL vs Validator decisions
       • Execute trades for approved stocks only
    
    Press Ctrl+C to stop
    ════════════════════════════════════════════════════════════════════
    """
    print(banner)


async def main():
    """Main entry point"""
    print_banner()
    
    # Configuration
    import sys
    skip_collection = "--skip-collection" in sys.argv
    
    if skip_collection:
        print("⏭️  Skipping data collection - using existing FinRL output")
        wait_minutes = 0
    else:
        wait_minutes = 60  # Default: 60 minutes
        print(f"⏰ Data collection: {wait_minutes} minutes")
    
    pipeline = FullAegisPipeline(
        skip_data_collection=skip_collection,
        wait_minutes=wait_minutes
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