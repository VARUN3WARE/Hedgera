"""
FinRL Service - Integrated with actual FinRL paper trading code.
Implements workflow: Collect data for 1 hour → Run FinRL → Get 10 tickers
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import pandas as pd
import sys
from pathlib import Path

# Add finrl_integration to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "finrl_integration"))

try:
    from redis import asyncio as aioredis
except ImportError:
    import redis.asyncio as aioredis

from backend.config.settings import settings
from backend.config.logging_setup import log_data

logger = logging.getLogger(__name__)


class FinRLIntegratedService:
    """FinRL Service with real paper trading integration."""
    
    def __init__(
        self,
        model_path: Optional[str] = None,
        ticker_list: Optional[List[str]] = None,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        collection_time_minutes: int = 60,  # Collect data for 1 hour
        run_interval_hours: int = 2,  # Run every 2 hours
        news_producer=None,  # Reference to news producer
        social_producer=None  # Reference to social producer
    ):
        """Initialize FinRL service."""
        self.model_path = model_path or settings.finrl_model_path
        
        # Separate VIXY from trading tickers
        # FinRL model expects exactly 30 tickers for trading
        # VIXY is only used for turbulence calculation
        all_symbols = ticker_list or settings.symbols_list
        self.ticker_list = [t for t in all_symbols if t != 'VIXY'][:30]  # Exactly 30 trading tickers
        self.all_symbols = all_symbols  # Keep all symbols for fetching
        
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_client = None
        self.collection_time_minutes = collection_time_minutes
        self.run_interval_hours = run_interval_hours
        
        # Producer references for activating after ticker selection
        self.news_producer = news_producer
        self.social_producer = social_producer
        
        # FinRL indicators (from finrl/config.py)
        self.tech_indicators = [
            'macd', 'boll_ub', 'boll_lb', 'rsi_30',
            'cci_30', 'dx_30', 'close_30_sma', 'close_60_sma'
        ]
        
        # Alpaca credentials
        self.api_key = settings.alpaca_api_key
        self.api_secret = settings.alpaca_secret_key
        self.api_base_url = settings.alpaca_base_url
        
        # Data loggers
        self.finrl_decisions_log = None
        
        # Concurrency control - prevent duplicate executions
        self.is_running = False
        
        logger.info(f"🤖 FinRL Integrated Service initialized")
        logger.info(f"   Model: {self.model_path}")
        logger.info(f"   Trading Tickers: {len(self.ticker_list)} symbols (30 DOW)")
        logger.info(f"   All Symbols: {len(self.all_symbols)} symbols (including VIXY for turbulence)")
        logger.info(f"   Collection time: {collection_time_minutes} minutes")
        logger.info(f"   Run interval: {run_interval_hours} hours")
    
    def set_logging(self, finrl_log_path: str):
        """Set logging paths."""
        self.finrl_decisions_log = finrl_log_path
    
    async def connect_redis(self):
        """Connect to Redis."""
        self.redis_client = await aioredis.from_url(
            f"redis://{self.redis_host}:{self.redis_port}",
            decode_responses=True
        )
        await self.redis_client.ping()
        logger.info("✅ FinRL connected to Redis")
    
    async def wait_for_data_collection(self):
        """Wait for data collection period (1 hour)."""
        logger.info(f"⏰ Waiting {self.collection_time_minutes} minutes for data collection...")
        logger.info(f"   Data collection will complete at: {(datetime.now() + timedelta(minutes=self.collection_time_minutes)).strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Show progress every 10 minutes
        remaining = self.collection_time_minutes
        while remaining > 0:
            wait_chunk = min(10, remaining)
            await asyncio.sleep(wait_chunk * 60)
            remaining -= wait_chunk
            if remaining > 0:
                logger.info(f"⏱️  Data collection in progress... {remaining} minutes remaining")
        
        logger.info("✅ Data collection period complete! Fetching data for FinRL...")
    
    async def fetch_data_from_redis(self) -> Optional[pd.DataFrame]:
        """
        Fetch processed market data from Redis.
        Returns DataFrame in FinRL format with EXACTLY 30 trading tickers + VIXY.
        """
        try:
            # Read all available data from processed stream
            # Fetch more messages to ensure we get all symbols
            messages = await self.redis_client.xrevrange(
                'processed:price',
                count=len(self.all_symbols) * 2
            )
            
            if not messages:
                logger.warning("⚠️  No data in processed:price stream")
                return None
            
            # Build ticker data dictionary
            ticker_data = {}
            vixy_value = None  # Track VIXY separately for turbulence
            
            for message_id, fields in messages:
                try:
                    data = json.loads(fields.get('data', '{}'))
                    ticker = data['metadata']['ticker']
                    
                    # Handle VIXY separately (used ONLY for turbulence calculation)
                    if ticker == 'VIXY':
                        price_data = data.get('price_data', {})
                        vixy_value = price_data.get('close', 0)
                        # Add VIXY to DataFrame for turbulence calculation in paper_trading.py
                        ticker_data['VIXY'] = {
                            'tic': 'VIXY',
                            'open': price_data.get('open', 0),
                            'high': price_data.get('high', 0),
                            'low': price_data.get('low', 0),
                            'close': price_data.get('close', 0),
                            'volume': price_data.get('volume', 0),
                            'macd': 0,
                            'boll_ub': 0,
                            'boll_lb': 0,
                            'rsi_30': 50,
                            'cci_30': 0,
                            'dx_30': 0,
                            'close_30_sma': price_data.get('close', 0),
                            'close_60_sma': price_data.get('close', 0),
                        }
                        continue
                    
                    # Only include the 30 trading tickers (NOT VIXY)
                    if ticker not in self.ticker_list:
                        continue
                    
                    # Skip if already have this ticker (take most recent)
                    if ticker in ticker_data:
                        continue
                    
                    price_data = data.get('price_data', {})
                    momentum = data.get('momentum_indicators', {})
                    volatility = data.get('volatility_indicators', {})
                    trend = data.get('trend_indicators', {})
                    moving_averages = data.get('moving_averages', {})
                    
                    # Build record matching FinRL expectations
                    ticker_data[ticker] = {
                        'tic': ticker,
                        'open': price_data.get('open', 0),
                        'high': price_data.get('high', 0),
                        'low': price_data.get('low', 0),
                        'close': price_data.get('close', 0),
                        'volume': price_data.get('volume', 0),
                        'macd': momentum.get('macd', {}).get('macd_line', 0),
                        'boll_ub': volatility.get('boll_ub', 0),
                        'boll_lb': volatility.get('boll_lb', 0),
                        'rsi_30': momentum.get('rsi_30', 50),
                        'cci_30': momentum.get('cci_30', 0),
                        'dx_30': trend.get('dx_30', 0),
                        'close_30_sma': moving_averages.get('close_30_sma', price_data.get('close', 0)),
                        'close_60_sma': moving_averages.get('close_60_sma', price_data.get('close', 0)),
                    }
                except Exception as e:
                    logger.warning(f"⚠️  Error parsing message: {e}")
                    continue
            
            if not ticker_data:
                logger.warning("⚠️  No valid ticker data found")
                return None
            
            # Convert to DataFrame
            df = pd.DataFrame(list(ticker_data.values()))
            
            # Count trading tickers (exclude VIXY)
            trading_tickers = [t for t in df['tic'].tolist() if t != 'VIXY']
            
            logger.info(f"✅ Fetched data for {len(trading_tickers)} trading tickers + VIXY")
            logger.info(f"   Columns: {list(df.columns)}")
            logger.info(f"   Trading tickers: {trading_tickers[:5]}...")
            if vixy_value:
                logger.info(f"   VIXY close (turbulence indicator): {vixy_value}")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Error fetching from Redis: {e}", exc_info=True)
            return None
    
    def run_finrl_model(self, market_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Run actual FinRL paper trading model.
        Uses the real FinRL code from paper_trading.py
        """
        try:
            # Count trading tickers (exclude VIXY)
            trading_tickers = [t for t in market_data['tic'].tolist() if t != 'VIXY']
            
            logger.info("🤖 Running FinRL Paper Trading Model...")
            logger.info(f"   Input: {len(trading_tickers)} trading tickers + VIXY (turbulence)")
            logger.info(f"   Model: {self.model_path}")
            logger.info(f"   Expected state dim: 301 (1 + 30*2 + 30*8)")
            
            # Import FinRL trading function
            from paper_trading import get_trading_decisions
            
            # Call actual FinRL model
            # ticker_list should be exactly 30 trading tickers (no VIXY)
            # market_data DataFrame includes VIXY for turbulence calculation
            decisions = get_trading_decisions(
                redis_data=market_data,
                model_path=self.model_path,
                ticker_list=self.ticker_list,  # 30 trading tickers only
                tech_indicators=self.tech_indicators,
                api_key=self.api_key,
                api_secret=self.api_secret,
                api_base_url=self.api_base_url
            )
            
            # Extract selected tickers (top 10)
            buy_tickers = list(decisions.get('buy', {}).keys())
            sell_tickers = list(decisions.get('sell', {}).keys())
            all_selected = list(set(buy_tickers + sell_tickers))
            selected_tickers = all_selected[:settings.finrl_output_tickers]
            
            logger.info(f"✅ FinRL Model Complete")
            logger.info(f"   Buy signals: {len(buy_tickers)} tickers")
            logger.info(f"   Sell signals: {len(sell_tickers)} tickers")
            logger.info(f"   Selected top {len(selected_tickers)} tickers: {selected_tickers}")
            
            # Activate news and social producers for selected tickers
            if selected_tickers:
                if self.news_producer:
                    self.news_producer.set_active_symbols(selected_tickers)
                if self.social_producer:
                    self.social_producer.set_active_symbols(selected_tickers)
                logger.info(f"✅ Activated news/social producers for {len(selected_tickers)} selected tickers")
            
            # Log detailed decisions
            for ticker in selected_tickers[:5]:
                if ticker in decisions['buy']:
                    logger.info(f"      {ticker}: BUY {decisions['buy'][ticker]} shares")
                elif ticker in decisions['sell']:
                    logger.info(f"      {ticker}: SELL {decisions['sell'][ticker]} shares")
            
            return {
                'timestamp': decisions.get('timestamp', datetime.now().isoformat()),
                'selected_tickers': selected_tickers,
                'buy_decisions': decisions.get('buy', {}),
                'sell_decisions': decisions.get('sell', {}),
                'total_analyzed': len(market_data)
            }
            
        except Exception as e:
            logger.error(f"❌ FinRL model error: {e}", exc_info=True)
            return {
                'timestamp': datetime.now().isoformat(),
                'selected_tickers': [],
                'buy_decisions': {},
                'sell_decisions': {},
                'error': str(e)
            }
    
    async def publish_to_redis(self, decisions: Dict[str, Any]) -> bool:
        """Publish FinRL decisions to Redis."""
        try:
            message_data = {
                'timestamp': decisions.get('timestamp'),
                'selected_tickers': json.dumps(decisions.get('selected_tickers', [])),
                'buy_decisions': json.dumps(decisions.get('buy_decisions', {})),
                'sell_decisions': json.dumps(decisions.get('sell_decisions', {})),
                'total_analyzed': decisions.get('total_analyzed', 0)
            }
            
            stream_id = await self.redis_client.xadd('finrl-decisions', message_data)
            logger.info(f"✅ Published to finrl-decisions stream: {stream_id}")
            
            # Store as latest
            await self.redis_client.set(
                'finrl:latest',
                json.dumps(decisions),
                ex=7200
            )
            
            # Log to file
            if self.finrl_decisions_log:
                log_data(self.finrl_decisions_log, decisions)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Publish failed: {e}", exc_info=True)
            return False
    
    async def run_finrl_immediate(self) -> Dict[str, Any]:
        """Run FinRL IMMEDIATELY on current data (trigger-based, no wait)."""
        if self.is_running:
            logger.warning("⚠️  FinRL already running, skipping immediate trigger execution")
            return {
                'success': False,
                'error': 'FinRL already running',
                'timestamp': datetime.now().isoformat()
            }
        
        self.is_running = True
        try:
            cycle_start = datetime.now()
            logger.info("=" * 80)
            logger.info(f"🚀 IMMEDIATE FinRL Trigger Execution at {cycle_start.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("   (Running on current data - NO WAIT)")
            logger.info("=" * 80)
            
            # Step 1: Fetch current data immediately (no wait)
            market_data = await self.fetch_data_from_redis()
            if market_data is None or len(market_data) == 0:
                logger.error("❌ No market data available for immediate execution")
                return {
                    'success': False,
                    'error': 'No data available',
                    'timestamp': cycle_start.isoformat()
                }
            
            logger.info(f"✅ Fetched current data: {len(market_data)} records")
            
            # Step 2: Run FinRL model immediately
            decisions = self.run_finrl_model(market_data)
            if 'error' in decisions:
                return {
                    'success': False,
                    'error': decisions['error'],
                    'timestamp': cycle_start.isoformat()
                }
            
            # Step 3: Publish results
            publish_success = await self.publish_to_redis(decisions)
            
            cycle_end = datetime.now()
            cycle_duration = (cycle_end - cycle_start).total_seconds()
            
            logger.info("=" * 80)
            logger.info(f"✅ Immediate FinRL Execution Completed in {cycle_duration:.2f}s")
            logger.info(f"   Selected Tickers: {decisions.get('selected_tickers', [])}")
            logger.info("=" * 80)
            
            return {
                'success': publish_success,
                'timestamp': cycle_start.isoformat(),
                'duration_seconds': cycle_duration,
                'selected_tickers': decisions.get('selected_tickers', []),
                'execution_type': 'immediate_trigger'
            }
        finally:
            self.is_running = False
    
    async def run_finrl(self) -> Dict[str, Any]:
        """Run FinRL cycle if not already running (trigger-based execution)."""
        if self.is_running:
            logger.warning("⚠️  FinRL already running, skipping trigger-based execution")
            return {
                'success': False,
                'error': 'FinRL already running',
                'timestamp': datetime.now().isoformat()
            }
        
        return await self.run_cycle()
    
    async def run_cycle(self) -> Dict[str, Any]:
        """Run complete FinRL cycle with data collection wait."""
        self.is_running = True
        try:
            cycle_start = datetime.now()
            logger.info("=" * 80)
            logger.info(f"🚀 Starting FinRL Cycle at {cycle_start.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 80)
            
            # Step 1: Wait for data collection (1 hour)
            await self.wait_for_data_collection()
            
            # Step 2: Fetch accumulated data
            market_data = await self.fetch_data_from_redis()
            if market_data is None or len(market_data) == 0:
                logger.error("❌ No market data available after collection period")
                return {
                    'success': False,
                    'error': 'No data after collection',
                    'timestamp': cycle_start.isoformat()
                }
            
            # Step 3: Run FinRL model
            decisions = self.run_finrl_model(market_data)
            if 'error' in decisions:
                return {
                    'success': False,
                    'error': decisions['error'],
                    'timestamp': cycle_start.isoformat()
                }
            
            # Step 4: Publish results
            publish_success = await self.publish_to_redis(decisions)
            
            cycle_end = datetime.now()
            cycle_duration = (cycle_end - cycle_start).total_seconds()
            
            logger.info("=" * 80)
            logger.info(f"✅ FinRL Cycle Completed in {cycle_duration:.2f}s")
            logger.info(f"   Selected Tickers: {decisions.get('selected_tickers', [])}")
            logger.info("=" * 80)
            
            return {
                'success': publish_success,
                'timestamp': cycle_start.isoformat(),
                'duration_seconds': cycle_duration,
                'selected_tickers': decisions.get('selected_tickers', [])
            }
        finally:
            self.is_running = False
    
    async def start_service(self):
        """Start FinRL service with periodic execution."""
        logger.info("🚀 Starting FinRL Integrated Service")
        
        await self.connect_redis()
        
        while True:
            try:
                await self.run_cycle()
                
                wait_time = self.run_interval_hours * 3600
                logger.info(f"⏰ Waiting {self.run_interval_hours} hours until next FinRL cycle...")
                await asyncio.sleep(wait_time)
            except Exception as e:
                logger.error(f"❌ Service error: {e}", exc_info=True)
                await asyncio.sleep(60)


def create_finrl_service(**kwargs):
    """Create FinRL service instance."""
    return FinRLIntegratedService(**kwargs)