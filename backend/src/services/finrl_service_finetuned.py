"""
Updated FinRL Service with Fine-tuning Support

Uses:
1. Fine-tuned model from finetuning_service (if available)
2. Scaled predictions: (action * MAX_STOCK).astype(int)
3. Direct PPO model prediction (no wrapper)
4. SHAP & LIME explainability (logs top 10 features for every decision)
5. Same input/output as before (Redis integration preserved)
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path
import pandas as pd
import numpy as np

try:
    from redis import asyncio as aioredis
except ImportError:
    import redis.asyncio as aioredis

from stable_baselines3 import PPO

from backend.config.settings import settings
from backend.config.logging_setup import log_data
from backend.src.services.explainability_service import ExplainabilityService

logger = logging.getLogger(__name__)


class FinRLServiceWithFineTuning:
    """FinRL Service using fine-tuned PPO model with scaled predictions."""
    
    def __init__(
        self,
        news_producer=None,
        social_producer=None
    ):
        """Initialize FinRL service."""
        # Model paths
        base_path = settings.finrl_model_path
        self.base_model_path = base_path.replace('.zip', '') if base_path.endswith('.zip') else base_path
        self.finetuned_model_path = str(Path(self.base_model_path).parent / "agent_ppo_finetuned")
        
        # Tickers (30 trading tickers only, no VIXY)
        self.ticker_list = settings.symbols_list[:30]  # Ensure exactly 30 tickers
        self.all_symbols = settings.symbols_list
        
        # Redis config
        self.redis_host = settings.redis_host
        self.redis_port = settings.redis_port
        self.redis_client = None
        self.run_interval_hours = 2
        
        # Trading parameters
        self.max_stock = 100
        self.min_action = 10  # Threshold for buy/sell
        
        # Technical indicators
        self.tech_indicators = [
            'macd', 'boll_ub', 'boll_lb', 'rsi_30',
            'cci_30', 'dx_30', 'close_30_sma', 'close_60_sma'
        ]
        
        # Producer references
        self.news_producer = news_producer
        self.social_producer = social_producer
        
        # Model
        self.model = None
        self.current_model_path = None  # Track which model is currently loaded
        self.last_reload_time = 0  # Track when we last reloaded the model
        
        # Running state
        self.is_running = False
        
        # Data loggers
        self.finrl_decisions_log = None
        
        # Explainability service (with JSONL logging)
        logger.info("   🔧 Initializing explainability service...")
        explainer_dir = Path(self.base_model_path).parent / "explainers"
        logger.info(f"      Explainer directory: {explainer_dir}")
        logger.info(f"      Log directory: logs/explainability")
        
        try:
            self.explainability = ExplainabilityService(
                ticker_list=self.ticker_list,
                tech_indicators=self.tech_indicators,
                explainer_dir=str(explainer_dir),
                log_dir="logs/explainability"
            )
            logger.info("   ✅ Explainability service ready")
        except Exception as e:
            logger.error(f"   ❌ Explainability service failed: {e}")
            logger.exception("Full traceback:")
            raise
        
        logger.info(f"🤖 FinRL Service (Fine-tuned) initialized")
        logger.info(f"   Base model: {self.base_model_path}")
        logger.info(f"   Fine-tuned model: {self.finetuned_model_path}")
        logger.info(f"   Trading Tickers: {len(self.ticker_list)} symbols")
        logger.info(f"   Max stock: {self.max_stock}")
        logger.info(f"   Explainability: SHAP (50 samples) + LIME (100 samples)")
    
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
    
    def load_model(self):
        """Load fine-tuned model if available, otherwise base model."""
        try:
            # Try fine-tuned model first
            if Path(self.finetuned_model_path).exists():
                logger.info(f"🎓 Loading fine-tuned model: {self.finetuned_model_path}")
                self.model = PPO.load(self.finetuned_model_path)
                self.current_model_path = self.finetuned_model_path
                logger.info("✅ Fine-tuned model loaded")
            else:
                logger.info(f"🤖 Fine-tuned model not found, loading base model: {self.base_model_path}")
                self.model = PPO.load(self.base_model_path)
                self.current_model_path = self.base_model_path
                logger.info("✅ Base model loaded")
            
            return True
        except Exception as e:
            logger.error(f"❌ Failed to load model: {e}")
            return False
    
    def reload_model_if_updated(self):
        """Check if fine-tuned model was updated and reload it."""
        try:
            # Check if fine-tuned model exists
            if Path(self.finetuned_model_path).exists():
                finetuned_mtime = Path(self.finetuned_model_path).stat().st_mtime
                
                # Reload if:
                # 1. We're using base model (switch to fine-tuned)
                # 2. Fine-tuned was updated since our last reload
                if (self.current_model_path == self.base_model_path) or \
                   (finetuned_mtime > self.last_reload_time):
                    logger.info("🔄 Reloading fine-tuned model (updated or newly available)...")
                    self.model = PPO.load(self.finetuned_model_path)
                    self.current_model_path = self.finetuned_model_path
                    self.last_reload_time = datetime.now().timestamp()
                    logger.info("✅ Fine-tuned model reloaded successfully")
                    return True
            return False
        except Exception as e:
            logger.warning(f"⚠️  Model reload check failed: {e}")
            return False
    
    async def fetch_data_from_redis(self) -> Optional[pd.DataFrame]:
        """
        Fetch market data with simple fallback strategy:
        1. Try Redis first (has live data from last 24 hours)
        2. If Redis is empty, get last 3 hours from MongoDB
        """
        try:
            # STEP 1: Try Redis first (live data with indicators)
            messages = await self.redis_client.xrevrange(
                'processed:price',
                count=len(self.all_symbols) * 2
            )
            
            if messages:
                # Redis has data - use it
                ticker_data = {}
                
                for message_id, fields in messages:
                    try:
                        data = json.loads(fields.get('data', '{}'))
                        ticker = data['metadata']['ticker']
                        
                        if ticker not in self.all_symbols or ticker in ticker_data:
                            continue
                        
                        price_data = data.get('price_data', {})
                        momentum = data.get('momentum_indicators', {})
                        volatility = data.get('volatility_indicators', {})
                        trend = data.get('trend_indicators', {})
                        moving_averages = data.get('moving_averages', {})
                        
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
                            'close_30_sma': moving_averages.get('close_30_sma', 0),
                            'close_60_sma': moving_averages.get('close_60_sma', 0),
                        }
                    except Exception as e:
                        logger.warning(f"⚠️  Error parsing Redis message: {e}")
                        continue
                
                if ticker_data:
                    df = pd.DataFrame(list(ticker_data.values()))
                    logger.info(f"✅ Fetched from REDIS: {len(df)} tickers")
                    
                    # Log sample to verify indicators
                    if self.ticker_list and self.ticker_list[0] in ticker_data:
                        sample = ticker_data[self.ticker_list[0]]
                        logger.info(f"   Sample ({self.ticker_list[0]}): "
                                  f"Close=${sample['close']:.2f}, "
                                  f"SMA30=${sample['close_30_sma']:.2f}, "
                                  f"SMA60=${sample['close_60_sma']:.2f}, "
                                  f"RSI={sample['rsi_30']:.1f}")
                    
                    return df
            
            # STEP 2: Redis is empty - fallback to MongoDB (last 3 hours)
            logger.warning("⚠️  Redis stream is empty, fetching last 3 hours from MongoDB...")
            
            from pymongo import MongoClient
            from backend.config.settings import settings
            from datetime import datetime, timedelta
            
            mongo_client = MongoClient(settings.mongodb_uri_streaming)
            db = mongo_client[settings.mongodb_db_name]
            collection = db[settings.mongodb_collection_name]
            
            # Get data from last 3 hours (use local time to match MongoDB storage)
            # MongoDB stores dates in local timezone, not UTC
            cutoff_time = datetime.now() - timedelta(hours=3)
            
            ticker_data = {}
            for ticker in self.all_symbols:
                # Get most recent document for this ticker (within last 3 hours)
                doc = collection.find_one(
                    {'tic': ticker, 'date': {'$gte': cutoff_time}},
                    sort=[('date', -1)]
                )
                
                if doc:
                    ticker_data[ticker] = {
                        'tic': ticker,
                        'open': float(doc.get('open', 0)),
                        'high': float(doc.get('high', 0)),
                        'low': float(doc.get('low', 0)),
                        'close': float(doc.get('close', 0)),
                        'volume': int(doc.get('volume', 0)),
                        'macd': float(doc.get('macd', 0)),
                        'boll_ub': float(doc.get('boll_ub', 0)),
                        'boll_lb': float(doc.get('boll_lb', 0)),
                        'rsi_30': float(doc.get('rsi_30', 50)),
                        'cci_30': float(doc.get('cci_30', 0)),
                        'dx_30': float(doc.get('dx_30', 0)),
                        'close_30_sma': float(doc.get('close_30_sma', 0)),
                        'close_60_sma': float(doc.get('close_60_sma', 0)),
                    }
            
            mongo_client.close()
            
            if not ticker_data:
                logger.warning("⚠️  No data found in MongoDB either (last 3 hours)")
                return None
            
            df = pd.DataFrame(list(ticker_data.values()))
            logger.info(f"✅ Fetched from MONGODB (last 3h): {len(df)} tickers")
            
            # Log sample to verify indicators
            if self.ticker_list and self.ticker_list[0] in ticker_data:
                sample = ticker_data[self.ticker_list[0]]
                logger.info(f"   Sample ({self.ticker_list[0]}): "
                          f"Close=${sample['close']:.2f}, "
                          f"SMA30=${sample['close_30_sma']:.2f}, "
                          f"SMA60=${sample['close_60_sma']:.2f}, "
                          f"RSI={sample['rsi_30']:.1f}")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Error fetching data: {e}", exc_info=True)
            return None
    
    def run_finrl_model(self, market_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Run PPO model with SCALED PREDICTIONS.
        
        Uses direct PPO model prediction and scales actions by max_stock.
        """
        try:
            # Auto-reload fine-tuned model if it was recently updated
            self.reload_model_if_updated()
            
            actual_tickers = market_data['tic'].unique().tolist()
            
            logger.info("🤖 Running FinRL PPO Model...")
            logger.info(f"   Model: {Path(self.current_model_path).name}")
            logger.info(f"   Input: {len(actual_tickers)} tickers")
            logger.info(f"   Max stock: {self.max_stock}")
            
            # Build state vector (301 dims)
            state = self._build_state_vector(market_data)
            
            # Get model prediction
            action, _ = self.model.predict(state.reshape(1, -1), deterministic=True)
            action = action[0]
            
            # SCALE ACTIONS (key part from streaming code)
            scaled_action = (action * self.max_stock).astype(int)
            
            logger.info(f"   Action range: [{scaled_action.min()}, {scaled_action.max()}]")
            
            # Process actions to buy/sell decisions
            buy_decisions = {}
            sell_decisions = {}
            
            # Apply threshold logic (from streaming code)
            for i, act in enumerate(scaled_action):
                ticker = self.ticker_list[i]
                
                if act < -self.min_action:
                    # Sell signal
                    qty = abs(int(act))
                    sell_decisions[ticker] = qty
                elif act > self.min_action:
                    # Buy signal
                    qty = abs(int(act))
                    buy_decisions[ticker] = qty
                # else: HOLD (do nothing)
            
            # Select top tickers (buy + sell)
            all_selected = list(set(list(buy_decisions.keys()) + list(sell_decisions.keys())))
            selected_tickers = all_selected[:settings.finrl_output_tickers]
            
            logger.info(f"✅ FinRL Model Complete")
            logger.info(f"   Buy signals: {len(buy_decisions)} tickers")
            logger.info(f"   Sell signals: {len(sell_decisions)} tickers")
            logger.info(f"   Selected top {len(selected_tickers)}: {selected_tickers}")
            
            # === EXPLAINABILITY: Generate and log explanations ===
            self.explainability.add_background_sample(state)
            sample_status = self.explainability.has_enough_samples()
            
            explanations = {}
            global_importance = None
            
            if sample_status['shap'] or sample_status['lime']:
                logger.info(f"🔍 Generating explainability with JSONL logging...")
                logger.info(f"   Samples collected: {sample_status['samples_collected']}")
                logger.info(f"   SHAP ready: {'✅' if sample_status['shap'] else '❌'}")
                logger.info(f"   LIME ready: {'✅' if sample_status['lime'] else '❌'}")
                
                if len(selected_tickers) > 0:
                    # Prepare action data for logging
                    actions_data = {}
                    for ticker in selected_tickers:
                        if ticker in buy_decisions:
                            actions_data[ticker] = {
                                'action': 'buy',
                                'quantity': buy_decisions[ticker]
                            }
                        elif ticker in sell_decisions:
                            actions_data[ticker] = {
                                'action': 'sell',
                                'quantity': sell_decisions[ticker]
                            }
                        else:
                            actions_data[ticker] = {
                                'action': 'hold',
                                'quantity': 0
                            }
                    
                    # Get ticker indices for top 3 selected tickers
                    ticker_indices = [
                        self.ticker_list.index(ticker) 
                        for ticker in selected_tickers[:3]
                    ]
                    
                    # Generate LOCAL explanations and log to JSONL
                    explanations = {}
                    for ticker_idx in ticker_indices:
                        ticker = self.ticker_list[ticker_idx]
                        action_data = actions_data.get(ticker, {})
                        
                        # Generate explanation
                        explanation = self.explainability.explain_prediction(
                            model=self.model,
                            state=state,
                            ticker_idx=ticker_idx,
                            methods=['shap', 'lime'],
                            top_k=3  # Top 3 for local explanations
                        )
                        
                        # Log to JSONL
                        self.explainability.log_to_jsonl(
                            ticker=ticker,
                            action=action_data.get('action', 'hold'),
                            quantity=action_data.get('quantity', 0),
                            explanation=explanation
                        )
                        
                        explanations[ticker] = explanation
                    
                    logger.info(f"✅ Generated {len(explanations)} local explanations (logged to JSONL)")
                    
                    # Generate GLOBAL explanation and log to JSONL
                    if sample_status['shap']:
                        global_importance = self.explainability.compute_global_importance(
                            model=self.model,
                            state=state,
                            top_k=10  # Top 10 for global importance
                        )
                        
                        if global_importance:
                            # Log global to JSONL
                            self.explainability.log_to_jsonl(
                                ticker="GLOBAL",
                                action="analysis",
                                quantity=0,
                                explanation={'global': global_importance}
                            )
                            
                            logger.info(f"✅ Generated global importance (logged to JSONL)")
                            self.explainability.log_global_importance(
                                global_importance,
                                logger_fn=logger.info
                            )
            else:
                logger.info(f"⏳ Collecting samples for explainability...")
                logger.info(f"   Current: {sample_status['samples_collected']} samples")
                logger.info(f"   Need: {max(50, 100)} samples minimum")
            
            # Activate producers
            if selected_tickers:
                if self.news_producer:
                    self.news_producer.set_active_symbols(selected_tickers)
                if self.social_producer:
                    self.social_producer.set_active_symbols(selected_tickers)
                logger.info(f"✅ Activated news/social for {len(selected_tickers)} tickers")
            
            return {
                'timestamp': datetime.now().isoformat(),
                'selected_tickers': selected_tickers,
                'buy_decisions': buy_decisions,
                'sell_decisions': sell_decisions,
                'total_analyzed': len(market_data),
                'scaled_actions': scaled_action.tolist(),  # For logging
                'explanations': explanations,  # Local: top 3 per ticker
                'global_importance': global_importance,  # Global: top 10 overall
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
    
    def _build_state_vector(self, market_data: pd.DataFrame) -> np.ndarray:
        """
        Build state vector (301 dims) from market data.
        Matches streaming/production_live_trading.py state construction.
        """
        # Extract prices and indicators
        prices = []
        tech_indicators = []
        
        for ticker in self.ticker_list:
            ticker_data = market_data[market_data['tic'] == ticker]
            
            if ticker_data.empty:
                prices.append(100.0)
                for _ in self.tech_indicators:
                    tech_indicators.append(0.0)
            else:
                prices.append(float(ticker_data['close'].iloc[0]))
                for indicator in self.tech_indicators:
                    tech_indicators.append(float(ticker_data[indicator].iloc[0]))
        
        prices = np.array(prices, dtype=np.float32)
        tech = np.array(tech_indicators, dtype=np.float32)
        
        # Simulated portfolio (start with zero positions)
        stocks = np.zeros(30, dtype=np.float32)
        cash = 1_000_000.0
        
        # Scale values
        amount = np.array(cash * (2 ** -12), dtype=np.float32)
        scale = np.array(2 ** -6, dtype=np.float32)
        tech_scaled = tech * 2 ** -7
        
        # Build state: cash + prices + stocks + tech
        state = np.hstack((
            amount,           # 1 value
            prices * scale,   # 30 values
            stocks * scale,   # 30 values
            tech_scaled,      # 240 values (8 * 30)
        )).astype(np.float32)
        
        # Handle NaN/Inf
        state[np.isnan(state)] = 0.0
        state[np.isinf(state)] = 0.0
        
        return state
    
    async def publish_to_redis(self, decisions: Dict[str, Any]) -> bool:
        """Publish decisions to Redis."""
        try:
            message_data = {
                'timestamp': decisions.get('timestamp'),
                'data': json.dumps({
                    'selected_tickers': decisions.get('selected_tickers', []),
                    'buy_decisions': decisions.get('buy_decisions', {}),
                    'sell_decisions': decisions.get('sell_decisions', {}),
                    'total_analyzed': decisions.get('total_analyzed', 0),
                    'scaled_actions': decisions.get('scaled_actions', []),
                })
            }
            
            stream_id = await self.redis_client.xadd('finrl-decisions', message_data)
            logger.info(f"✅ Published to finrl-decisions: {stream_id}")
            
            await self.redis_client.set(
                'finrl:latest',
                json.dumps(decisions),
                ex=7200
            )
            
            if self.finrl_decisions_log:
                log_data(self.finrl_decisions_log, decisions)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Publish failed: {e}", exc_info=True)
            return False
    
    async def run_cycle(self) -> Dict[str, Any]:
        """Run complete FinRL cycle."""
        cycle_start = datetime.now()
        logger.info("=" * 80)
        logger.info(f"🚀 FinRL Cycle Start: {cycle_start.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 80)
        
        # Fetch data from Redis (no waiting - MongoDB has historical data)
        market_data = await self.fetch_data_from_redis()
        if market_data is None or len(market_data) == 0:
            logger.error("❌ No market data available")
            return {'success': False, 'error': 'No data'}
        
        # Run model
        decisions = self.run_finrl_model(market_data)
        if 'error' in decisions:
            return {'success': False, 'error': decisions['error']}
        
        # Publish
        publish_success = await self.publish_to_redis(decisions)
        
        logger.info("=" * 80)
        logger.info(f"✅ Cycle Complete")
        logger.info("=" * 80)
        
        return {
            'success': publish_success,
            'selected_tickers': decisions.get('selected_tickers', [])
        }
    
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
    
    async def start_service(self):
        """Start FinRL service."""
        logger.info("🚀 Starting FinRL Service (Fine-tuned)")
        
        await self.connect_redis()
        
        if not self.load_model():
            logger.error("❌ Cannot start without model")
            return
        
        while True:
            try:
                await self.run_cycle()
                
                wait_time = self.run_interval_hours * 3600
                logger.info(f"⏰ Next cycle in {self.run_interval_hours}h...")
                await asyncio.sleep(wait_time)
            except Exception as e:
                logger.error(f"❌ Service error: {e}", exc_info=True)
                await asyncio.sleep(60)


def create_finrl_service(**kwargs):
    """Create FinRL service instance."""
    return FinRLServiceWithFineTuning(**kwargs)
