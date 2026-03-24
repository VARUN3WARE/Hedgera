"""
FinRL Service - Integration with FinRL for ticker selection.
Reads processed data from Redis, runs FinRL model, outputs 10 selected tickers.
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
import pandas as pd

try:
    from redis import asyncio as aioredis
except ImportError:
    import redis.asyncio as aioredis

from backend.config.settings import settings

logger = logging.getLogger(__name__)


class FinRLService:
    """Service that integrates FinRL with the AEGIS pipeline."""
    
    def __init__(
        self,
        model_path: Optional[str] = None,
        ticker_list: Optional[List[str]] = None,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        run_interval: int = 7200  # 2 hours in seconds
    ):
        """Initialize FinRL service."""
        self.model_path = model_path or settings.finrl_model_path
        self.ticker_list = ticker_list or settings.symbols_list
        self.run_interval = run_interval
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_client = None
        
        # Technical indicators required by FinRL
        self.tech_indicators = [
            'macd', 'boll_ub', 'boll_lb', 'rsi_30', 
            'cci_30', 'dx_30', 'close_30_sma', 'close_60_sma'
        ]
        
        logger.info(f"🤖 FinRL Service initialized - {len(self.ticker_list)} tickers, interval: {run_interval/3600}h")
    
    async def connect_redis(self):
        """Connect to Redis."""
        self.redis_client = await aioredis.from_url(
            f"redis://{self.redis_host}:{self.redis_port}",
            decode_responses=True
        )
        logger.info("✅ FinRL connected to Redis")
    
    async def fetch_data_from_redis(self) -> Optional[pd.DataFrame]:
        """Fetch processed market data from Redis master-state stream."""
        try:
            messages = await self.redis_client.xrevrange(
                'processed:master-state',
                count=len(self.ticker_list) * 2
            )
            
            if not messages:
                logger.warning("⚠️  No data in processed:master-state stream")
                return None
            
            ticker_data = {}
            for message_id, fields in messages:
                try:
                    data = json.loads(fields.get('data', '{}'))
                    ticker = data['metadata']['ticker']
                    
                    if ticker not in self.ticker_list:
                        continue
                    
                    price_data = data.get('price_data', {})
                    momentum = data.get('momentum_indicators', {})
                    volatility = data.get('volatility_indicators', {})
                    trend = data.get('trend_indicators', {})
                    moving_averages = data.get('moving_averages', {})
                    
                    ticker_record = {
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
                    
                    ticker_data[ticker] = ticker_record
                except Exception as e:
                    logger.warning(f"⚠️  Error parsing message: {e}")
                    continue
            
            if not ticker_data:
                return None
            
            OHLCV_COLUMNS = ['open', 'high', 'low', 'close', 'volume']
            REQUIRED_COLUMNS = ['tic'] + OHLCV_COLUMNS + self.tech_indicators
            
            data_rows = []
            for ticker, record in ticker_data.items():
                row = {'tic': ticker}
                row.update(record)
                data_rows.append(row)
            
            df = pd.DataFrame(data_rows, columns=REQUIRED_COLUMNS)
            logger.info(f"✅ Fetched {len(df)} tickers with {len(REQUIRED_COLUMNS)} columns")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Error fetching from Redis: {e}", exc_info=True)
            return None
    
    def run_finrl_model(self, market_data: pd.DataFrame) -> Dict[str, Any]:
        """Run FinRL model - simplified selection logic."""
        try:
            logger.info(f"🤖 Running FinRL on {len(market_data)} tickers")
            
            scores = []
            for idx, row in market_data.iterrows():
                rsi_score = 1 if 30 < row['rsi_30'] < 70 else 0
                macd_score = 1 if row['macd'] > 0 else 0
                trend_score = 1 if row['close'] > row['close_30_sma'] else 0
                volume_score = 1 if row['volume'] > 1000 else 0
                
                total_score = rsi_score + macd_score + trend_score + volume_score
                
                scores.append({
                    'ticker': row['tic'],
                    'score': total_score,
                    'close': row['close'],
                    'rsi_30': row['rsi_30'],
                    'macd': row['macd']
                })
            
            scores.sort(key=lambda x: x['score'], reverse=True)
            selected_tickers = [s['ticker'] for s in scores[:settings.finrl_output_tickers]]
            
            actions = {}
            for item in scores[:settings.finrl_output_tickers]:
                action = 'BUY' if item['macd'] > 0 else 'HOLD'
                actions[item['ticker']] = {
                    'action': action,
                    'shares': 10,
                    'score': item['score']
                }
            
            logger.info(f"✅ Selected {len(selected_tickers)} tickers: {selected_tickers[:5]}...")
            
            return {
                'timestamp': datetime.now().isoformat(),
                'selected_tickers': selected_tickers,
                'actions': actions,
                'total_analyzed': len(market_data)
            }
            
        except Exception as e:
            logger.error(f"❌ FinRL model error: {e}", exc_info=True)
            return {
                'timestamp': datetime.now().isoformat(),
                'selected_tickers': [],
                'actions': {},
                'error': str(e)
            }
    
    async def publish_to_redis(self, decisions: Dict[str, Any]) -> bool:
        """Publish FinRL decisions to Redis stream."""
        try:
            message_data = {
                'timestamp': decisions.get('timestamp', datetime.now().isoformat()),
                'selected_tickers': json.dumps(decisions.get('selected_tickers', [])),
                'actions': json.dumps(decisions.get('actions', {})),
                'total_analyzed': decisions.get('total_analyzed', 0)
            }
            
            stream_id = await self.redis_client.xadd('finrl-decisions', message_data)
            logger.info(f"✅ Published to finrl-decisions: {stream_id}")
            
            await self.redis_client.set(
                'finrl:latest',
                json.dumps(decisions),
                ex=7200
            )
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Publish failed: {e}", exc_info=True)
            return False
    
    async def run_cycle(self) -> Dict[str, Any]:
        """Run complete FinRL cycle."""
        cycle_start = datetime.now()
        logger.info(f"🚀 Starting FinRL cycle at {cycle_start}")
        
        market_data = await self.fetch_data_from_redis()
        if market_data is None or len(market_data) == 0:
            logger.error("❌ No market data, skipping")
            return {'success': False, 'error': 'No data', 'timestamp': cycle_start.isoformat()}
        
        decisions = self.run_finrl_model(market_data)
        if 'error' in decisions:
            return {'success': False, 'error': decisions['error'], 'timestamp': cycle_start.isoformat()}
        
        publish_success = await self.publish_to_redis(decisions)
        
        cycle_duration = (datetime.now() - cycle_start).total_seconds()
        
        logger.info(f"✅ FinRL cycle completed in {cycle_duration:.2f}s")
        
        return {
            'success': publish_success,
            'timestamp': cycle_start.isoformat(),
            'duration_seconds': cycle_duration,
            'selected_tickers': decisions.get('selected_tickers', [])
        }
    
    async def start_service(self):
        """Start FinRL service with periodic execution."""
        logger.info(f"🚀 Starting FinRL Service")
        
        await self.connect_redis()
        
        while True:
            try:
                await self.run_cycle()
                logger.info(f"⏰ Waiting {self.run_interval}s until next cycle...")
                await asyncio.sleep(self.run_interval)
            except Exception as e:
                logger.error(f"❌ Service error: {e}", exc_info=True)
                await asyncio.sleep(60)


def create_finrl_service(**kwargs) -> FinRLService:
    """Create FinRL service instance."""
    return FinRLService(**kwargs)
