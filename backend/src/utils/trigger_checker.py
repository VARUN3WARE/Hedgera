"""
Trigger Checker - Detects major price breakouts using Redis Stream data
"""

import logging
import json
import numpy as np
import pandas as pd
from typing import Tuple, Dict, List

try:
    from redis import asyncio as aioredis
except ImportError:
    import redis.asyncio as aioredis

from backend.config.settings import settings

logger = logging.getLogger(__name__)


class TriggerChecker:
    """Check if major breakout detected - triggers early FinRL execution."""
    
    def __init__(self, redis_host: str = "localhost", redis_port: int = 6379):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_client = None
    
    async def connect(self):
        """Connect to Redis."""
        self.redis_client = await aioredis.from_url(
            f"redis://{self.redis_host}:{self.redis_port}",
            decode_responses=True
        )
        logger.info("✅ Trigger Checker connected to Redis")
    
    async def check_trigger(self) -> bool:
        """
        Main trigger check function.
        Gets last 60 candles from Redis Stream and checks for major breakout.
        
        Returns:
            True if major breakout detected in latest candle, False otherwise
        """
        if not self.redis_client:
            await self.connect()
        
        try:
            # Get last 60 candles from processed:price stream (mixed tickers)
            candles_data = await self._get_last_60_candles()
            
            if not candles_data:
                logger.warning("⚠️  No candle data available yet")
                return False
            
            # Check for major breakout
            triggered, info = self._detect_major_breakout(candles_data)
            
            if triggered:
                logger.info("🚨 MAJOR BREAKOUT DETECTED!")
                logger.info(f"   Close: {info.get('latest_close', 'N/A')}")
                logger.info(f"   Upper BB: {info.get('upper_bb', 'N/A')}")
                logger.info(f"   Lower BB: {info.get('lower_bb', 'N/A')}")
                logger.info(f"   Candle Range: {info.get('candle_range', 'N/A')}")
                logger.info(f"   ATR: {info.get('atr', 'N/A')}")
                logger.info(f"   Outside BB: {info.get('outside_bb', False)}")
                logger.info(f"   ATR Spike: {info.get('atr_spike', False)}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Error in check_trigger: {e}", exc_info=True)
            return False
    
    async def _get_last_60_candles(self) -> List[Dict]:
        """
        Get last 60 candles from Redis Stream "processed:price".
        
        Returns:
            List of candle dictionaries: [{'open': ..., 'high': ..., 'low': ..., 'close': ...}, ...]
        """
        try:
            # Read last 300 entries (to account for 30 tickers)
            entries = await self.redis_client.xrevrange("processed:price", count=300)
            
            if not entries:
                logger.warning("⚠️  No entries in processed:price stream")
                return []
            
            # Parse and collect candles
            candles = []
            for entry_id, entry_data in entries:
                try:
                    data = json.loads(entry_data.get("data", "{}"))
                    
                    # Extract price data
                    price_data = data.get("price_data", {})
                    if price_data:
                        candle = {
                            'open': float(price_data.get("open", 0)),
                            'high': float(price_data.get("high", 0)),
                            'low': float(price_data.get("low", 0)),
                            'close': float(price_data.get("close", 0)),
                            'volume': float(price_data.get("volume", 0))
                        }
                        candles.append(candle)
                        
                        # Stop when we have 60
                        if len(candles) >= 60:
                            break
                except Exception as e:
                    logger.debug(f"Error parsing entry: {e}")
                    continue
            
            # Reverse to get chronological order (oldest to newest)
            candles = candles[::-1]
            
            logger.debug(f"📊 Retrieved {len(candles)} candles from Redis Stream")
            return candles
            
        except Exception as e:
            logger.error(f"Error getting last 60 candles: {e}")
            return []
    
    def _detect_major_breakout(self, candles_list: List[Dict]) -> Tuple[bool, Dict]:
        """
        Detect major breakout using Bollinger Bands + ATR.
        
        Parameters:
            candles_list: List of candle dicts with 'open', 'high', 'low', 'close'
        
        Returns:
            (triggered: bool, info: dict)
        """
        try:
            # Convert to pandas DataFrame
            df = pd.DataFrame(candles_list)
            
            if len(df) < 20:
                return False, {"reason": "insufficient_candles", "count": len(df)}
            
            # Extract OHLCV
            closes = df["close"].astype(float).values
            highs = df["high"].astype(float).values
            lows = df["low"].astype(float).values
            opens = df["open"].astype(float).values
            
            # ===== 1. Bollinger Bands (20 SMA, 2 stddev) =====
            bb_period = 20
            sma = pd.Series(closes).rolling(bb_period).mean().iloc[-1]
            std = pd.Series(closes).rolling(bb_period).std().iloc[-1]
            
            upper_bb = sma + 2 * std
            lower_bb = sma - 2 * std
            
            # ===== 2. ATR (20 period) =====
            atr_period = 20
            
            # True Range calculation
            trs = np.maximum(
                highs[1:] - lows[1:],
                np.maximum(
                    np.abs(highs[1:] - closes[:-1]),
                    np.abs(lows[1:] - closes[:-1])
                )
            )
            
            # ATR as EMA of TR
            if len(trs) < atr_period:
                atr = np.mean(trs[-atr_period:]) if len(trs) > 0 else 0.0
            else:
                atr = pd.Series(trs).ewm(alpha=1/atr_period, adjust=False).mean().iloc[-1]
            
            # ===== 3. Latest candle analysis =====
            latest_close = closes[-1]
            latest_high = highs[-1]
            latest_low = lows[-1]
            latest_open = opens[-1]
            candle_range = latest_high - latest_low
            
            # ===== 4. Check breakout conditions =====
            outside_bb = latest_close > upper_bb or latest_close < lower_bb
            atr_spike = (atr > 0) and (candle_range > atr * 1.8)
            
            triggered = outside_bb or atr_spike
            
            info = {
                "latest_close": float(latest_close),
                "upper_bb": float(upper_bb),
                "lower_bb": float(lower_bb),
                "sma_20": float(sma),
                "candle_range": float(candle_range),
                "atr": float(atr),
                "outside_bb": bool(outside_bb),
                "atr_spike": bool(atr_spike),
                "triggered": bool(triggered)
            }
            
            return triggered, info
            
        except Exception as e:
            logger.error(f"Error in detect_major_breakout: {e}", exc_info=True)
            return False, {"error": str(e)}
    
    async def close(self):
        """Close Redis connection."""
        if self.redis_client:
            await self.redis_client.close()
            logger.info("✅ Trigger Checker connection closed")