"""Streaming engine for real-time data processing using Redis Streams."""
import asyncio
import json
import time
import logging
import statistics
from collections import defaultdict
from typing import Dict, Any, List
from datetime import datetime

try:
    from redis import asyncio as aioredis
except ImportError:
    import redis.asyncio as aioredis

from backend.config.settings import settings
from backend.config.logging_setup import log_data

logger = logging.getLogger(__name__)


class StreamingEngine:
    """Real-time streaming engine using Redis Streams."""
    
    def __init__(self):
        self.redis = None
        self.consumer_group = settings.pathway_consumer_group
        self.consumer_name = "engine-worker-1"
        self.streams = {
            "raw:price-updates": ">",
            "raw:news-articles": ">",
            "raw:social": ">"
        }
        self.aggregated_data = defaultdict(dict)
        self.last_published_data = {}
        self.data_updated = False
        
        # Valid symbols from settings
        self.valid_symbols = set(settings.symbols_list)
        logger.info(f"🎯 Valid symbols for aggregation ({len(self.valid_symbols)}): {list(self.valid_symbols)[:5]}...")
        
        # Historical data for advanced indicators
        self.price_history = defaultdict(list)
        self.high_history = defaultdict(list)
        self.low_history = defaultdict(list)
        self.volume_history = defaultdict(list)
        self.news_sentiment_history = defaultdict(list)
        self.social_sentiment_history = defaultdict(list)
        
        # Window sizes for indicators
        self.max_history_size = 60
        self.short_window = 5
        self.medium_window = 10
        self.long_window = 20
        self.rsi_window = 14
        self.ema_short = 12
        self.ema_long = 26
        
        # Data logging paths
        self.raw_data_log = None
        self.processed_data_log = None
    
    def set_logging(self, raw_log_path: str, processed_log_path: str):
        """Set logging paths for data."""
        self.raw_data_log = raw_log_path
        self.processed_data_log = processed_log_path
    
    def calculate_sma(self, prices: List[float], window: int) -> float:
        """
        Calculate Simple Moving Average.
        If we don't have enough data points (< window), use available data.
        Example: SMA5 with 3 points = sum(3 points) / 3
        """
        if not prices:
            return 0.0
        if len(prices) < window:
            # Use available data points
            return sum(prices) / len(prices)
        # Use last 'window' data points
        return sum(prices[-window:]) / window
    
    def calculate_ema(self, prices: List[float], window: int) -> float:
        """
        Calculate Exponential Moving Average.
        If we don't have enough data, use SMA of available points as seed.
        """
        if not prices:
            return 0.0
        
        if len(prices) < window:
            # Use SMA of available data as EMA seed
            return self.calculate_sma(prices, len(prices))
        
        multiplier = 2 / (window + 1)
        # Calculate initial SMA for seed
        ema = self.calculate_sma(prices[:window], window)
        
        # Apply EMA formula for remaining prices
        for price in prices[window:]:
            ema = (price * multiplier) + (ema * (1 - multiplier))
        
        return ema
    
    def calculate_rsi(self, prices: List[float], period: int = 14) -> float:
        """
        Calculate Relative Strength Index (RSI).
        If insufficient data, return neutral RSI (50.0).
        Needs at least 2 prices to calculate deltas.
        """
        if len(prices) < 2:
            return 50.0  # Neutral RSI when no price changes available
        
        if len(prices) < period + 1:
            # Use available deltas if less than period
            deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
        else:
            # Use full period
            deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
            deltas = deltas[-period:]  # Last 'period' deltas
        
        gains = [d if d > 0 else 0 for d in deltas]
        losses = [-d if d < 0 else 0 for d in deltas]
        
        avg_gain = sum(gains) / len(gains)
        avg_loss = sum(losses) / len(losses)
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def calculate_macd(self, prices: List[float]) -> Dict[str, float]:
        """Calculate MACD (Moving Average Convergence Divergence)."""
        if len(prices) < self.ema_long:
            return {"macd_line": 0.0, "signal_line": 0.0, "histogram": 0.0}
        
        ema_12 = self.calculate_ema(prices, self.ema_short)
        ema_26 = self.calculate_ema(prices, self.ema_long)
        macd_line = ema_12 - ema_26
        signal_line = macd_line  # Simplified (should be 9-period EMA of MACD)
        histogram = macd_line - signal_line
        
        return {
            "macd_line": macd_line,
            "signal_line": signal_line,
            "histogram": histogram
        }
    
    def calculate_bollinger_bands(self, prices: List[float], window: int = 20, num_std: float = 2.0) -> Dict[str, float]:
        """
        Calculate Bollinger Bands.
        If insufficient data, use available prices.
        """
        if not prices:
            return {"upper": 0.0, "middle": 0.0, "lower": 0.0}
        
        if len(prices) < window:
            # Use all available prices
            middle = sum(prices) / len(prices)
            if len(prices) < 2:
                # Need at least 2 points for std dev
                return {"upper": middle, "middle": middle, "lower": middle}
            std_dev = statistics.stdev(prices)
        else:
            # Use last 'window' prices
            middle = self.calculate_sma(prices, window)
            std_dev = statistics.stdev(prices[-window:])
        
        upper = middle + (std_dev * num_std)
        lower = middle - (std_dev * num_std)
        
        return {"upper": upper, "middle": middle, "lower": lower}
    
    def calculate_cci(self, highs: List[float], lows: List[float], closes: List[float], period: int = 30) -> float:
        """Calculate Commodity Channel Index (CCI) - required by FinRL."""
        if len(closes) < period:
            return 0.0
        
        typical_prices = [(h + l + c) / 3 for h, l, c in zip(highs[-period:], lows[-period:], closes[-period:])]
        sma_tp = sum(typical_prices) / period
        mean_deviation = sum(abs(tp - sma_tp) for tp in typical_prices) / period
        
        if mean_deviation == 0:
            return 0.0
        
        cci = (typical_prices[-1] - sma_tp) / (0.015 * mean_deviation)
        return cci
    
    def calculate_dx(self, highs: List[float], lows: List[float], closes: List[float], period: int = 30) -> float:
        """Calculate Directional Index (DX) - required by FinRL."""
        # Simplified DX calculation
        if len(closes) < period + 1:
            return 0.0
        
        # Calculate +DM and -DM
        plus_dm = sum(max(highs[i] - highs[i-1], 0) for i in range(1, min(len(highs), period + 1)))
        minus_dm = sum(max(lows[i-1] - lows[i], 0) for i in range(1, min(len(lows), period + 1)))
        
        # Calculate True Range
        true_range = sum(max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1])) 
                        for i in range(1, min(len(closes), period + 1)))
        
        if true_range == 0:
            return 0.0
        
        plus_di = (plus_dm / true_range) * 100
        minus_di = (minus_dm / true_range) * 100
        
        if plus_di + minus_di == 0:
            return 0.0
        
        dx = abs(plus_di - minus_di) / (plus_di + minus_di) * 100
        return dx
    
    async def connect(self):
        """Connect to Redis."""
        self.redis = await aioredis.from_url(
            f"redis://{settings.redis_host}:{settings.redis_port}",
            decode_responses=True
        )
        logger.info("✅ Connected to Redis")
        
        # Create consumer groups if they don't exist
        for stream in self.streams.keys():
            try:
                await self.redis.xgroup_create(
                    stream, self.consumer_group, id="0", mkstream=True
                )
                logger.info(f"✅ Created consumer group for {stream}")
            except Exception as e:
                if "BUSYGROUP" not in str(e):
                    logger.warning(f"Consumer group creation: {e}")
    
    async def process_price_data(self, data: Dict[str, Any]):
        """Process price tick data."""
        symbol = data.get("symbol")
        
        # Log raw data
        if self.raw_data_log:
            log_data(self.raw_data_log, {'type': 'price', 'data': data, 'timestamp': datetime.now().isoformat()})
        
        # Validate symbol
        if not symbol or symbol not in self.valid_symbols:
            return
        
        if symbol not in self.aggregated_data:
            self.aggregated_data[symbol] = {
                "symbol": symbol,
                "last_price": 0,
                "prices": [],
                "highs": [],
                "lows": [],
                "volumes": [],
                "news_sentiment": 0,
                "social_sentiment": 0
            }
        
        # Extract OHLCV data
        price = data.get("price", 0)
        open_val = data.get("open", price)
        high = data.get("high", price)
        low = data.get("low", price)
        close_val = data.get("close", price)
        volume = data.get("volume", 0)
        
        self.aggregated_data[symbol]["last_price"] = close_val
        self.aggregated_data[symbol]["prices"].append(close_val)
        self.aggregated_data[symbol]["highs"].append(high)
        self.aggregated_data[symbol]["lows"].append(low)
        self.aggregated_data[symbol]["volumes"].append(volume)
        
        # Add to history for indicators
        self.price_history[symbol].append(close_val)
        self.high_history[symbol].append(high)
        self.low_history[symbol].append(low)
        self.volume_history[symbol].append(volume)
        
        # Keep only last N data points
        if len(self.price_history[symbol]) > self.max_history_size:
            self.price_history[symbol] = self.price_history[symbol][-self.max_history_size:]
            self.high_history[symbol] = self.high_history[symbol][-self.max_history_size:]
            self.low_history[symbol] = self.low_history[symbol][-self.max_history_size:]
            self.volume_history[symbol] = self.volume_history[symbol][-self.max_history_size:]
        
        # Keep only last 60 in aggregated data
        for key in ["prices", "highs", "lows", "volumes"]:
            if len(self.aggregated_data[symbol][key]) > 60:
                self.aggregated_data[symbol][key] = self.aggregated_data[symbol][key][-60:]
        
        self.data_updated = True
        
        # Publish immediately for every price update to ensure MongoDB gets every 1-minute bar
        logger.info(f"📊 Price update received for {symbol}, publishing to processed:price stream")
        await self.publish_single_symbol(symbol)
    
    async def process_news_data(self, data: Dict[str, Any]):
        """Process news article data."""
        symbol = data.get("symbol")
        
        # Log raw data
        if self.raw_data_log:
            log_data(self.raw_data_log, {'type': 'news', 'data': data, 'timestamp': datetime.now().isoformat()})
        
        # Validate symbol
        if not symbol or symbol not in self.valid_symbols:
            return
        
        if symbol not in self.aggregated_data:
            self.aggregated_data[symbol] = {
                "symbol": symbol,
                "last_price": 0,
                "prices": [],
                "highs": [],
                "lows": [],
                "volumes": [],
                "news_sentiment": 0,
                "social_sentiment": 0
            }
        
        sentiment = data.get("sentiment", 0)
        self.aggregated_data[symbol]["news_sentiment"] = sentiment
        
        # Add to history
        self.news_sentiment_history[symbol].append(sentiment)
        if len(self.news_sentiment_history[symbol]) > self.max_history_size:
            self.news_sentiment_history[symbol] = self.news_sentiment_history[symbol][-self.max_history_size:]
        
        self.data_updated = True
    
    async def process_social_data(self, data: Dict[str, Any]):
        """Process social sentiment data."""
        symbol = data.get("symbol")
        
        if not symbol or symbol not in self.valid_symbols:
            return
        
        if symbol not in self.aggregated_data:
            self.aggregated_data[symbol] = {
                "symbol": symbol,
                "last_price": 0,
                "prices": [],
                "highs": [],
                "lows": [],
                "volumes": [],
                "news_sentiment": 0,
                "social_sentiment": 0
            }
        
        sentiment = data.get("sentiment", 0)
        self.aggregated_data[symbol]["social_sentiment"] = sentiment
        
        # Add to history
        self.social_sentiment_history[symbol].append(sentiment)
        if len(self.social_sentiment_history[symbol]) > self.max_history_size:
            self.social_sentiment_history[symbol] = self.social_sentiment_history[symbol][-self.max_history_size:]
        
        self.data_updated = True
    
    async def publish_single_symbol(self, symbol: str):
        """Publish single symbol immediately when price data arrives (for MongoDB sync)."""
        try:
            if symbol not in self.aggregated_data:
                return
            
            state = self.aggregated_data[symbol]
            
            # Get historical data
            prices = self.price_history.get(symbol, [])
            highs = self.high_history.get(symbol, [])
            lows = self.low_history.get(symbol, [])
            volumes = self.volume_history.get(symbol, [])
            
            # Need at least 1 data point to publish
            if len(prices) < 1:
                return
            
            # Basic OHLCV data
            recent_prices = state.get("prices", [])
            recent_highs = state.get("highs", [])
            recent_lows = state.get("lows", [])
            recent_volumes = state.get("volumes", [])
            
            current_price = float(state["last_price"]) if state["last_price"] else 0.0
            open_price = float(recent_prices[0]) if recent_prices else current_price
            current_high = max(recent_highs) if recent_highs else current_price
            current_low = min(recent_lows) if recent_lows else current_price
            current_volume = sum(recent_volumes) if recent_volumes else 0.0
            
            # Calculate indicators required by FinRL
            sma_30 = self.calculate_sma(prices, 30)
            sma_60 = self.calculate_sma(prices, 60)
            ema_12 = self.calculate_ema(prices, 12)
            ema_26 = self.calculate_ema(prices, 26)
            
            # Momentum Indicators
            rsi_30 = self.calculate_rsi(prices, 30)
            macd = self.calculate_macd(prices)
            cci_30 = self.calculate_cci(highs, lows, prices, 30)
            
            # Trend Indicators
            dx_30 = self.calculate_dx(highs, lows, prices, 30)
            
            # Volatility Indicators
            bollinger = self.calculate_bollinger_bands(prices, 20, 2.0)
            
            # Build master state with FinRL-required indicators
            master_state = {
                "metadata": {
                    "company_name": symbol,
                    "ticker": symbol,
                    "timestamp": datetime.now().isoformat(),
                    "date": datetime.now().strftime('%Y-%m-%d'),
                    "timeframe": "1min"
                },
                "price_data": {
                    "open": round(open_price, 2),
                    "high": round(current_high, 2),
                    "low": round(current_low, 2),
                    "close": round(current_price, 2),
                    "volume": round(current_volume, 2)
                },
                "moving_averages": {
                    "close_30_sma": round(sma_30, 2),
                    "close_60_sma": round(sma_60, 2),
                    "ema_12": round(ema_12, 2),
                    "ema_26": round(ema_26, 2)
                },
                "momentum_indicators": {
                    "rsi_30": round(rsi_30, 2),
                    "cci_30": round(cci_30, 2),
                    "macd": {
                        "macd_line": round(macd["macd_line"], 2),
                        "signal_line": round(macd["signal_line"], 2),
                        "histogram": round(macd["histogram"], 2)
                    }
                },
                "trend_indicators": {
                    "dx_30": round(dx_30, 2)
                },
                "volatility_indicators": {
                    "boll_ub": round(bollinger["upper"], 2),
                    "boll_lb": round(bollinger["lower"], 2),
                    "bollinger_middle": round(bollinger["middle"], 2)
                },
                "sentiment_indicators": {
                    "news_sentiment": round(float(state.get("news_sentiment", 0)), 2),
                    "social_sentiment": round(float(state.get("social_sentiment", 0)), 2)
                },
                "timestamp": datetime.now().isoformat()
            }
            
            # Publish to processed:price stream for MongoDB sync
            await self.redis.xadd(
                "processed:price",
                {"data": json.dumps(master_state)}
            )
            
        except Exception as e:
            logger.error(f"❌ Error publishing single symbol {symbol}: {e}", exc_info=True)
    
    async def publish_aggregated_state(self):
        """Publish aggregated market state with comprehensive technical indicators for FinRL."""
        try:
            logger.info(f"🔄 Starting publish for {len(self.aggregated_data)} symbols...")
            published_count = 0
            skipped_count = 0
            
            for symbol, state in self.aggregated_data.items():
                try:
                    # Get historical data
                    prices = self.price_history[symbol]
                    highs = self.high_history[symbol]
                    lows = self.low_history[symbol]
                    volumes = self.volume_history[symbol]
                    
                    # Debug: Log history size for first few symbols
                    if published_count < 3:
                        logger.info(f"   📊 {symbol}: {len(prices)} price points in history (range: {min(prices) if prices else 0:.2f}-{max(prices) if prices else 0:.2f})")
                    
                    # Need at least 1 data point to publish
                    # Indicators will adapt to available data
                    if len(prices) < 1:
                        skipped_count += 1
                        logger.debug(f"   Skipping {symbol}: no price data")
                        continue
                    
                    # Basic OHLCV data
                    recent_prices = state.get("prices", [])
                    recent_highs = state.get("highs", [])
                    recent_lows = state.get("lows", [])
                    recent_volumes = state.get("volumes", [])
                    
                    current_price = float(state["last_price"]) if state["last_price"] else 0.0
                    open_price = float(recent_prices[0]) if recent_prices else current_price
                    current_high = max(recent_highs) if recent_highs else current_price
                    current_low = min(recent_lows) if recent_lows else current_price
                    current_volume = sum(recent_volumes) if recent_volumes else 0.0
                    
                    # Calculate indicators required by FinRL
                    # Moving Averages (functions handle insufficient data automatically)
                    sma_30 = self.calculate_sma(prices, 30)
                    sma_60 = self.calculate_sma(prices, 60)  # Let function handle insufficient data
                    ema_12 = self.calculate_ema(prices, 12)
                    ema_26 = self.calculate_ema(prices, 26)
                    
                    # Momentum Indicators
                    rsi_30 = self.calculate_rsi(prices, 30)
                    macd = self.calculate_macd(prices)
                    cci_30 = self.calculate_cci(highs, lows, prices, 30)
                    
                    # Trend Indicators
                    dx_30 = self.calculate_dx(highs, lows, prices, 30)
                    
                    # Volatility Indicators
                    bollinger = self.calculate_bollinger_bands(prices, 20, 2.0)
                    
                    # Build master state with FinRL-required indicators
                    master_state = {
                        "metadata": {
                            "company_name": symbol,
                            "ticker": symbol,
                            "date": datetime.now().strftime('%Y-%m-%d'),
                            "timeframe": f"{settings.producer_fetch_interval}s"
                        },
                        "price_data": {
                            "open": round(open_price, 2),
                            "high": round(current_high, 2),
                            "low": round(current_low, 2),
                            "close": round(current_price, 2),
                            "volume": round(current_volume, 2)
                        },
                        "moving_averages": {
                            "close_30_sma": round(sma_30, 2),
                            "close_60_sma": round(sma_60, 2),
                            "ema_12": round(ema_12, 2),
                            "ema_26": round(ema_26, 2)
                        },
                        "momentum_indicators": {
                            "rsi_30": round(rsi_30, 2),
                            "cci_30": round(cci_30, 2),
                            "macd": {
                                "macd_line": round(macd["macd_line"], 2),
                                "signal_line": round(macd["signal_line"], 2),
                                "histogram": round(macd["histogram"], 2)
                            }
                        },
                        "trend_indicators": {
                            "dx_30": round(dx_30, 2)
                        },
                        "volatility_indicators": {
                            "boll_ub": round(bollinger["upper"], 2),
                            "boll_lb": round(bollinger["lower"], 2),
                            "bollinger_middle": round(bollinger["middle"], 2)
                        },
                        "sentiment_indicators": {
                            "news_sentiment": round(float(state.get("news_sentiment", 0)), 2),
                            "social_sentiment": round(float(state.get("social_sentiment", 0)), 2)
                        },
                        "timestamp": datetime.now().isoformat()
                    }
                    
                    # Log processed data
                    if self.processed_data_log:
                        log_data(self.processed_data_log, {
                            'ticker': symbol,
                            'data': master_state,
                            'timestamp': datetime.now().isoformat()
                        })
                    
                    # Publish to processed streams
                    # processed:price - for FinRL consumption
                    await self.redis.xadd(
                        "processed:price",
                        {"data": json.dumps(master_state)}
                    )
                    # processed:master-state - for general consumption
                    await self.redis.xadd(
                        "processed:master-state",
                        {"data": json.dumps(master_state)}
                    )
                    published_count += 1
                except Exception as e:
                    logger.error(f"❌ Error publishing {symbol}: {e}", exc_info=True)
            
            if published_count > 0:
                logger.info(f"📊 Published aggregated state for {published_count} symbols with FinRL indicators")
            if skipped_count > 0:
                logger.info(f"⏭️  Skipped {skipped_count} symbols (insufficient data)")
            
            self.data_updated = False
        except Exception as e:
            logger.error(f"❌ Fatal error in publish_aggregated_state: {e}", exc_info=True)
    
    async def consume_streams(self):
        """Consume from Redis Streams."""
        logger.info("🚀 Starting stream consumption...")
        logger.info(f"📥 Consuming from streams: {list(self.streams.keys())}")
        
        last_publish_time = asyncio.get_event_loop().time()
        message_count = 0
        
        while True:
            try:
                # Read from all streams
                try:
                    messages = await self.redis.xreadgroup(
                        self.consumer_group,
                        self.consumer_name,
                        streams=self.streams,
                        count=10,
                        block=1000
                    )
                except Exception as e:
                    # Handle NOGROUP error during startup when streams don't exist yet
                    if "NOGROUP" in str(e):
                        # Recreate consumer groups if needed
                        for stream in self.streams.keys():
                            try:
                                await self.redis.xgroup_create(
                                    stream, self.consumer_group, id="0", mkstream=True
                                )
                            except Exception:
                                pass  # Group already exists
                        await asyncio.sleep(1)
                        continue
                    else:
                        raise
                
                if messages:
                    logger.info(f"📨 Received {len(messages)} stream(s) with messages")
                    for stream_name, stream_messages in messages:
                        logger.info(f"   {stream_name}: {len(stream_messages)} messages")
                        for message_id, message_data in stream_messages:
                            try:
                                data = json.loads(message_data.get("data", "{}"))
                                message_count += 1
                                
                                if stream_name == "raw:price-updates":
                                    await self.process_price_data(data)
                                    logger.info(f"💰 Processed price for {data.get('symbol', 'unknown')}")
                                elif stream_name == "raw:news-articles":
                                    await self.process_news_data(data)
                                    logger.info(f"📰 Processed news for {data.get('symbol', 'unknown')}")
                                elif stream_name == "raw:social":
                                    await self.process_social_data(data)
                                    logger.info(f"📱 Processed social for {data.get('symbol', 'unknown')}")
                                
                                # Acknowledge the message
                                await self.redis.xack(stream_name, self.consumer_group, message_id)
                                
                            except Exception as e:
                                logger.error(f"❌ Error processing message: {e}", exc_info=True)
                
                # Publish aggregated state every N seconds
                current_time = asyncio.get_event_loop().time()
                if current_time - last_publish_time >= settings.pathway_publish_interval:
                    logger.info(f"⏰ Publish interval reached. Aggregated data: {len(self.aggregated_data)} symbols, data_updated: {self.data_updated}")
                    if self.aggregated_data and self.data_updated:
                        await self.publish_aggregated_state()
                    last_publish_time = current_time
                
            except Exception as e:
                logger.error(f"❌ Error in consume loop: {e}", exc_info=True)
                await asyncio.sleep(1)
    
    async def run(self):
        """Run the streaming engine."""
        await self.connect()
        await self.consume_streams()
    
    async def close(self):
        """Close connections."""
        if self.redis:
            await self.redis.close()


def run_pathway_engine():
    """Run the streaming engine (main entry point)."""
    engine = StreamingEngine()
    try:
        asyncio.run(engine.run())
    except KeyboardInterrupt:
        logger.info("🛑 Shutting down streaming engine...")
    finally:
        asyncio.run(engine.close())
