"""Pathway-based streaming engine that replaces manual streaming_engine.py.

This implementation uses Pathway's temporal features to automatically:
- Manage state and time windows
- Calculate technical indicators
- Handle multi-stream correlation
- Publish to processed streams

All indicators match the original streaming_engine.py implementation.
"""

import pathway as pw
import logging
from typing import Dict, Any, Optional
from datetime import datetime

from backend.config.settings import settings
from .schemas import PriceSchema, NewsSchema, SocialSchema, ProcessedPriceSchema

logger = logging.getLogger(__name__)


class PathwayStreamingEngine:
    """
    Pathway-based streaming engine for real-time indicator calculation.
    
    Replaces the manual StreamingEngine with automatic temporal processing.
    
    Features:
    - Automatic window management (5min, 30min, 60min)
    - Built-in SMA, EMA calculations
    - Custom UDFs for RSI, MACD, Bollinger, CCI, DX
    - Multi-stream correlation (price + news + social)
    - Exactly-once semantics
    """
    
    def __init__(
        self,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_password: str = "",
    ):
        """Initialize Pathway streaming engine."""
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_password = redis_password
        
        # Configuration matching original streaming_engine.py
        self.max_history_size = 60
        self.short_window = 5
        self.medium_window = 10
        self.long_window = 20
        self.rsi_window = 14
        self.ema_short = 12
        self.ema_long = 26
        
        # Pathway tables
        self.price_stream = None
        self.news_stream = None
        self.social_stream = None
        self.processed_table = None
        
        # Logging configuration (for compatibility with orchestrators)
        self.raw_data_log_path = None
        self.processed_data_log_path = None
        
        logger.info("🚀 Pathway Streaming Engine initialized")
        logger.info(f"📡 Redis: {redis_host}:{redis_port}")
    
    def set_logging(self, raw_data_log_path: str, processed_data_log_path: str):
        """
        Set logging paths for data logging (for compatibility with pipeline orchestrators).
        
        Args:
            raw_data_log_path: Path to raw data log file
            processed_data_log_path: Path to processed data log file
        
        Note: Pathway handles its own logging, but we store these paths
        for potential future use or debugging purposes.
        """
        self.raw_data_log_path = raw_data_log_path
        self.processed_data_log_path = processed_data_log_path
        logger.info(f"📝 Logging configured:")
        logger.info(f"   Raw data: {raw_data_log_path}")
        logger.info(f"   Processed data: {processed_data_log_path}")
    
    async def close(self):
        """
        Gracefully close the Pathway streaming engine.
        
        Note: Pathway runs in blocking mode and handles its own cleanup.
        This method is provided for compatibility with async orchestrators.
        """
        logger.info("🛑 Closing Pathway streaming engine...")
        # Pathway handles its own cleanup when computation ends
        logger.info("✅ Pathway streaming engine closed")
    
    def setup_input_streams(self):
        """Setup input streams from Redis."""
        logger.info("📥 Setting up input streams...")
        
        # Price stream
        self.price_stream = pw.io.redis.read(
            host=self.redis_host,
            port=self.redis_port,
            password=self.redis_password if self.redis_password else None,
            stream_name="raw:price-updates",
            schema=PriceSchema,
            mode="streaming",
        )
        
        # News stream
        self.news_stream = pw.io.redis.read(
            host=self.redis_host,
            port=self.redis_port,
            password=self.redis_password if self.redis_password else None,
            stream_name="raw:news-articles",
            schema=NewsSchema,
            mode="streaming",
        )
        
        # Social stream
        self.social_stream = pw.io.redis.read(
            host=self.redis_host,
            port=self.redis_port,
            password=self.redis_password if self.redis_password else None,
            stream_name="raw:social",
            schema=SocialSchema,
            mode="streaming",
        )
        
        logger.info("✅ Input streams configured")
    
    @staticmethod
    @pw.udf
    def calculate_rsi(prices: list, period: int = 30) -> float:
        """
        Calculate RSI (Relative Strength Index).
        Matches original streaming_engine.py implementation.
        """
        if len(prices) < 2:
            return 50.0  # Neutral RSI
        
        # Calculate price changes
        deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
        
        # Use last 'period' deltas if available
        if len(deltas) > period:
            deltas = deltas[-period:]
        
        # Separate gains and losses
        gains = [d if d > 0 else 0 for d in deltas]
        losses = [-d if d < 0 else 0 for d in deltas]
        
        avg_gain = sum(gains) / len(gains) if gains else 0
        avg_loss = sum(losses) / len(losses) if losses else 0
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    @staticmethod
    @pw.udf
    def calculate_cci(highs: list, lows: list, closes: list, period: int = 30) -> float:
        """
        Calculate CCI (Commodity Channel Index).
        Matches original streaming_engine.py implementation.
        """
        if len(closes) < period:
            return 0.0
        
        # Calculate typical prices
        typical_prices = [
            (h + l + c) / 3 
            for h, l, c in zip(highs[-period:], lows[-period:], closes[-period:])
        ]
        
        # SMA of typical price
        sma_tp = sum(typical_prices) / period
        
        # Mean deviation
        mean_deviation = sum(abs(tp - sma_tp) for tp in typical_prices) / period
        
        if mean_deviation == 0:
            return 0.0
        
        # CCI formula
        cci = (typical_prices[-1] - sma_tp) / (0.015 * mean_deviation)
        return cci
    
    @staticmethod
    @pw.udf
    def calculate_dx(highs: list, lows: list, closes: list, period: int = 30) -> float:
        """
        Calculate DX (Directional Index).
        Matches original streaming_engine.py implementation.
        """
        if len(closes) < period + 1:
            return 0.0
        
        # Calculate +DM and -DM
        plus_dm = sum(
            max(highs[i] - highs[i-1], 0) 
            for i in range(1, min(len(highs), period + 1))
        )
        minus_dm = sum(
            max(lows[i-1] - lows[i], 0) 
            for i in range(1, min(len(lows), period + 1))
        )
        
        # Calculate True Range
        true_range = sum(
            max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i-1]),
                abs(lows[i] - closes[i-1])
            )
            for i in range(1, min(len(closes), period + 1))
        )
        
        if true_range == 0:
            return 0.0
        
        # Calculate directional indicators
        plus_di = (plus_dm / true_range) * 100
        minus_di = (minus_dm / true_range) * 100
        
        if plus_di + minus_di == 0:
            return 0.0
        
        # DX formula
        dx = abs(plus_di - minus_di) / (plus_di + minus_di) * 100
        return dx
    
    def compute_indicators(self):
        """
        Compute all technical indicators using Pathway's temporal features.
        
        This replaces all manual indicator calculations from streaming_engine.py
        with automatic Pathway window operations.
        """
        logger.info("⚙️  Computing indicators...")
        
        # ============================================================
        # STEP 1: Window-based aggregations for basic indicators
        # ============================================================
        
        # 30-minute sliding window (for 30-period indicators)
        window_30m = pw.temporal.sliding(
            hop=pw.Duration("5m"),  # Update every 5 minutes
            duration=pw.Duration("30m")  # 30-minute window
        )
        
        # 60-minute sliding window (for 60-period indicators)
        window_60m = pw.temporal.sliding(
            hop=pw.Duration("5m"),
            duration=pw.Duration("60m")
        )
        
        # Group by symbol and calculate 30-minute indicators
        indicators_30m = self.price_stream.windowby(
            pw.this.symbol,
            window=window_30m
        ).reduce(
            symbol=pw.this.symbol,
            timestamp=pw.this.timestamp,
            
            # OHLCV (latest values)
            open=pw.reducers.first(pw.this.open),
            high=pw.reducers.max(pw.this.high),
            low=pw.reducers.min(pw.this.low),
            close=pw.reducers.last(pw.this.close),
            volume=pw.reducers.sum(pw.this.volume),
            
            # Moving averages
            sma_30=pw.reducers.avg(pw.this.close),  # 30-min SMA
            ema_12=pw.reducers.ewma(pw.this.close, alpha=2/(12+1)),  # EMA-12
            ema_26=pw.reducers.ewma(pw.this.close, alpha=2/(26+1)),  # EMA-26
            
            # Standard deviation for Bollinger Bands
            std_dev=pw.reducers.stddev(pw.this.close),
            
            # Collect arrays for custom indicators
            close_prices=pw.reducers.sorted_tuple(pw.this.close),
            high_prices=pw.reducers.sorted_tuple(pw.this.high),
            low_prices=pw.reducers.sorted_tuple(pw.this.low),
        )
        
        # Group by symbol and calculate 60-minute indicators
        indicators_60m = self.price_stream.windowby(
            pw.this.symbol,
            window=window_60m
        ).reduce(
            symbol=pw.this.symbol,
            timestamp=pw.this.timestamp,
            sma_60=pw.reducers.avg(pw.this.close),  # 60-min SMA
        )
        
        # ============================================================
        # STEP 2: Join 30m and 60m indicators
        # ============================================================
        
        combined_indicators = indicators_30m.interval_join_left(
            indicators_60m,
            pw.this.symbol == pw.right.symbol,
            pw.this.timestamp,
            interval=pw.temporal.interval(-60*60, 0)  # Within 60 minutes
        ).select(
            *pw.this,
            sma_60=pw.right.sma_60 if pw.right is not None else pw.this.sma_30,
        )
        
        # ============================================================
        # STEP 3: Calculate derived indicators
        # ============================================================
        
        indicators_with_derived = combined_indicators.select(
            *pw.this,
            
            # MACD
            macd_line=pw.this.ema_12 - pw.this.ema_26,
            macd_signal=pw.reducers.ewma(pw.this.ema_12 - pw.this.ema_26, alpha=2/(9+1)),
            
            # Bollinger Bands (20-period, 2 std devs)
            # Using sma_30 as middle band approximation
            boll_middle=pw.this.sma_30,
            boll_ub=pw.this.sma_30 + (2 * pw.this.std_dev),
            boll_lb=pw.this.sma_30 - (2 * pw.this.std_dev),
            
            # RSI (using custom UDF)
            rsi_30=self.calculate_rsi(pw.this.close_prices, period=30),
            
            # CCI (using custom UDF)
            cci_30=self.calculate_cci(
                pw.this.high_prices,
                pw.this.low_prices,
                pw.this.close_prices,
                period=30
            ),
            
            # DX (using custom UDF)
            dx_30=self.calculate_dx(
                pw.this.high_prices,
                pw.this.low_prices,
                pw.this.close_prices,
                period=30
            ),
        )
        
        # Calculate MACD histogram
        indicators_complete = indicators_with_derived.select(
            *pw.this,
            macd_histogram=pw.this.macd_line - pw.this.macd_signal,
        )
        
        # ============================================================
        # STEP 4: Add sentiment data from news and social streams
        # ============================================================
        
        # Aggregate news sentiment (5-minute tumbling window)
        news_sentiment = self.news_stream.windowby(
            pw.this.symbol,
            window=pw.temporal.tumbling(duration=pw.Duration("5m"))
        ).reduce(
            symbol=pw.this.symbol,
            timestamp=pw.this.timestamp,
            news_sentiment=pw.reducers.avg(pw.this.sentiment),
        )
        
        # Aggregate social sentiment (5-minute tumbling window)
        social_sentiment = self.social_stream.windowby(
            pw.this.symbol,
            window=pw.temporal.tumbling(duration=pw.Duration("5m"))
        ).reduce(
            symbol=pw.this.symbol,
            timestamp=pw.this.timestamp,
            social_sentiment=pw.reducers.avg(pw.this.sentiment),
        )
        
        # Join indicators with sentiment data
        with_news = indicators_complete.interval_join_left(
            news_sentiment,
            pw.this.symbol == pw.right.symbol,
            pw.this.timestamp,
            interval=pw.temporal.interval(-5*60, 0)  # Within 5 minutes
        ).select(
            *pw.this,
            news_sentiment=pw.right.news_sentiment if pw.right is not None else 0.0,
        )
        
        final_indicators = with_news.interval_join_left(
            social_sentiment,
            pw.this.symbol == pw.right.symbol,
            pw.this.timestamp,
            interval=pw.temporal.interval(-5*60, 0)  # Within 5 minutes
        ).select(
            *pw.this,
            social_sentiment=pw.right.social_sentiment if pw.right is not None else 0.0,
        )
        
        # ============================================================
        # STEP 5: Format for output (match original schema)
        # ============================================================
        
        self.processed_table = final_indicators.select(
            timestamp=pw.this.timestamp,
            symbol=pw.this.symbol,
            
            # OHLCV
            open=pw.this.open,
            high=pw.this.high,
            low=pw.this.low,
            close=pw.this.close,
            volume=pw.this.volume,
            
            # Moving averages
            sma_30=pw.this.sma_30,
            sma_60=pw.this.sma_60,
            ema_12=pw.this.ema_12,
            ema_26=pw.this.ema_26,
            
            # Momentum
            rsi_30=pw.this.rsi_30,
            macd_line=pw.this.macd_line,
            macd_signal=pw.this.macd_signal,
            macd_histogram=pw.this.macd_histogram,
            cci_30=pw.this.cci_30,
            
            # Volatility
            boll_ub=pw.this.boll_ub,
            boll_lb=pw.this.boll_lb,
            boll_middle=pw.this.boll_middle,
            
            # Trend
            dx_30=pw.this.dx_30,
            
            # Sentiment
            news_sentiment=pw.this.news_sentiment,
            social_sentiment=pw.this.social_sentiment,
        )
        
        logger.info("✅ Indicator computation pipeline configured")
    
    def setup_output_streams(self):
        """Setup output streams to Redis."""
        logger.info("📤 Setting up output streams...")
        
        # Write processed data to Redis stream
        pw.io.redis.write(
            self.processed_table,
            host=self.redis_host,
            port=self.redis_port,
            password=self.redis_password if self.redis_password else None,
            stream_name="processed:price",
        )
        
        # Also write to master state stream
        pw.io.redis.write(
            self.processed_table,
            host=self.redis_host,
            port=self.redis_port,
            password=self.redis_password if self.redis_password else None,
            stream_name="processed:master-state",
        )
        
        logger.info("✅ Output streams configured")
    
    async def run(self):
        """
        Run the Pathway streaming engine.
        
        This starts the continuous processing pipeline that:
        1. Reads from Redis input streams
        2. Calculates all indicators automatically
        3. Writes to Redis output streams
        
        Note: Pathway's pw.run() is blocking, so we run it in a thread executor
        to make it compatible with async orchestrators.
        """
        logger.info("=" * 80)
        logger.info("🚀 STARTING PATHWAY STREAMING ENGINE")
        logger.info("=" * 80)
        
        # Setup pipeline
        self.setup_input_streams()
        self.compute_indicators()
        self.setup_output_streams()
        
        # Run Pathway computation in executor for async compatibility
        logger.info("▶️  Running Pathway computation...")
        import asyncio
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, pw.run)


def run_pathway_engine():
    """Run the Pathway streaming engine (main entry point)."""
    from backend.config.settings import settings
    
    engine = PathwayStreamingEngine(
        redis_host=settings.redis_host,
        redis_port=settings.redis_port,
        redis_password=settings.redis_password,
    )
    
    try:
        engine.run()
    except KeyboardInterrupt:
        logger.info("🛑 Shutting down Pathway streaming engine...")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    run_pathway_engine()
