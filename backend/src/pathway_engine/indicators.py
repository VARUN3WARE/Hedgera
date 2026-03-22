"""
Technical Indicators Calculator using Pathway's Temporal Features

This module provides all technical indicators using Pathway's built-in
temporal operations, replacing manual calculations with declarative pipelines.
"""

import pathway as pw
from typing import List
import logging

logger = logging.getLogger(__name__)


class TechnicalIndicators:
    """
    Technical indicators calculator using Pathway temporal features.
    
    All indicators match the original streaming_engine.py implementation
    but use Pathway's automatic temporal processing.
    """
    
    @staticmethod
    def calculate_all(price_table: pw.Table) -> pw.Table:
        """
        Calculate all technical indicators using Pathway's temporal features.
        
        Args:
            price_table: Pathway table with price data (symbol, timestamp, OHLCV)
        
        Returns:
            Pathway table with all indicators calculated
        
        Indicators calculated:
        - Moving Averages: SMA-30, SMA-60, EMA-12, EMA-26
        - Momentum: RSI-30, MACD, CCI-30
        - Volatility: Bollinger Bands
        - Trend: DX-30
        """
        
        # ================================================================
        # STEP 1: Define temporal windows
        # ================================================================
        
        # 30-minute sliding window (hop every 5 minutes)
        window_30m = pw.temporal.sliding(
            hop=pw.Duration("5m"),
            duration=pw.Duration("30m")
        )
        
        # 60-minute sliding window (hop every 5 minutes)
        window_60m = pw.temporal.sliding(
            hop=pw.Duration("5m"),
            duration=pw.Duration("60m")
        )
        
        # ================================================================
        # STEP 2: Calculate 30-minute window indicators
        # ================================================================
        
        indicators_30m = price_table.windowby(
            pw.this.symbol,
            window=window_30m
        ).reduce(
            symbol=pw.this.symbol,
            timestamp=pw.this.timestamp,
            
            # OHLCV aggregation
            open=pw.reducers.first(pw.this.open),
            high=pw.reducers.max(pw.this.high),
            low=pw.reducers.min(pw.this.low),
            close=pw.reducers.last(pw.this.close),
            volume=pw.reducers.sum(pw.this.volume),
            
            # Moving averages
            close_30_sma=pw.reducers.avg(pw.this.close),
            ema_12=pw.reducers.ewma(pw.this.close, alpha=2/(12+1)),
            ema_26=pw.reducers.ewma(pw.this.close, alpha=2/(26+1)),
            
            # Volatility components
            close_30_std=pw.reducers.stddev(pw.this.close),
            
            # Collect arrays for custom indicators
            close_prices=pw.reducers.sorted_tuple(pw.this.close),
            high_prices=pw.reducers.sorted_tuple(pw.this.high),
            low_prices=pw.reducers.sorted_tuple(pw.this.low),
        )
        
        # ================================================================
        # STEP 3: Calculate 60-minute window indicators
        # ================================================================
        
        indicators_60m = price_table.windowby(
            pw.this.symbol,
            window=window_60m
        ).reduce(
            symbol=pw.this.symbol,
            timestamp=pw.this.timestamp,
            close_60_sma=pw.reducers.avg(pw.this.close),
        )
        
        # ================================================================
        # STEP 4: Join 30m and 60m indicators using interval join
        # ================================================================
        
        combined_indicators = indicators_30m.interval_join_left(
            indicators_60m,
            pw.this.symbol == pw.right.symbol,
            pw.this.timestamp,
            interval=pw.temporal.interval(-60*60, 0)  # Within 60 minutes
        ).select(
            # Keep all 30m indicators
            *pw.this,
            # Add 60m SMA (fallback to 30m if not available)
            close_60_sma=pw.right.close_60_sma if pw.right is not None else pw.this.close_30_sma,
        )
        
        # ================================================================
        # STEP 5: Calculate derived indicators
        # ================================================================
        
        with_derived = combined_indicators.select(
            # Keep all existing columns
            symbol=pw.this.symbol,
            timestamp=pw.this.timestamp,
            open=pw.this.open,
            high=pw.this.high,
            low=pw.this.low,
            close=pw.this.close,
            volume=pw.this.volume,
            
            # Moving averages (rename for clarity)
            sma_30=pw.this.close_30_sma,
            sma_60=pw.this.close_60_sma,
            ema_12=pw.this.ema_12,
            ema_26=pw.this.ema_26,
            
            # MACD components
            macd_line=pw.this.ema_12 - pw.this.ema_26,
            # Signal line is 9-period EMA of MACD (approximated)
            macd_signal=pw.reducers.ewma(pw.this.ema_12 - pw.this.ema_26, alpha=2/(9+1)),
            
            # Bollinger Bands
            boll_middle=pw.this.close_30_sma,
            boll_ub=pw.this.close_30_sma + (2 * pw.this.close_30_std),
            boll_lb=pw.this.close_30_sma - (2 * pw.this.close_30_std),
            
            # Keep arrays for custom UDFs
            close_prices=pw.this.close_prices,
            high_prices=pw.this.high_prices,
            low_prices=pw.this.low_prices,
        )
        
        # ================================================================
        # STEP 6: Add custom indicators (RSI, CCI, DX)
        # ================================================================
        
        final_indicators = with_derived.select(
            # Keep all columns
            *pw.this,
            
            # Calculate MACD histogram
            macd_histogram=pw.this.macd_line - pw.this.macd_signal,
            
            # Custom indicators using UDFs
            rsi_30=TechnicalIndicators.calculate_rsi_udf(pw.this.close_prices, period=30),
            cci_30=TechnicalIndicators.calculate_cci_udf(
                pw.this.high_prices,
                pw.this.low_prices,
                pw.this.close_prices,
                period=30
            ),
            dx_30=TechnicalIndicators.calculate_dx_udf(
                pw.this.high_prices,
                pw.this.low_prices,
                pw.this.close_prices,
                period=30
            ),
        )
        
        return final_indicators
    
    @staticmethod
    @pw.udf
    def calculate_rsi_udf(prices: list, period: int = 30) -> float:
        """
        Calculate RSI (Relative Strength Index).
        
        Formula:
        RSI = 100 - (100 / (1 + RS))
        where RS = Average Gain / Average Loss
        
        Args:
            prices: List of closing prices
            period: RSI period (default: 30)
        
        Returns:
            RSI value (0-100)
        """
        if len(prices) < 2:
            return 50.0  # Neutral RSI when insufficient data
        
        # Calculate price changes
        deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
        
        # Use last 'period' deltas if available
        if len(deltas) > period:
            deltas = deltas[-period:]
        
        # Separate gains and losses
        gains = [d if d > 0 else 0 for d in deltas]
        losses = [-d if d < 0 else 0 for d in deltas]
        
        # Calculate averages
        avg_gain = sum(gains) / len(gains) if gains else 0
        avg_loss = sum(losses) / len(losses) if losses else 0
        
        if avg_loss == 0:
            return 100.0
        
        # Calculate RSI
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    @staticmethod
    @pw.udf
    def calculate_cci_udf(highs: list, lows: list, closes: list, period: int = 30) -> float:
        """
        Calculate CCI (Commodity Channel Index).
        
        Formula:
        CCI = (Typical Price - SMA of Typical Price) / (0.015 * Mean Deviation)
        where Typical Price = (High + Low + Close) / 3
        
        Args:
            highs: List of high prices
            lows: List of low prices
            closes: List of closing prices
            period: CCI period (default: 30)
        
        Returns:
            CCI value
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
    def calculate_dx_udf(highs: list, lows: list, closes: list, period: int = 30) -> float:
        """
        Calculate DX (Directional Index).
        
        Formula:
        DX = |+DI - -DI| / (+DI + -DI) * 100
        where:
        +DI = (+DM / TR) * 100
        -DI = (-DM / TR) * 100
        
        Args:
            highs: List of high prices
            lows: List of low prices
            closes: List of closing prices
            period: DX period (default: 30)
        
        Returns:
            DX value
        """
        if len(closes) < period + 1:
            return 0.0
        
        # Calculate +DM (Positive Directional Movement)
        plus_dm = sum(
            max(highs[i] - highs[i-1], 0) 
            for i in range(1, min(len(highs), period + 1))
        )
        
        # Calculate -DM (Negative Directional Movement)
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
        
        # Calculate Directional Indicators
        plus_di = (plus_dm / true_range) * 100
        minus_di = (minus_dm / true_range) * 100
        
        if plus_di + minus_di == 0:
            return 0.0
        
        # Calculate DX
        dx = abs(plus_di - minus_di) / (plus_di + minus_di) * 100
        
        return dx
    
    @staticmethod
    def add_sentiment_data(
        indicators_table: pw.Table,
        news_stream: pw.Table,
        social_stream: pw.Table
    ) -> pw.Table:
        """
        Add sentiment data from news and social streams to indicators.
        
        Args:
            indicators_table: Table with technical indicators
            news_stream: News sentiment stream
            social_stream: Social sentiment stream
        
        Returns:
            Table with indicators + sentiment
        """
        
        # Aggregate news sentiment (5-minute tumbling window)
        news_sentiment = news_stream.windowby(
            pw.this.symbol,
            window=pw.temporal.tumbling(duration=pw.Duration("5m"))
        ).reduce(
            symbol=pw.this.symbol,
            timestamp=pw.this.timestamp,
            news_sentiment=pw.reducers.avg(pw.this.sentiment),
        )
        
        # Aggregate social sentiment (5-minute tumbling window)
        social_sentiment = social_stream.windowby(
            pw.this.symbol,
            window=pw.temporal.tumbling(duration=pw.Duration("5m"))
        ).reduce(
            symbol=pw.this.symbol,
            timestamp=pw.this.timestamp,
            social_sentiment=pw.reducers.avg(pw.this.sentiment),
        )
        
        # Join indicators with news sentiment
        with_news = indicators_table.interval_join_left(
            news_sentiment,
            pw.this.symbol == pw.right.symbol,
            pw.this.timestamp,
            interval=pw.temporal.interval(-5*60, 0)  # Within 5 minutes
        ).select(
            *pw.this,
            news_sentiment=pw.right.news_sentiment if pw.right is not None else 0.0,
        )
        
        # Join with social sentiment
        with_all_sentiment = with_news.interval_join_left(
            social_sentiment,
            pw.this.symbol == pw.right.symbol,
            pw.this.timestamp,
            interval=pw.temporal.interval(-5*60, 0)  # Within 5 minutes
        ).select(
            *pw.this,
            social_sentiment=pw.right.social_sentiment if pw.right is not None else 0.0,
        )
        
        return with_all_sentiment
