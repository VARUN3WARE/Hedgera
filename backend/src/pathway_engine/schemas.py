"""Pathway schemas for AEGIS Trading System streams."""

import pathway as pw


class PriceSchema(pw.Schema):
    """Schema for raw price data from Alpaca."""
    timestamp: int
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: int


class NewsSchema(pw.Schema):
    """Schema for news articles."""
    timestamp: int
    symbol: str
    title: str
    description: str
    sentiment: float
    source: str
    url: str


class SocialSchema(pw.Schema):
    """Schema for social media posts."""
    timestamp: int
    symbol: str
    text: str
    sentiment: float
    engagement: int
    platform: str


class ProcessedPriceSchema(pw.Schema):
    """Schema for processed price data with all technical indicators."""
    timestamp: int
    symbol: str
    
    # OHLCV data
    open: float
    high: float
    low: float
    close: float
    volume: float
    
    # Moving averages
    sma_30: float
    sma_60: float
    ema_12: float
    ema_26: float
    
    # Momentum indicators
    rsi_30: float
    macd_line: float
    macd_signal: float
    macd_histogram: float
    cci_30: float
    
    # Volatility indicators
    boll_ub: float  # Bollinger Upper Band
    boll_lb: float  # Bollinger Lower Band
    boll_middle: float
    
    # Trend indicators
    dx_30: float
    
    # Sentiment
    news_sentiment: float
    social_sentiment: float
