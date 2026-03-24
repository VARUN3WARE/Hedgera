"""Market Analyst Agent consuming from Redis price stream."""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate

from .base_agent import BaseRedisAgent


class MarketAnalystRedis(BaseRedisAgent):
    """Market analyst that consumes price data from Redis and performs technical analysis."""

    def __init__(
        self,
        redis_url: str,
        stream_key: str = "price_stream",
        consumer_group: str = "market_analyst_group",
        consumer_name: str = "market_analyst_1",
        openai_api_key: Optional[str] = None,
        model: str = "gpt-4o-mini",
        logger: Optional[logging.Logger] = None,
    ):
        """
        Initialize the Market Analyst.

        Args:
            redis_url: Redis connection URL
            stream_key: Redis stream key for price data
            consumer_group: Consumer group name
            consumer_name: Consumer name within the group
            openai_api_key: OpenAI API key for LLM
            model: OpenAI model to use
            logger: Optional logger instance
        """
        super().__init__(redis_url, stream_key, consumer_group, consumer_name, logger)
        
        # Initialize LLM
        self.llm = ChatOpenAI(
            model=model,
            api_key=openai_api_key,
            temperature=0.1,
        )
        
        # Price data buffer for technical analysis
        self.price_buffer: Dict[str, List[Dict]] = {}
        self.max_buffer_size = 100
        
        # Latest analysis results
        self.latest_analysis: Dict[str, Dict] = {}

    async def process_message(self, message_id: str, data: Dict[str, Any]):
        """
        Process a price data message and perform analysis.

        Args:
            message_id: Redis stream message ID
            data: Price data dictionary
        """
        try:
            # Parse the price data
            price_data = json.loads(data.get("data", "{}"))
            ticker = price_data.get("ticker")
            
            if not ticker:
                self.logger.warning(f"No ticker in message {message_id}")
                return

            # Add to price buffer
            if ticker not in self.price_buffer:
                self.price_buffer[ticker] = []
            
            self.price_buffer[ticker].append(price_data)
            
            # Keep buffer size manageable
            if len(self.price_buffer[ticker]) > self.max_buffer_size:
                self.price_buffer[ticker] = self.price_buffer[ticker][-self.max_buffer_size:]

            # Perform analysis if we have enough data
            if len(self.price_buffer[ticker]) >= 20:
                analysis = await self.analyze_market_data(ticker)
                self.latest_analysis[ticker] = analysis
                self.logger.info(f"Updated analysis for {ticker}")

        except Exception as e:
            self.logger.error(f"Error processing price data: {e}", exc_info=True)

    async def analyze_market_data(self, ticker: str) -> Dict[str, Any]:
        """
        Analyze market data for a ticker.

        Args:
            ticker: Stock ticker symbol

        Returns:
            Analysis results dictionary
        """
        try:
            # Get price data
            price_data = self.price_buffer.get(ticker, [])
            if not price_data:
                return {"ticker": ticker, "error": "No price data available"}

            # Convert to DataFrame for analysis
            df = pd.DataFrame(price_data)
            
            # Calculate technical indicators
            latest_price = df["price"].iloc[-1] if "price" in df.columns else None
            avg_price = df["price"].mean() if "price" in df.columns else None
            price_change = (
                ((df["price"].iloc[-1] - df["price"].iloc[0]) / df["price"].iloc[0] * 100)
                if "price" in df.columns and len(df) > 0
                else 0
            )
            
            # Calculate simple moving averages
            if "price" in df.columns and len(df) >= 20:
                df["sma_20"] = df["price"].rolling(window=20).mean()
                df["sma_10"] = df["price"].rolling(window=10).mean()
                sma_20 = df["sma_20"].iloc[-1]
                sma_10 = df["sma_10"].iloc[-1]
            else:
                sma_20 = avg_price
                sma_10 = avg_price

            # Create analysis prompt
            prompt = ChatPromptTemplate.from_messages([
                ("system", """You are an expert market analyst. Analyze the following market data and provide:
1. Overall market sentiment (bullish/bearish/neutral)
2. Key technical indicators analysis
3. Short-term outlook
4. Risk assessment

Be concise and actionable."""),
                ("user", """Analyze {ticker}:
- Latest Price: ${latest_price:.2f}
- Average Price: ${avg_price:.2f}
- Price Change: {price_change:.2f}%
- SMA(10): ${sma_10:.2f}
- SMA(20): ${sma_20:.2f}
- Data Points: {data_points}

Provide your analysis.""")
            ])

            # Get LLM analysis
            chain = prompt | self.llm
            response = await chain.ainvoke({
                "ticker": ticker,
                "latest_price": latest_price or 0,
                "avg_price": avg_price or 0,
                "price_change": price_change,
                "sma_10": sma_10 or 0,
                "sma_20": sma_20 or 0,
                "data_points": len(df),
            })

            # Determine sentiment
            sentiment = "neutral"
            if latest_price and sma_20:
                if latest_price > sma_20 and sma_10 > sma_20:
                    sentiment = "bullish"
                elif latest_price < sma_20 and sma_10 < sma_20:
                    sentiment = "bearish"

            return {
                "ticker": ticker,
                "timestamp": datetime.now().isoformat(),
                "analysis_type": "market",
                "latest_price": latest_price,
                "price_change_percent": price_change,
                "sentiment": sentiment,
                "sma_10": sma_10,
                "sma_20": sma_20,
                "llm_analysis": response.content,
                "data_points": len(df),
            }

        except Exception as e:
            self.logger.error(f"Error analyzing market data for {ticker}: {e}", exc_info=True)
            return {
                "ticker": ticker,
                "timestamp": datetime.now().isoformat(),
                "error": str(e),
            }

    def get_latest_analysis(self, ticker: Optional[str] = None) -> Dict[str, Any]:
        """
        Get the latest analysis results.

        Args:
            ticker: Optional ticker to get analysis for. If None, returns all.

        Returns:
            Analysis results dictionary
        """
        if ticker:
            return self.latest_analysis.get(ticker, {})
        return self.latest_analysis
