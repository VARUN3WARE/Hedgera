"""News Analyst Agent consuming from Redis news stream."""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate

from .base_agent import BaseRedisAgent


class NewsAnalystRedis(BaseRedisAgent):
    """News analyst that consumes news data from Redis and performs sentiment analysis."""

    def __init__(
        self,
        redis_url: str,
        stream_key: str = "news_stream",
        consumer_group: str = "news_analyst_group",
        consumer_name: str = "news_analyst_1",
        openai_api_key: Optional[str] = None,
        model: str = "gpt-4o-mini",
        logger: Optional[logging.Logger] = None,
    ):
        """
        Initialize the News Analyst.

        Args:
            redis_url: Redis connection URL
            stream_key: Redis stream key for news data
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
        
        # News buffer for analysis
        self.news_buffer: Dict[str, List[Dict]] = {}
        self.max_buffer_size = 50
        
        # Latest analysis results
        self.latest_analysis: Dict[str, Dict] = {}

    async def process_message(self, message_id: str, data: Dict[str, Any]):
        """
        Process a news data message and perform sentiment analysis.

        Args:
            message_id: Redis stream message ID
            data: News data dictionary
        """
        try:
            # Parse the news data
            news_data = json.loads(data.get("data", "{}"))
            ticker = news_data.get("ticker")
            
            if not ticker:
                self.logger.warning(f"No ticker in message {message_id}")
                return

            # Add to news buffer
            if ticker not in self.news_buffer:
                self.news_buffer[ticker] = []
            
            self.news_buffer[ticker].append(news_data)
            
            # Keep buffer size manageable
            if len(self.news_buffer[ticker]) > self.max_buffer_size:
                self.news_buffer[ticker] = self.news_buffer[ticker][-self.max_buffer_size:]

            # Perform analysis
            analysis = await self.analyze_news_sentiment(ticker)
            self.latest_analysis[ticker] = analysis
            self.logger.info(f"Updated news analysis for {ticker}")

        except Exception as e:
            self.logger.error(f"Error processing news data: {e}", exc_info=True)

    async def analyze_news_sentiment(self, ticker: str) -> Dict[str, Any]:
        """
        Analyze news sentiment for a ticker.

        Args:
            ticker: Stock ticker symbol

        Returns:
            Analysis results dictionary
        """
        try:
            # Get news data
            news_data = self.news_buffer.get(ticker, [])
            if not news_data:
                return {"ticker": ticker, "error": "No news data available"}

            # Get recent headlines and summaries
            recent_news = news_data[-10:]  # Last 10 news items
            headlines = [n.get("title", "") for n in recent_news]
            summaries = [n.get("summary", n.get("description", "")) for n in recent_news]

            # Create analysis prompt
            prompt = ChatPromptTemplate.from_messages([
                ("system", """You are an expert financial news analyst. Analyze the following news and provide:
1. Overall sentiment (positive/negative/neutral)
2. Key themes and topics
3. Potential market impact
4. Risk factors

Be concise and actionable."""),
                ("user", """Analyze news for {ticker}:

Headlines:
{headlines}

Summaries:
{summaries}

Provide your sentiment analysis and key insights.""")
            ])

            # Get LLM analysis
            chain = prompt | self.llm
            response = await chain.ainvoke({
                "ticker": ticker,
                "headlines": "\n".join([f"- {h}" for h in headlines if h]),
                "summaries": "\n".join([f"- {s[:200]}..." for s in summaries if s]),
            })

            # Simple sentiment scoring based on keywords
            content_lower = response.content.lower()
            positive_keywords = ["positive", "bullish", "growth", "strong", "increase", "gain"]
            negative_keywords = ["negative", "bearish", "decline", "weak", "decrease", "loss"]
            
            positive_count = sum(1 for kw in positive_keywords if kw in content_lower)
            negative_count = sum(1 for kw in negative_keywords if kw in content_lower)
            
            if positive_count > negative_count:
                sentiment = "positive"
                sentiment_score = min(0.8, 0.5 + (positive_count * 0.1))
            elif negative_count > positive_count:
                sentiment = "negative"
                sentiment_score = max(-0.8, -0.5 - (negative_count * 0.1))
            else:
                sentiment = "neutral"
                sentiment_score = 0.0

            return {
                "ticker": ticker,
                "timestamp": datetime.now().isoformat(),
                "analysis_type": "news",
                "sentiment": sentiment,
                "sentiment_score": sentiment_score,
                "news_count": len(news_data),
                "recent_headlines": headlines[:5],
                "llm_analysis": response.content,
            }

        except Exception as e:
            self.logger.error(f"Error analyzing news for {ticker}: {e}", exc_info=True)
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
