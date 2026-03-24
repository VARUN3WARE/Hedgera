"""Social Analyst Agent consuming from Redis social stream."""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate

from .base_agent import BaseRedisAgent


class SocialAnalystRedis(BaseRedisAgent):
    """Social analyst that consumes social media data from Redis and performs sentiment analysis."""

    def __init__(
        self,
        redis_url: str,
        stream_key: str = "social_stream",
        consumer_group: str = "social_analyst_group",
        consumer_name: str = "social_analyst_1",
        openai_api_key: Optional[str] = None,
        model: str = "gpt-4o-mini",
        logger: Optional[logging.Logger] = None,
    ):
        """
        Initialize the Social Analyst.

        Args:
            redis_url: Redis connection URL
            stream_key: Redis stream key for social data
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
        
        # Social data buffer for analysis
        self.social_buffer: Dict[str, List[Dict]] = {}
        self.max_buffer_size = 100
        
        # Latest analysis results
        self.latest_analysis: Dict[str, Dict] = {}

    async def process_message(self, message_id: str, data: Dict[str, Any]):
        """
        Process a social media data message and perform sentiment analysis.

        Args:
            message_id: Redis stream message ID
            data: Social data dictionary
        """
        try:
            # Parse the social data
            social_data = json.loads(data.get("data", "{}"))
            ticker = social_data.get("ticker")
            
            if not ticker:
                self.logger.warning(f"No ticker in message {message_id}")
                return

            # Add to social buffer
            if ticker not in self.social_buffer:
                self.social_buffer[ticker] = []
            
            self.social_buffer[ticker].append(social_data)
            
            # Keep buffer size manageable
            if len(self.social_buffer[ticker]) > self.max_buffer_size:
                self.social_buffer[ticker] = self.social_buffer[ticker][-self.max_buffer_size:]

            # Perform analysis if we have enough data
            if len(self.social_buffer[ticker]) >= 5:
                analysis = await self.analyze_social_sentiment(ticker)
                self.latest_analysis[ticker] = analysis
                self.logger.info(f"Updated social analysis for {ticker}")

        except Exception as e:
            self.logger.error(f"Error processing social data: {e}", exc_info=True)

    async def analyze_social_sentiment(self, ticker: str) -> Dict[str, Any]:
        """
        Analyze social media sentiment for a ticker.

        Args:
            ticker: Stock ticker symbol

        Returns:
            Analysis results dictionary
        """
        try:
            # Get social data
            social_data = self.social_buffer.get(ticker, [])
            if not social_data:
                return {"ticker": ticker, "error": "No social data available"}

            # Get recent posts
            recent_posts = social_data[-20:]  # Last 20 posts
            post_texts = []
            
            for post in recent_posts:
                text = post.get("text", post.get("content", ""))
                if text:
                    post_texts.append(text[:200])  # Limit to 200 chars

            # Calculate engagement metrics
            total_likes = sum(p.get("likes", 0) for p in recent_posts)
            total_comments = sum(p.get("comments", 0) for p in recent_posts)
            total_shares = sum(p.get("shares", 0) for p in recent_posts)
            avg_engagement = (total_likes + total_comments + total_shares) / max(len(recent_posts), 1)

            # Create analysis prompt
            prompt = ChatPromptTemplate.from_messages([
                ("system", """You are an expert social media analyst. Analyze the following social media posts and provide:
1. Overall sentiment (positive/negative/neutral)
2. Community mood and trends
3. Key discussion topics
4. Potential viral factors or concerns

Be concise and actionable."""),
                ("user", """Analyze social media for {ticker}:

Recent Posts:
{posts}

Engagement Metrics:
- Total Likes: {likes}
- Total Comments: {comments}
- Total Shares: {shares}
- Avg Engagement: {avg_engagement:.1f}

Provide your sentiment analysis and key insights.""")
            ])

            # Get LLM analysis
            chain = prompt | self.llm
            response = await chain.ainvoke({
                "ticker": ticker,
                "posts": "\n".join([f"- {p}" for p in post_texts[:10]]),
                "likes": total_likes,
                "comments": total_comments,
                "shares": total_shares,
                "avg_engagement": avg_engagement,
            })

            # Simple sentiment scoring based on keywords
            content_lower = response.content.lower()
            positive_keywords = ["positive", "bullish", "excited", "buying", "moon", "🚀"]
            negative_keywords = ["negative", "bearish", "worried", "selling", "crash", "⚠️"]
            
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

            # Calculate buzz score (normalized engagement)
            buzz_score = min(1.0, avg_engagement / 1000)  # Normalize to 0-1

            return {
                "ticker": ticker,
                "timestamp": datetime.now().isoformat(),
                "analysis_type": "social",
                "sentiment": sentiment,
                "sentiment_score": sentiment_score,
                "buzz_score": buzz_score,
                "post_count": len(social_data),
                "engagement_metrics": {
                    "likes": total_likes,
                    "comments": total_comments,
                    "shares": total_shares,
                    "avg_engagement": avg_engagement,
                },
                "llm_analysis": response.content,
            }

        except Exception as e:
            self.logger.error(f"Error analyzing social data for {ticker}: {e}", exc_info=True)
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
