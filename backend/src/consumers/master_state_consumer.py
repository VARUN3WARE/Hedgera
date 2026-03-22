"""Master State Consumer - Consumes data from Redis streams for agent processing"""

import asyncio
import json
import logging
import redis.asyncio as redis
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime


class MasterStateConsumer:
    """
    Consumer that fetches data from Redis streams for processing by agents.
    This consumer is responsible for:
    1. Fetching news data from news stream
    2. Fetching social data from social stream  
    3. Fetching market data from Redis keys
    4. Organizing data by ticker for agent consumption
    """
    
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the Master State Consumer.
        
        Args:
            redis_url: Redis connection URL
            logger: Optional logger instance
        """
        self.redis_url = redis_url
        self.logger = logger or logging.getLogger(__name__)
        self.redis_client: Optional[redis.Redis] = None
        
        # Stream names
        self.news_stream = "raw:news-articles"
        self.social_stream = "raw:social"
        
        # Data buffers organized by ticker
        self.news_by_ticker: Dict[str, List[Dict]] = {}
        self.social_by_ticker: Dict[str, List[Dict]] = {}
        self.market_by_ticker: Dict[str, Dict] = {}
        
    async def connect(self):
        """Connect to Redis"""
        self.redis_client = await redis.from_url(
            self.redis_url, 
            decode_responses=True
        )
        self.logger.info("✅ Master Consumer connected to Redis")
        
    async def disconnect(self):
        """Disconnect from Redis"""
        if self.redis_client:
            await self.redis_client.close()
            self.logger.info("✅ Master Consumer disconnected from Redis")
    
    async def fetch_news_for_tickers(self, tickers: List[str], count: int = 10) -> Dict[str, List[Dict]]:
        """
        Fetch news data from Redis stream for specified tickers.
        
        Args:
            tickers: List of ticker symbols
            count: Number of recent messages to fetch per ticker
            
        Returns:
            Dictionary mapping ticker to list of news items
        """
        news_data = {ticker: [] for ticker in tickers}
        
        try:
            # Fetch recent messages from news stream
            messages = await self.redis_client.xrevrange(
                self.news_stream,
                count=count * len(tickers)  # Fetch more to ensure we get enough per ticker
            )
            
            for msg_id, msg_data in messages:
                try:
                    # Parse the data field
                    data_str = msg_data.get("data", "{}")
                    data = json.loads(data_str)
                    
                    # Handle batch format from news producer
                    articles = []
                    if "batch" in data:
                        articles = data["batch"]
                    else:
                        articles = [data]
                    
                    # Process each article
                    for article in articles:
                        ticker = article.get("symbol") or article.get("ticker")
                        if ticker in tickers:
                            title = article.get("headline", article.get("title", ""))
                            description = article.get("description", "")
                            
                            news_data[ticker].append({
                                "ticker": ticker,
                                "title": title,
                                "description": description,
                                "url": article.get("url", ""),
                                "source": article.get("source", ""),
                                "timestamp": article.get("timestamp", datetime.now().isoformat()),
                                "sentiment": article.get("sentiment"),
                            })
                except Exception as e:
                    self.logger.debug(f"Error parsing news message: {e}")
                    continue
            
            self.logger.info(f"Fetched news for {len(tickers)} tickers")
            for ticker, items in news_data.items():
                self.logger.info(f"  {ticker}: {len(items)} news items")
                
            self.news_by_ticker.update(news_data)
            return news_data
            
        except Exception as e:
            self.logger.error(f"Error fetching news data: {e}")
            return news_data
    
    async def fetch_social_for_tickers(self, tickers: List[str], count: int = 20) -> Dict[str, List[Dict]]:
        """
        Fetch social media data from Redis stream for specified tickers.
        
        Args:
            tickers: List of ticker symbols
            count: Number of recent messages to fetch per ticker
            
        Returns:
            Dictionary mapping ticker to list of social items
        """
        social_data = {ticker: [] for ticker in tickers}
        
        try:
            # Fetch recent messages from social stream
            messages = await self.redis_client.xrevrange(
                self.social_stream,
                count=count * len(tickers)
            )
            
            for msg_id, msg_data in messages:
                try:
                    # Parse the data field
                    data_str = msg_data.get("data", "{}")
                    data = json.loads(data_str)
                    
                    ticker = data.get("symbol") or data.get("ticker")
                    if ticker in tickers:
                        # Get text from producer (now includes realistic posts)
                        text = data.get("text", data.get("content", ""))
                        
                        # Only generate fallback if truly missing
                        if not text:
                            sentiment_val = data.get("sentiment", 0)
                            if sentiment_val > 0.1:
                                text = f"Positive sentiment about {ticker} on {data.get('platform', 'social media')}"
                            elif sentiment_val < -0.1:
                                text = f"Negative sentiment about {ticker} on {data.get('platform', 'social media')}"
                            else:
                                text = f"Neutral discussion about {ticker} on {data.get('platform', 'social media')}"
                        
                        social_data[ticker].append({
                            "ticker": ticker,
                            "text": text,
                            "sentiment": data.get("sentiment", 0),
                            "platform": data.get("platform", "Unknown"),
                            "likes": data.get("likes", 0),
                            "comments": data.get("comments", 0),
                            "shares": data.get("shares", 0),
                            "engagement": data.get("engagement", 0),
                            "mention_count": data.get("mention_count", 1),
                            "timestamp": data.get("timestamp", datetime.now().isoformat()),
                        })
                except Exception as e:
                    self.logger.debug(f"Error parsing social message: {e}")
                    continue
            
            self.logger.info(f"Fetched social data for {len(tickers)} tickers")
            for ticker, items in social_data.items():
                self.logger.info(f"  {ticker}: {len(items)} social items")
                
            self.social_by_ticker.update(social_data)
            return social_data
            
        except Exception as e:
            self.logger.error(f"Error fetching social data: {e}")
            return social_data
    
    async def fetch_market_for_tickers(self, tickers: List[str]) -> Dict[str, Dict]:
        """
        Fetch market/technical data from Redis keys for specified tickers.
        
        Args:
            tickers: List of ticker symbols
            
        Returns:
            Dictionary mapping ticker to market data
        """
        market_data = {}
        
        try:
            for ticker in tickers:
                key = f"market_data:{ticker}"
                data_str = await self.redis_client.get(key)
                
                if data_str:
                    try:
                        data = json.loads(data_str)
                        market_data[ticker] = data
                        self.logger.debug(f"Fetched market data for {ticker}")
                    except json.JSONDecodeError as e:
                        self.logger.warning(f"Invalid JSON for {ticker}: {e}")
                else:
                    self.logger.debug(f"No market data found for {ticker}")
            
            self.logger.info(f"Fetched market data for {len(market_data)}/{len(tickers)} tickers")
            self.market_by_ticker.update(market_data)
            return market_data
            
        except Exception as e:
            self.logger.error(f"Error fetching market data: {e}")
            return market_data
    
    async def consume_all_for_tickers(self, tickers: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Consume all data types (news, social, market) for specified tickers.
        
        Args:
            tickers: List of ticker symbols
            
        Returns:
            Dictionary with all data organized by ticker
        """
        self.logger.info(f"Consuming data for {len(tickers)} tickers: {tickers}")
        
        # Fetch all data types in parallel
        news_task = self.fetch_news_for_tickers(tickers)
        social_task = self.fetch_social_for_tickers(tickers)
        market_task = self.fetch_market_for_tickers(tickers)
        
        news_data, social_data, market_data = await asyncio.gather(
            news_task, social_task, market_task
        )
        
        # Organize by ticker
        result = {}
        for ticker in tickers:
            result[ticker] = {
                "ticker": ticker,
                "news": news_data.get(ticker, []),
                "social": social_data.get(ticker, []),
                "market": market_data.get(ticker, {}),
                "timestamp": datetime.now().isoformat()
            }
        
        self.logger.info(f"✅ Consumed all data for {len(tickers)} tickers")
        return result
    
    def get_news_for_ticker(self, ticker: str) -> List[Dict]:
        """Get buffered news data for a ticker"""
        return self.news_by_ticker.get(ticker, [])
    
    def get_social_for_ticker(self, ticker: str) -> List[Dict]:
        """Get buffered social data for a ticker"""
        return self.social_by_ticker.get(ticker, [])
    
    def get_market_for_ticker(self, ticker: str) -> Dict:
        """Get buffered market data for a ticker"""
        return self.market_by_ticker.get(ticker, {})
    
    def clear_buffers(self):
        """Clear all data buffers"""
        self.news_by_ticker.clear()
        self.social_by_ticker.clear()
        self.market_by_ticker.clear()
        self.logger.info("Cleared all data buffers")
