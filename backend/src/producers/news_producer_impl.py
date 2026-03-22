"""Real news producer using NewsAPI.org."""
import asyncio
import aiohttp
import time
from typing import Optional, Dict, Any, Set
import logging
from backend.src.producers.base_producer_impl import BaseProducer
from backend.config.settings import settings

logger = logging.getLogger(__name__)


class NewsProducer(BaseProducer):
    """
    Real news producer using NewsAPI.org.
    
    Free tier: 100 requests/day
    Endpoint: /v2/everything
    """
    
    def __init__(self):
        super().__init__(
            stream_name="raw:news-articles",
            fetch_interval=settings.news_fetch_interval,
            name="NewsProducer",
        )
        self.symbols = settings.symbols_list  # Default to all 30 symbols
        self.active_symbols = []  # Will be set by FinRL after it selects top 10
        self.session: Optional[aiohttp.ClientSession] = None
        self.api_key = settings.news_api_key
        self.seen_articles: Set[str] = set()
        self.base_url = "https://newsapi.org/v2/everything"
        self.current_symbol_index = 0
        
        # API rate limiting (NewsAPI free tier: 100 requests/day)
        self.api_calls_today = 0
        self.max_calls_per_day = 95  # Leave some margin
        self.calls_per_cycle = 5  # Fetch 5 symbols per cycle
        self.articles_per_symbol = 3  # Number of articles to publish per symbol
        
        # Flag to control when to start fetching
        self.enabled = False  # Disabled by default until FinRL selects tickers
    
    def set_active_symbols(self, symbols: list):
        """Set the active symbols to fetch news for (called after FinRL selection)."""
        self.active_symbols = symbols
        self.enabled = True if symbols else False
        if self.enabled:
            logger.info(f"📰 News producer activated for {len(symbols)} selected tickers: {symbols}")
        else:
            logger.info("📰 News producer disabled (no active symbols)")
    
    async def initialize(self):
        """Initialize HTTP session."""
        if not self.api_key:
            logger.warning("⚠️  NEWS_API_KEY not set in environment variables. News producer will be limited.")
        
        self.session = aiohttp.ClientSession()
        logger.info(f"✅ News producer initialized (DISABLED until FinRL selects tickers)")
        logger.info(f"⏱️  Fetch interval: {self.fetch_interval} seconds")
        logger.info(f"📰 Rate limit: {self.calls_per_cycle} symbols per cycle, max {self.max_calls_per_day} calls/day")
    
    async def fetch_data(self) -> Optional[Dict[str, Any]]:
        """Fetch real news articles for selected symbols only (after FinRL runs)."""
        # Skip if not enabled or no active symbols
        if not self.enabled or not self.active_symbols:
            logger.debug("📰 News producer skipped (waiting for FinRL to select tickers)")
            return None
        
        if not self.api_key:
            logger.warning("⚠️  NEWS_API_KEY not set - cannot fetch news")
            return None
        
        articles_to_publish = []
        
        # Check rate limit
        if self.api_calls_today >= self.max_calls_per_day:
            logger.warning(f"⚠️  NewsAPI daily rate limit reached ({self.api_calls_today}/{self.max_calls_per_day})")
            return None
        
        # Fetch for limited number of symbols per cycle (round-robin)
        symbols_to_fetch = []
        for i in range(self.calls_per_cycle):
            if self.api_calls_today >= self.max_calls_per_day:
                break
            # Use active_symbols instead of all symbols
            symbols_to_fetch.append(self.active_symbols[self.current_symbol_index])
            self.current_symbol_index = (self.current_symbol_index + 1) % len(self.active_symbols)
        
        logger.info(f"📰 Fetching news for symbols: {symbols_to_fetch} (API calls: {self.api_calls_today}/{self.max_calls_per_day})")
        
        # Fetch news for selected symbols
        for symbol in symbols_to_fetch:
            try:
                articles = await self._fetch_news(symbol)
                self.api_calls_today += 1
                
                if not articles:
                    continue
                
                # Find unseen articles for this symbol
                articles_found_for_symbol = 0
                for article in articles:
                    if articles_found_for_symbol >= self.articles_per_symbol:
                        break
                    
                    article_id = article.get("url", "")
                    
                    if article_id and article_id not in self.seen_articles:
                        self.seen_articles.add(article_id)
                        
                        # Limit cache size
                        if len(self.seen_articles) > 1000:
                            self.seen_articles = set(list(self.seen_articles)[-500:])
                        
                        formatted = self._format_article(symbol, article)
                        articles_to_publish.append(formatted)
                        articles_found_for_symbol += 1
                        
                        headline = article.get('title', 'N/A')[:80]
                        logger.info(f"✅ New article for {symbol}: {headline}")
            
            except Exception as e:
                logger.error(f"❌ Error fetching news for {symbol}: {e}")
                continue
        
        # Return batch if multiple articles, single if one, None if zero
        if len(articles_to_publish) == 0:
            return None
        elif len(articles_to_publish) == 1:
            return articles_to_publish[0]
        else:
            logger.info(f"📰 Publishing batch of {len(articles_to_publish)} articles")
            return {"batch": articles_to_publish}
    
    async def _fetch_news(self, symbol: str) -> list:
        """Fetch news from NewsAPI."""
        try:
            # Search query: company name or ticker
            query = f"{symbol} stock OR {symbol} market"
            
            params = {
                "q": query,
                "apiKey": self.api_key,
                "language": "en",
                "sortBy": "publishedAt",
                "pageSize": 10,
            }
            
            async with self.session.get(self.base_url, params=params, timeout=10) as response:
                if response.status == 429:
                    logger.warning("⚠️  NewsAPI rate limit exceeded")
                    return []
                
                if response.status != 200:
                    logger.warning(f"NewsAPI error: {response.status}")
                    return []
                
                data = await response.json()
                
                if data.get("status") != "ok":
                    logger.warning(f"NewsAPI status: {data.get('message')}")
                    return []
                
                return data.get("articles", [])
        
        except asyncio.TimeoutError:
            logger.warning(f"NewsAPI timeout for {symbol}")
            return []
        except Exception as e:
            logger.error(f"NewsAPI error for {symbol}: {e}")
            return []
    
    def _format_article(self, symbol: str, article: Dict) -> Dict[str, Any]:
        """Format article data."""
        title = article.get("title", "")
        description = article.get("description", article.get("content", ""))
        
        # Use title as description fallback if description is empty
        if not description and title:
            description = title
        
        sentiment = self._calculate_sentiment(
            title + " " + description
        )
        
        return {
            "symbol": symbol,
            "sentiment": sentiment,
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
            "headline": title,
            "description": description,
            "url": article.get("url", ""),
            "source": article.get("source", {}).get("name", "Unknown"),
            "published_at": article.get("publishedAt", ""),
        }
    
    def _calculate_sentiment(self, text: str) -> float:
        """Simple keyword-based sentiment."""
        positive = [
            "bullish", "buy", "surge", "rally", "gain", "profit", "growth", 
            "rise", "up", "strong", "positive", "success", "beat", "exceed"
        ]
        negative = [
            "bearish", "sell", "crash", "fall", "loss", "decline", "drop",
            "down", "weak", "negative", "miss", "disappoint", "concern"
        ]
        
        text_lower = text.lower()
        
        pos_count = sum(1 for word in positive if word in text_lower)
        neg_count = sum(1 for word in negative if word in text_lower)
        
        total = pos_count + neg_count
        if total == 0:
            return 0.0
        
        # Normalize to [-1, 1]
        return (pos_count - neg_count) / max(total, 1)
    
    async def cleanup(self):
        """Close HTTP session."""
        if self.session:
            await self.session.close()
            logger.info("✅ News session closed")

