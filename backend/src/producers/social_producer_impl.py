"""Social media sentiment producer (placeholder for future Twitter/Reddit integration)."""
import asyncio
import time
from typing import Optional, Dict, Any
import logging
import random
from backend.src.producers.base_producer_impl import BaseProducer
from backend.config.settings import settings

logger = logging.getLogger(__name__)


class SocialProducer(BaseProducer):
    """
    Social sentiment producer.
    
    Currently generates synthetic data. 
    Can be extended with Twitter API v2 or Reddit API in the future.
    """
    
    def __init__(self):
        super().__init__(
            stream_name="raw:social",
            fetch_interval=settings.social_fetch_interval,
            name="SocialProducer",
        )
        self.symbols = settings.symbols_list  # Default to all 30 symbols
        self.active_symbols = []  # Will be set by FinRL after it selects top 10
        self.current_symbol_index = 0
        
        # Flag to control when to start fetching
        self.enabled = False  # Disabled by default until FinRL selects tickers
    
    def set_active_symbols(self, symbols: list):
        """Set the active symbols to fetch social data for (called after FinRL selection)."""
        self.active_symbols = symbols
        self.enabled = True if symbols else False
        if self.enabled:
            logger.info(f"📱 Social producer activated for {len(symbols)} selected tickers: {symbols}")
        else:
            logger.info("📱 Social producer disabled (no active symbols)")
    
    async def initialize(self):
        """Initialize social producer."""
        logger.info(f"✅ Social producer initialized (DISABLED until FinRL selects tickers)")
        logger.info(f"⏱️  Fetch interval: {self.fetch_interval} seconds")
        logger.info(f"⚠️  Currently using synthetic data (Twitter/Reddit APIs can be integrated later)")
    
    async def fetch_data(self) -> Optional[Dict[str, Any]]:
        """Fetch social sentiment data for selected symbols only (after FinRL runs)."""
        # Skip if not enabled or no active symbols
        if not self.enabled or not self.active_symbols:
            logger.debug("📱 Social producer skipped (waiting for FinRL to select tickers)")
            return None
        
        # Round-robin through active symbols only
        symbol = self.active_symbols[self.current_symbol_index]
        self.current_symbol_index = (self.current_symbol_index + 1) % len(self.active_symbols)
        
        # Generate realistic social post
        post_data = self._generate_realistic_post(symbol)
        
        return post_data
    
    def _generate_synthetic_sentiment(self) -> float:
        """Generate realistic-looking sentiment value."""
        # Random walk centered around 0, range [-1, 1]
        return round(random.uniform(-0.3, 0.3), 2)
    
    def _generate_realistic_post(self, symbol: str) -> Dict[str, Any]:
        """Generate realistic social media post for a symbol."""
        
        # Templates for different sentiment types
        bullish_posts = [
            f"{symbol} earnings strong beat and ecosystem domination. {symbol} is heading higher.\n\nI believe the numbers don't lie and {symbol} is going up. Here's why I'm bullish:\n\n1. Revenue up 10% in Q3 2025, EPS beat estimates\n2. Strong product demand showing resilience\n3. Innovation pipeline looks solid for next 12 months\n4. Market share gains in key segments\n5. Shareholder returns through buybacks and dividends\n\nMy thesis: {symbol} has strong fundamentals and clear growth drivers. I expect continued upside unless macro conditions deteriorate. Risk factors include competition and regulatory headwinds, but the setup looks asymmetric to the upside.",
            f"Just added more {symbol} to my portfolio. The recent dip is a buying opportunity IMO. Strong financials, growing market share, and solid management execution. Chart looking good too - broke above resistance at key level. Target is 15% higher from here.",
            f"{symbol} absolutely crushing it this quarter. Beat on revenue, beat on earnings, raising guidance. This is what a quality growth stock looks like. Long and strong 🚀",
        ]
        
        bearish_posts = [
            f"Concerned about {symbol} valuation at these levels. Trading at premium multiples while growth is slowing. Competition intensifying and margins under pressure. Might be time to take some profits and wait for better entry.\n\nKey risks:\n1. Valuation stretched vs historical averages\n2. Macro headwinds building\n3. Competitive threats increasing\n4. Regulatory scrutiny rising\n\nNot saying sell everything, but trimming exposure makes sense here.",
            f"{symbol} looking weak here. Failed to hold key support level. Volume declining, momentum fading. Watching for potential breakdown if it can't reclaim the 50-day MA. Cash gang for now.",
            f"Disappointed with {symbol} guidance. Management being too conservative or are real issues emerging? Either way, market won't like the uncertainty. Staying on sidelines until we get clarity.",
        ]
        
        neutral_posts = [
            f"{symbol} trading in range. No strong catalyst either way right now. Waiting for next earnings to see if growth story intact. Fair value around current levels. Will reassess after more data.",
            f"Mixed signals on {symbol}. Some positives, some negatives. Not adding or trimming position. Just holding and monitoring. Need more clarity on forward outlook before making moves.",
            f"{symbol} consolidating after recent run. Healthy price action, building base for next leg. No strong conviction either direction short-term. Long-term thesis still intact though.",
        ]
        
        # Randomly select sentiment
        sentiment_choice = random.choice(['bullish', 'bearish', 'neutral'])
        
        if sentiment_choice == 'bullish':
            text = random.choice(bullish_posts)
            sentiment = round(random.uniform(0.4, 0.8), 2)
            likes = random.randint(150, 500)
            comments = random.randint(30, 100)
            shares = random.randint(20, 80)
        elif sentiment_choice == 'bearish':
            text = random.choice(bearish_posts)
            sentiment = round(random.uniform(-0.8, -0.4), 2)
            likes = random.randint(80, 300)
            comments = random.randint(20, 80)
            shares = random.randint(10, 50)
        else:
            text = random.choice(neutral_posts)
            sentiment = round(random.uniform(-0.2, 0.2), 2)
            likes = random.randint(50, 200)
            comments = random.randint(10, 50)
            shares = random.randint(5, 30)
        
        return {
            "symbol": symbol,
            "text": text,
            "sentiment": sentiment,
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
            "platform": "Reddit/Twitter",
            "likes": likes,
            "comments": comments,
            "shares": shares,
            "mention_count": random.randint(10, 100),
            "engagement": likes + comments + shares,
        }
    
    async def cleanup(self):
        """Cleanup resources."""
        logger.info("✅ Social producer cleaned up")


# Future Twitter integration (commented out for now)
"""
class TwitterProducer(BaseProducer):
    def __init__(self):
        super().__init__(
            stream_name="raw:social",
            fetch_interval=settings.social_fetch_interval,
            name="TwitterProducer",
        )
        self.bearer_token = settings.twitter_bearer_token
        self.session = None
        
    async def initialize(self):
        if not self.bearer_token:
            raise ValueError("Twitter bearer token required")
        
        headers = {"Authorization": f"Bearer {self.bearer_token}"}
        self.session = aiohttp.ClientSession(headers=headers)
        
    async def fetch_data(self):
        # Implement Twitter API v2 integration
        pass
"""
