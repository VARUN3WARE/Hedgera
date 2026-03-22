"""Real-time price producer using Alpaca Paper Trading API."""
import asyncio
import aiohttp
import time
from typing import Optional, Dict, Any
import logging
from backend.src.producers.base_producer_impl import BaseProducer
from backend.config.settings import settings

logger = logging.getLogger(__name__)


class PriceProducer(BaseProducer):
    """
    Real-time price producer using Alpaca Paper Trading API.
    
    Alpaca provides real-time and historical market data.
    Paper trading endpoint allows testing without real money.
    
    Implements timestamp-based deduplication to prevent duplicate bars.
    """
    
    def __init__(self):
        super().__init__(
            stream_name="raw:price-updates",
            fetch_interval=settings.producer_fetch_interval,
            name="PriceProducer",
        )
        self.symbols = settings.symbols_list
        self.session: Optional[aiohttp.ClientSession] = None
        self.api_key = settings.alpaca_api_key
        self.secret_key = settings.alpaca_secret_key
        self.base_url = settings.alpaca_base_url
        
        # Deduplication: Track last bar timestamp for each symbol
        self.last_bar_timestamps: Dict[str, str] = {}
    
    async def initialize(self):
        """Initialize HTTP session with Alpaca credentials."""
        if not self.api_key or not self.secret_key:
            logger.error("❌ Alpaca API credentials not found! Please set ALPACA_API_KEY and ALPACA_SECRET_KEY in .env")
            raise ValueError("Alpaca API credentials are required")
        
        # Create session with authentication headers
        headers = {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.secret_key,
        }
        self.session = aiohttp.ClientSession(headers=headers)
        logger.info(f"✅ Alpaca Paper Trading API configured")
        logger.info(f"📊 Price producer initialized for {len(self.symbols)} symbols: {', '.join(self.symbols[:5])}...")
        logger.info(f"⏱️  Fetch interval: {self.fetch_interval} seconds")
    
    async def fetch_data(self) -> Optional[Dict[str, Any]]:
        """Fetch all symbols - use bars (OHLCV) instead of quotes."""
        try:
            # Fetch OHLCV bars instead of just quotes
            price_data = await self._fetch_alpaca_bars()
            return price_data
        
        except Exception as e:
            logger.error(f"❌ Error fetching prices: {e}")
            return None
    
    async def _fetch_alpaca_bars(self) -> Optional[Dict[str, Any]]:
        """
        Fetch latest 1-minute bars (OHLCV) from Alpaca for all symbols.
        
        API: GET /v2/stocks/bars/latest?symbols={symbols}&timeframe=1Min
        """
        try:
            # Get Alpaca server timestamp
            try:
                clock_url = f"{self.base_url}/v2/clock"
                async with self.session.get(clock_url, timeout=5) as clock_response:
                    if clock_response.status == 200:
                        clock_data = await clock_response.json()
                        alpaca_server_time = clock_data.get("timestamp")
                    else:
                        alpaca_server_time = None
            except:
                alpaca_server_time = None
            
            symbols_param = ",".join(self.symbols)
            url = f"{self.base_url}/v2/stocks/bars/latest"
            params = {
                "symbols": symbols_param
            }
            
            fetch_time_utc = time.time()
            
            async with self.session.get(url, params=params, timeout=10) as response:
                if response.status == 429:
                    logger.warning(f"⚠️  Alpaca rate limit hit")
                    return None
                
                if response.status != 200:
                    text = await response.text()
                    logger.warning(f"Alpaca API error: {response.status} - {text}")
                    return None
                
                data = await response.json()
                bars = data.get("bars", {})
                
                if not bars:
                    logger.warning("No bar data received from Alpaca, falling back to quotes")
                    return await self._fetch_alpaca_quotes()
                
                # Process all bars and create OHLCV data
                results = []
                duplicate_count = 0
                for symbol, bar in bars.items():
                    if bar:
                        open_price = float(bar.get("o", 0))
                        high_price = float(bar.get("h", 0))
                        low_price = float(bar.get("l", 0))
                        close_price = float(bar.get("c", 0))
                        volume = int(bar.get("v", 0))
                        bar_timestamp = bar.get("t")  # Actual bar timestamp from Alpaca
                        
                        # Deduplication: Skip if we've already seen this exact bar
                        if symbol in self.last_bar_timestamps:
                            if self.last_bar_timestamps[symbol] == bar_timestamp:
                                duplicate_count += 1
                                continue  # Skip duplicate bar
                        
                        if close_price > 0:
                            # Update last bar timestamp for this symbol
                            self.last_bar_timestamps[symbol] = bar_timestamp
                            
                            result = {
                                "symbol": symbol,
                                "price": round(close_price, 2),  # Use close as current price
                                "open": round(open_price, 2),
                                "high": round(high_price, 2),
                                "low": round(low_price, 2),
                                "close": round(close_price, 2),
                                "volume": volume,
                                "timestamp": bar_timestamp,  # Actual bar time from Alpaca
                                "alpaca_server_time": alpaca_server_time,  # Alpaca server clock
                                "fetch_time_utc": fetch_time_utc,  # When we fetched it
                                "source": "alpaca_bars"
                            }
                            results.append(result)
                            
                            if len(results) <= 3:  # Log first few
                                logger.info(f"📊 {symbol}: O=${open_price:.2f} H=${high_price:.2f} L=${low_price:.2f} C=${close_price:.2f} V={volume:,} | Bar: {bar_timestamp}")
                
                # Return all results as a batch
                if results:
                    logger.info(f"✅ Fetched {len(results)} NEW bars (skipped {duplicate_count} duplicates)")
                    return {"batch": results, "timestamp": time.time()}
                elif duplicate_count > 0:
                    logger.info(f"ℹ️  All {duplicate_count} bars were duplicates (no new data since last fetch)")
                    return None  # Don't publish anything if all were duplicates
                else:
                    logger.warning("No valid bar data, falling back to quotes")
                    return await self._fetch_alpaca_quotes()
        
        except asyncio.TimeoutError:
            logger.warning(f"Alpaca API timeout, trying quotes")
            return await self._fetch_alpaca_quotes()
        except Exception as e:
            logger.error(f"Alpaca bars API error: {e}, falling back to quotes")
            return await self._fetch_alpaca_quotes()
    
    async def _fetch_alpaca_quotes(self) -> Optional[Dict[str, Any]]:
        """
        Fetch latest quotes from Alpaca for all symbols.
        Fallback when bars are not available.
        
        API: GET /v2/stocks/quotes/latest?symbols={symbols}
        """
        try:
            # Get Alpaca server timestamp
            try:
                clock_url = f"{self.base_url}/v2/clock"
                async with self.session.get(clock_url, timeout=5) as clock_response:
                    if clock_response.status == 200:
                        clock_data = await clock_response.json()
                        alpaca_server_time = clock_data.get("timestamp")
                    else:
                        alpaca_server_time = None
            except:
                alpaca_server_time = None
            
            symbols_param = ",".join(self.symbols)
            url = f"{self.base_url}/v2/stocks/quotes/latest"
            params = {"symbols": symbols_param}
            
            fetch_time_utc = time.time()
            
            async with self.session.get(url, params=params, timeout=10) as response:
                if response.status == 429:
                    logger.warning(f"⚠️  Alpaca rate limit hit")
                    return None
                
                if response.status != 200:
                    text = await response.text()
                    logger.warning(f"Alpaca API error: {response.status} - {text}")
                    return None
                
                data = await response.json()
                quotes = data.get("quotes", {})
                
                if not quotes:
                    logger.warning("No quote data available")
                    return None
                
                # Process quotes and simulate OHLCV
                results = []
                duplicate_count = 0
                for symbol, quote in quotes.items():
                    if quote:
                        ask_price = float(quote.get("ap", 0))
                        bid_price = float(quote.get("bp", 0))
                        mid_price = (ask_price + bid_price) / 2 if ask_price and bid_price else 0
                        quote_timestamp = quote.get("t")  # Quote timestamp from Alpaca
                        
                        # Deduplication: Skip if we've already seen this exact quote
                        if symbol in self.last_bar_timestamps:
                            if self.last_bar_timestamps[symbol] == quote_timestamp:
                                duplicate_count += 1
                                continue  # Skip duplicate quote
                        
                        if mid_price > 0:
                            # Update last timestamp for this symbol
                            self.last_bar_timestamps[symbol] = quote_timestamp
                            
                            # Simulate OHLCV from quote
                            result = {
                                "symbol": symbol,
                                "price": round(mid_price, 2),
                                "open": round(mid_price, 2),  # Approximation
                                "high": round(ask_price, 2),
                                "low": round(bid_price, 2),
                                "close": round(mid_price, 2),
                                "volume": 0,  # Not available in quotes
                                "timestamp": quote_timestamp,  # Actual quote time from Alpaca
                                "alpaca_server_time": alpaca_server_time,  # Alpaca server clock
                                "fetch_time_utc": fetch_time_utc,  # When we fetched it
                                "source": "alpaca_quotes"
                            }
                            results.append(result)
                
                if results:
                    logger.info(f"✅ Fetched {len(results)} NEW quotes (skipped {duplicate_count} duplicates)")
                    return {"batch": results, "timestamp": time.time()}
                elif duplicate_count > 0:
                    logger.info(f"ℹ️  All {duplicate_count} quotes were duplicates")
                return None
        
        except Exception as e:
            logger.error(f"Alpaca quotes API error: {e}")
            return None
    
    async def cleanup(self):
        """Close HTTP session."""
        if self.session:
            await self.session.close()
            logger.info("✅ Alpaca session closed")
