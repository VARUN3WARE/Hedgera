"""Price fetching tools for Alpaca MCP Server."""
import yfinance as yf
from .config import alpaca


def get_price(ticker: str) -> float:
    """
    Returns best available real-time market price.
    Uses yfinance first, then Alpaca fallback.
    
    Args:
        ticker: Stock ticker symbol
        
    Returns:
        float: Current market price
        
    Raises:
        ValueError: If price cannot be fetched from any source
    """
    # Try yfinance first (best reliability)
    try:
        stock = yf.Ticker(ticker)
        info = stock.info

        price = info.get("regularMarketPrice") or \
                info.get("currentPrice") or \
                info.get("previousClose")

        if price and price > 0:
            return float(price)
    except Exception:
        pass

    # Fallback: Alpaca's last trade
    try:
        trade = alpaca.get_last_trade(ticker)
        if hasattr(trade, "price"):
            return float(trade.price)
    except Exception:
        pass

    raise ValueError(f"❌ Could not fetch a valid price for {ticker}")
