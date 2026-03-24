"""Portfolio management tools for Alpaca MCP Server."""
import requests
from .config import ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_BASE_URL


def get_portfolio() -> dict:
    """
    Fetch real-time portfolio details from Alpaca.
    Includes: cash, equity, buying power, and current positions.
    
    Returns:
        dict: Portfolio information with cash, positions, and equity details
    """
    headers = {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY
    }

    # Fetch account info
    account = requests.get(f"{ALPACA_BASE_URL}/v2/account", headers=headers).json()

    # Fetch positions
    positions = requests.get(f"{ALPACA_BASE_URL}/v2/positions", headers=headers).json()

    parsed_positions = []
    for p in positions:
        parsed_positions.append({
            "symbol": p["symbol"],
            "qty": float(p["qty"]),
            "avg_entry_price": float(p["avg_entry_price"]),
            "market_value": float(p["market_value"]),
            "current_price": float(p.get("current_price", p["avg_entry_price"])),
            "unrealized_pl": float(p["unrealized_pl"]),
            "unrealized_plpc": float(p["unrealized_plpc"])
        })

    return {
        "cash": float(account["cash"]),
        "buying_power": float(account["buying_power"]),
        "equity": float(account["equity"]),
        "portfolio_value": float(account["portfolio_value"]),
        "positions": parsed_positions
    }
