"""Trading execution tools for Alpaca MCP Server."""
from .config import alpaca


def submit_order(ticker: str, side: str, quantity: int) -> dict:
    """
    Executes a market BUY or SELL order through Alpaca.
    
    Args:
        ticker: Stock ticker symbol
        side: "buy" or "sell"
        quantity: Number of shares to trade
        
    Returns:
        dict: Order confirmation with order_id and details
    """
    order = alpaca.submit_order(
        symbol=ticker,
        qty=quantity,
        side=side.lower(),
        type="market",
        time_in_force="day"
    )

    return {
        "status": "submitted",
        "order_id": order.id,
        "symbol": ticker,
        "side": side.upper(),
        "qty": quantity
    }


def submit_bulk_orders(orders: list) -> list:
    """
    Accepts a list of orders and submits them sequentially.
    
    Args:
        orders: List of order dictionaries with format:
            [
                {"ticker": "AAPL", "side": "BUY", "quantity": 10},
                {"ticker": "MSFT", "side": "SELL", "quantity": 5}
            ]
    
    Returns:
        list: List of order results with status for each order
    """
    results = []
    
    for o in orders:
        try:
            result = submit_order(o["ticker"], o["side"], o["quantity"])
            results.append(result)
        except Exception as e:
            results.append({
                "symbol": o["ticker"],
                "side": o["side"],
                "quantity": o["quantity"],
                "status": "failed",
                "error": str(e)
            })
    
    return results
