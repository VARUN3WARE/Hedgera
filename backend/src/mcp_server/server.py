"""MCP Server setup - functions are exposed via HTTP in http_server.py"""
from .portfolio import get_portfolio
from .pricing import get_price
from .trading import submit_order, submit_bulk_orders


def create_server():
    """
    Create a server reference object.
    
    Note: The actual MCP tools are exposed via FastAPI HTTP endpoints
    in http_server.py, not through the MCP Server object.
    """
    # Return a simple object indicating server is ready
    # The real implementation is in http_server.py
    return {
        "portfolio": get_portfolio,
        "pricing": get_price,
        "trading": {"submit_order": submit_order, "submit_bulk_orders": submit_bulk_orders}
    }

