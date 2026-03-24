"""HTTP wrapper for MCP Server to enable remote access via REST."""
import json
import asyncio
import logging
from typing import Any, Dict
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from .server import create_server

logger = logging.getLogger(__name__)

app = FastAPI(title="Alpaca MCP Server")
mcp_server = None


@app.on_event("startup")
async def startup_event():
    """Initialize MCP server on startup."""
    global mcp_server
    try:
        mcp_server = create_server()
        logger.info("✅ MCP Server initialized")
    except Exception as e:
        logger.error(f"❌ Failed to initialize MCP server: {e}")
        raise


@app.get("/health")
async def health() -> Dict[str, str]:
    """Health check endpoint."""
    return {"status": "healthy", "service": "alpaca-mcp-server"}


@app.post("/tools/{tool_name}")
async def call_tool(tool_name: str, body: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Generic tool invocation endpoint.
    
    Args:
        tool_name: Name of the MCP tool to call
        body: JSON body with tool arguments
        
    Returns:
        Tool result as JSON
    """
    if mcp_server is None:
        raise HTTPException(status_code=503, detail="MCP server not initialized")
    
    try:
        # Map tool names to functions
        tools = {
            "get_portfolio": _call_get_portfolio,
            "get_price": _call_get_price,
            "submit_order": _call_submit_order,
            "submit_bulk_orders": _call_submit_bulk_orders,
        }
        
        if tool_name not in tools:
            raise HTTPException(
                status_code=404, 
                detail=f"Tool '{tool_name}' not found. Available: {list(tools.keys())}"
            )
        
        result = await tools[tool_name](body or {})
        return {"status": "success", "data": result}
        
    except Exception as e:
        logger.error(f"❌ Tool '{tool_name}' failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


async def _call_get_portfolio(args: Dict[str, Any]) -> Dict[str, Any]:
    """Call get_portfolio tool."""
    from .portfolio import get_portfolio
    return get_portfolio()


async def _call_get_price(args: Dict[str, Any]) -> float:
    """Call get_price tool."""
    from .pricing import get_price
    ticker = args.get("ticker")
    if not ticker:
        raise ValueError("ticker argument required")
    return get_price(ticker)


async def _call_submit_order(args: Dict[str, Any]) -> Dict[str, Any]:
    """Call submit_order tool."""
    from .trading import submit_order
    ticker = args.get("ticker")
    side = args.get("side")
    quantity = args.get("quantity")
    
    if not all([ticker, side, quantity]):
        raise ValueError("ticker, side, and quantity arguments required")
    
    return submit_order(ticker, side, int(quantity))


async def _call_submit_bulk_orders(args: Dict[str, Any]) -> list:
    """Call submit_bulk_orders tool."""
    from .trading import submit_bulk_orders
    orders = args.get("orders")
    if not orders:
        raise ValueError("orders argument required")
    
    return submit_bulk_orders(orders)


@app.get("/tools")
async def list_tools() -> Dict[str, Any]:
    """List all available tools."""
    return {
        "tools": [
            {
                "name": "get_portfolio",
                "description": "Fetch current portfolio details from Alpaca",
                "args": {}
            },
            {
                "name": "get_price",
                "description": "Get current market price for a stock",
                "args": {"ticker": "str"}
            },
            {
                "name": "submit_order",
                "description": "Execute a single market order",
                "args": {"ticker": "str", "side": "str", "quantity": "int"}
            },
            {
                "name": "submit_bulk_orders",
                "description": "Execute multiple orders",
                "args": {"orders": "list"}
            }
        ]
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
