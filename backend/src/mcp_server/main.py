"""Main entry point for Alpaca MCP Server."""
from .server import create_server


def main():
    """Start the Alpaca MCP Server."""
    server = create_server()
    
    print("🚀 Alpaca MCP Server is running locally...")
    print("Available tools:")
    print("  - get_portfolio: Fetch portfolio details")
    print("  - get_price: Get real-time stock price")
    print("  - submit_order: Execute single trade")
    print("  - submit_bulk_orders: Execute multiple trades")
    print("\nPress Ctrl+C to stop the server.")
    
    server.run()


if __name__ == "__main__":
    main()
