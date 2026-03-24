"""Configuration and initialization for Alpaca MCP Server."""
import os
from dotenv import load_dotenv
from alpaca_trade_api import REST

load_dotenv()

# ----------------------------------------------------------
# Load Alpaca Credentials
# ----------------------------------------------------------
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
ALPACA_BASE_URL = os.getenv("ALPACA_PAPER_BASE_URL", "https://paper-api.alpaca.markets")

if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
    raise ValueError("❌ Alpaca API credentials missing in .env file!")

# Initialize Alpaca REST client
alpaca = REST(
    ALPACA_API_KEY,
    ALPACA_SECRET_KEY,
    ALPACA_BASE_URL
)
