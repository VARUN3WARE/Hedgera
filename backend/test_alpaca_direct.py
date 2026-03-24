"""
Simple test script to make a trade request directly via Alpaca API
"""

import os
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

load_dotenv()

# Initialize Alpaca client
api_key = os.getenv("ALPACA_API_KEY")
secret_key = os.getenv("ALPACA_SECRET_KEY")
base_url = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")

client = TradingClient(api_key=api_key, secret_key=secret_key, paper=True)

print("=" * 80)
print("ALPACA DIRECT API TEST - MAKE A TRADE")
print("=" * 80)

try:
    # Get account info
    account = client.get_account()
    print(f"\n📊 Account Info:")
    print(f"   Cash: ${float(account.cash):.2f}")
    print(f"   Buying Power: ${float(account.buying_power):.2f}")
    print(f"   Portfolio Value: ${float(account.portfolio_value):.2f}")
    
    # Get positions
    positions = client.get_all_positions()
    print(f"\n📈 Current Positions: {len(positions)}")
    for pos in positions:
        print(f"   {pos.symbol}: {float(pos.qty):.0f} shares @ ${float(pos.current_price):.2f}")
    
    # Place a test market buy order
    print(f"\n💰 Placing Test BUY Order...")
    market_order_data = MarketOrderRequest(
        symbol="AAPL",
        qty=1,
        side=OrderSide.BUY,
        time_in_force=TimeInForce.DAY,
    )
    
    order = client.submit_order(market_order_data)
    print(f"✅ Order Submitted!")
    print(f"   Order ID: {order.id}")
    print(f"   Symbol: {order.symbol}")
    print(f"   Side: {order.side}")
    print(f"   Qty: {order.qty}")
    print(f"   Status: {order.status}")
    print(f"   Type: {order.order_type}")
    
    # Get the order details
    print(f"\n📝 Order Details:")
    print(f"   Created At: {order.created_at}")
    print(f"   Updated At: {order.updated_at}")
    print(f"   Filled Qty: {order.filled_qty}")
    print(f"   Filled Avg Price: {order.filled_avg_price}")
    
    # Get all orders
    print(f"\n📋 Recent Orders:")
    orders = client.get_orders(status="all", limit=5)
    for o in orders:
        print(f"   {o.id}: {o.symbol} {o.side} {o.qty} - {o.status}")
    
    print(f"\n✅ Test Complete!")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()

print("=" * 80)
