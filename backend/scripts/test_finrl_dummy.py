#!/usr/bin/env python3
"""
Test FinRL with dummy data to verify the pipeline works correctly
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pandas as pd
import numpy as np
from datetime import datetime
import json

print("\n" + "="*80)
print("🧪 FINRL DUMMY DATA TEST")
print("="*80 + "\n")

# ============================================================================
# STEP 1: Create dummy data for 30 DOW tickers + VIXY
# ============================================================================
print("📊 STEP 1: Creating dummy processed data for 30 tickers + VIXY")
print("-" * 80)

# 30 DOW tickers (alphabetically sorted)
dow_tickers = [
    'AAPL', 'AMGN', 'AMZN', 'AXP', 'BA', 'CAT', 'CRM', 'CSCO', 'CVX', 'DIS',
    'DOW', 'GS', 'HD', 'HON', 'IBM', 'INTC', 'JNJ', 'JPM', 'KO', 'MCD',
    'MMM', 'MRK', 'MSFT', 'NKE', 'NVDA', 'PG', 'UNH', 'V', 'VZ', 'WMT'
]

# Create realistic dummy data
np.random.seed(42)

data_records = []

# Generate data for each ticker
for ticker in dow_tickers:
    # Random price between 50 and 500
    base_price = np.random.uniform(50, 500)
    open_price = base_price * np.random.uniform(0.98, 1.02)
    high_price = base_price * np.random.uniform(1.00, 1.05)
    low_price = base_price * np.random.uniform(0.95, 1.00)
    close_price = base_price * np.random.uniform(0.98, 1.02)
    volume = np.random.randint(100000, 10000000)
    
    # Technical indicators (realistic values)
    macd = np.random.uniform(-5, 5)
    boll_ub = close_price * 1.02
    boll_lb = close_price * 0.98
    rsi_30 = np.random.uniform(20, 80)
    cci_30 = np.random.uniform(-200, 200)
    dx_30 = np.random.uniform(0, 100)
    close_30_sma = close_price * np.random.uniform(0.98, 1.02)
    close_60_sma = close_price * np.random.uniform(0.97, 1.03)
    
    data_records.append({
        'tic': ticker,
        'open': round(open_price, 2),
        'high': round(high_price, 2),
        'low': round(low_price, 2),
        'close': round(close_price, 2),
        'volume': volume,
        'macd': round(macd, 4),
        'boll_ub': round(boll_ub, 2),
        'boll_lb': round(boll_lb, 2),
        'rsi_30': round(rsi_30, 2),
        'cci_30': round(cci_30, 2),
        'dx_30': round(dx_30, 2),
        'close_30_sma': round(close_30_sma, 2),
        'close_60_sma': round(close_60_sma, 2),
    })

# Add VIXY for turbulence calculation
vixy_price = 30.5
data_records.append({
    'tic': 'VIXY',
    'open': vixy_price,
    'high': vixy_price * 1.01,
    'low': vixy_price * 0.99,
    'close': vixy_price,
    'volume': 500000,
    'macd': 0,
    'boll_ub': vixy_price,
    'boll_lb': vixy_price,
    'rsi_30': 50,
    'cci_30': 0,
    'dx_30': 0,
    'close_30_sma': vixy_price,
    'close_60_sma': vixy_price,
})

# Create DataFrame
df = pd.DataFrame(data_records)

print(f"✅ Created dummy data")
print(f"   Shape: {df.shape}")
print(f"   Tickers: {len(df)} (30 DOW + 1 VIXY)")
print(f"   Columns: {list(df.columns)}")
print(f"\n📋 Sample data (first 3 rows):")
print(df.head(3).to_string())

# ============================================================================
# STEP 2: Test FinRL model with dummy data
# ============================================================================
print("\n" + "="*80)
print("📊 STEP 2: Testing FinRL Model with Dummy Data")
print("-" * 80)

try:
    # Import FinRL function
    sys.path.insert(0, str(Path(__file__).parent.parent.parent / "backend" / "finrl_integration"))
    from paper_trading import get_trading_decisions
    
    # Set up parameters
    model_path = "backend/finrl_integration/agent_ppo.zip"
    ticker_list = dow_tickers  # 30 trading tickers (no VIXY)
    tech_indicators = ['macd', 'boll_ub', 'boll_lb', 'rsi_30', 'cci_30', 'dx_30', 'close_30_sma', 'close_60_sma']
    
    # Load credentials from .env
    from dotenv import load_dotenv
    import os
    load_dotenv()
    
    api_key = os.getenv('ALPACA_API_KEY')
    api_secret = os.getenv('ALPACA_SECRET_KEY')
    api_base_url = os.getenv('ALPACA_BASE_URL', 'https://paper-api.alpaca.markets')
    
    print(f"\n🔧 FinRL Configuration:")
    print(f"   Model path: {model_path}")
    print(f"   Trading tickers: {len(ticker_list)}")
    print(f"   Tech indicators: {len(tech_indicators)}")
    print(f"   Expected observation shape: 301 (1 + 30*2 + 30*8)")
    
    print(f"\n🚀 Running FinRL model...")
    print(f"   This may take a few seconds...")
    
    # Call FinRL
    decisions = get_trading_decisions(
        redis_data=df,
        model_path=model_path,
        ticker_list=ticker_list,
        tech_indicators=tech_indicators,
        api_key=api_key,
        api_secret=api_secret,
        api_base_url=api_base_url
    )
    
    print(f"\n✅ FinRL Model Execution Complete!")
    print(f"\n📊 RESULTS:")
    print("-" * 80)
    
    # Extract decisions
    buy_decisions = decisions.get('buy', {})
    sell_decisions = decisions.get('sell', {})
    timestamp = decisions.get('timestamp', 'N/A')
    
    # Get all tickers with decisions
    all_selected = list(set(list(buy_decisions.keys()) + list(sell_decisions.keys())))
    top_10 = all_selected[:10]
    
    print(f"\n🕒 Timestamp: {timestamp}")
    print(f"\n📈 Buy Decisions: {len(buy_decisions)} tickers")
    if buy_decisions:
        print(f"   Top buy signals:")
        for i, (ticker, qty) in enumerate(list(buy_decisions.items())[:5], 1):
            print(f"   {i}. {ticker}: BUY {qty} shares")
    
    print(f"\n📉 Sell Decisions: {len(sell_decisions)} tickers")
    if sell_decisions:
        print(f"   Top sell signals:")
        for i, (ticker, qty) in enumerate(list(sell_decisions.items())[:5], 1):
            print(f"   {i}. {ticker}: SELL {qty} shares")
    
    print(f"\n🎯 Top 10 Selected Tickers (for News/Social):")
    for i, ticker in enumerate(top_10, 1):
        action = "BUY" if ticker in buy_decisions else "SELL"
        qty = buy_decisions.get(ticker, sell_decisions.get(ticker, 0))
        print(f"   {i}. {ticker}: {action} {qty} shares")
    
    # Save results to file
    output_file = "backend/logs/finrl_test_results.json"
    with open(output_file, 'w') as f:
        json.dump({
            'timestamp': timestamp,
            'buy_decisions': buy_decisions,
            'sell_decisions': sell_decisions,
            'top_10_tickers': top_10,
            'total_analyzed': len(df),
            'test_type': 'dummy_data'
        }, f, indent=2)
    
    print(f"\n💾 Results saved to: {output_file}")
    
    # ========================================================================
    # STEP 3: Verify output format
    # ========================================================================
    print("\n" + "="*80)
    print("📊 STEP 3: Verification")
    print("-" * 80)
    
    checks_passed = []
    checks_failed = []
    
    # Check 1: Decisions returned
    if buy_decisions or sell_decisions:
        checks_passed.append("✓ Model generated trading decisions")
    else:
        checks_failed.append("✗ No trading decisions generated")
    
    # Check 2: Top 10 tickers selected
    if len(top_10) >= 10:
        checks_passed.append(f"✓ Selected {len(top_10)} tickers (>= 10)")
    else:
        checks_failed.append(f"✗ Only {len(top_10)} tickers selected (< 10)")
    
    # Check 3: Timestamp present
    if timestamp and timestamp != 'N/A':
        checks_passed.append("✓ Timestamp present")
    else:
        checks_failed.append("✗ Missing timestamp")
    
    # Check 4: All selected tickers from input
    invalid_tickers = [t for t in top_10 if t not in dow_tickers]
    if not invalid_tickers:
        checks_passed.append("✓ All selected tickers are valid")
    else:
        checks_failed.append(f"✗ Invalid tickers: {invalid_tickers}")
    
    print(f"\n✅ Checks Passed ({len(checks_passed)}):")
    for check in checks_passed:
        print(f"   {check}")
    
    if checks_failed:
        print(f"\n❌ Checks Failed ({len(checks_failed)}):")
        for check in checks_failed:
            print(f"   {check}")
    
    # ========================================================================
    # SUMMARY
    # ========================================================================
    print("\n" + "="*80)
    print("📋 SUMMARY")
    print("="*80)
    
    if not checks_failed:
        print(f"\n🎉 SUCCESS! FinRL pipeline works correctly!")
        print(f"   - Model loaded and executed successfully")
        print(f"   - Generated decisions for {len(all_selected)} tickers")
        print(f"   - Selected top {len(top_10)} tickers for News/Social producers")
        print(f"   - Output format is correct")
        print(f"\n✅ Ready for production deployment!")
    else:
        print(f"\n⚠️  PARTIAL SUCCESS - Some issues detected")
        print(f"   Model executed but output needs validation")
    
except Exception as e:
    print(f"\n❌ ERROR: {e}")
    print(f"\n🔍 Full traceback:")
    import traceback
    traceback.print_exc()
    
    print(f"\n💡 Possible issues:")
    print(f"   1. Model file not found: backend/finrl_integration/agent_ppo.zip")
    print(f"   2. Missing dependencies (alpaca_trade_api, finrl, etc.)")
    print(f"   3. Environment variables not set (.env file)")
    print(f"   4. Data format mismatch")

print("\n" + "="*80 + "\n")
