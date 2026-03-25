#!/usr/bin/env python3
"""
Debug FinRL model predictions - check what the model is actually outputting
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "backend" / "finrl_integration"))

import pandas as pd
import numpy as np

print("\n" + "="*80)
print("🔍 FINRL MODEL DEBUG")
print("="*80 + "\n")

# Create minimal dummy data
dow_tickers = [
    'AAPL', 'AMGN', 'AMZN', 'AXP', 'BA', 'CAT', 'CRM', 'CSCO', 'CVX', 'DIS',
    'DOW', 'GS', 'HD', 'HON', 'IBM', 'INTC', 'JNJ', 'JPM', 'KO', 'MCD',
    'MMM', 'MRK', 'MSFT', 'NKE', 'NVDA', 'PG', 'UNH', 'V', 'VZ', 'WMT'
]

np.random.seed(42)
data_records = []

for ticker in dow_tickers:
    base_price = np.random.uniform(50, 500)
    data_records.append({
        'tic': ticker,
        'open': base_price,
        'high': base_price * 1.02,
        'low': base_price * 0.98,
        'close': base_price,
        'volume': 1000000,
        'macd': np.random.uniform(-2, 2),
        'boll_ub': base_price * 1.02,
        'boll_lb': base_price * 0.98,
        'rsi_30': np.random.uniform(30, 70),
        'cci_30': np.random.uniform(-100, 100),
        'dx_30': np.random.uniform(10, 50),
        'close_30_sma': base_price,
        'close_60_sma': base_price,
    })

# Add VIXY
data_records.append({
    'tic': 'VIXY',
    'open': 30, 'high': 31, 'low': 29, 'close': 30,
    'volume': 500000,
    'macd': 0, 'boll_ub': 30, 'boll_lb': 30,
    'rsi_30': 50, 'cci_30': 0, 'dx_30': 0,
    'close_30_sma': 30, 'close_60_sma': 30,
})

df = pd.DataFrame(data_records)

print(f"✅ Created test DataFrame: {df.shape}")

# Import and test
from paper_trading import PaperTradingJSON
from dotenv import load_dotenv
import os

load_dotenv()

print(f"\n🔧 Creating PaperTradingJSON instance...")

paper_trading = PaperTradingJSON(
    ticker_list=dow_tickers,
    time_interval='1Min',
    drl_lib='stable_baselines3',
    agent='ppo',
    cwd='backend/finrl_integration/agent_ppo.zip',
    state_dim=301,
    action_dim=30,
    net_dim=[64, 64],
    API_KEY=os.getenv('ALPACA_API_KEY'),
    API_SECRET=os.getenv('ALPACA_SECRET_KEY'),
    API_BASE_URL=os.getenv('ALPACA_BASE_URL'),
    tech_indicator_list=['macd', 'boll_ub', 'boll_lb', 'rsi_30', 'cci_30', 'dx_30', 'close_30_sma', 'close_60_sma'],
    max_stock=100
)

print(f"✅ Instance created")
print(f"   Stock Universe: {len(paper_trading.stockUniverse)} tickers")
print(f"   Tickers: {paper_trading.stockUniverse[:5]}...")

# Get state
print(f"\n🔧 Building state vector from DataFrame...")
state = paper_trading.get_state_from_redis(df)
print(f"✅ State vector created")
print(f"   Shape: {state.shape}")
print(f"   Expected: (301,)")
print(f"   Min value: {state.min():.6f}")
print(f"   Max value: {state.max():.6f}")
print(f"   Mean: {state.mean():.6f}")
print(f"   Contains NaN: {np.isnan(state).any()}")
print(f"   Contains Inf: {np.isinf(state).any()}")

# Get model prediction
print(f"\n🔧 Getting model prediction...")
action = paper_trading.model.predict(state)[0]
print(f"✅ Model prediction received")
print(f"   Action shape: {action.shape}")
print(f"   Expected: (30,) - one action per ticker")
print(f"   Min action: {action.min():.6f}")
print(f"   Max action: {action.max():.6f}")
print(f"   Mean action: {action.mean():.6f}")
print(f"   Action values (first 10): {action[:10]}")

# Check turbulence
print(f"\n🔧 Checking turbulence...")
print(f"   Turbulence bool: {paper_trading.turbulence_bool}")
print(f"   Max stock: {paper_trading.max_stock}")

# Manually process actions
print(f"\n🔧 Processing actions into buy/sell decisions...")
print(f"   Initial stocks_cd: {paper_trading.stocks_cd[:10]}")

# Initialize stocks_cd if not already done
if not hasattr(paper_trading, 'stocks_cd') or paper_trading.stocks_cd is None:
    paper_trading.stocks_cd = np.zeros(len(dow_tickers), dtype=int)

paper_trading.stocks_cd += 1
min_action = 10

buy_count = 0
sell_count = 0

for index in range(len(dow_tickers)):
    if action[index] < -min_action and paper_trading.stocks_cd[index] == 0:
        sell_count += 1
        print(f"   SELL signal for {dow_tickers[index]}: action={action[index]:.2f}")
    elif action[index] > min_action and paper_trading.stocks_cd[index] == 0:
        buy_count += 1
        print(f"   BUY signal for {dow_tickers[index]}: action={action[index]:.2f}")

print(f"\n📊 Summary:")
print(f"   Total BUY signals: {buy_count}")
print(f"   Total SELL signals: {sell_count}")
print(f"   Total signals: {buy_count + sell_count}")

if buy_count == 0 and sell_count == 0:
    print(f"\n❌ PROBLEM FOUND!")
    print(f"   No trading signals generated!")
    print(f"   Possible reasons:")
    print(f"   1. All actions between -10 and +10 (too small)")
    print(f"   2. stocks_cd values are not 0")
    print(f"   3. Turbulence threshold triggered")
    print(f"\n🔍 Detailed Analysis:")
    print(f"   Actions > 10: {(action > 10).sum()}")
    print(f"   Actions < -10: {(action < -10).sum()}")
    print(f"   stocks_cd == 0: {(paper_trading.stocks_cd == 0).sum()}")
    print(f"   Turbulence mode: {paper_trading.turbulence_bool == 1}")

print("\n" + "="*80 + "\n")
