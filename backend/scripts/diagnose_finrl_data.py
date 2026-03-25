#!/usr/bin/env python3
"""
Diagnose FinRL data flow - Check what FinRL receives and outputs
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import redis
import json
import pandas as pd
import numpy as np
from datetime import datetime

print("\n" + "="*80)
print("🔍 FINRL DATA FLOW DIAGNOSTIC")
print("="*80 + "\n")

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# ============================================================================
# STEP 1: Check processed:price stream
# ============================================================================
print("📊 STEP 1: Checking processed:price stream")
print("-" * 80)

entries = r.xrevrange('processed:price', count=35)  # Get 35 to cover all tickers
if not entries:
    print("❌ No data in processed:price!")
    sys.exit(1)

print(f"✅ Found {len(entries)} entries in stream")

# Parse all entries
all_tickers = {}
for entry_id, fields in entries:
    if 'data' in fields:
        try:
            data = json.loads(fields['data'])
            ticker = data['metadata']['ticker']
            all_tickers[ticker] = data
        except Exception as e:
            print(f"⚠️  Error parsing entry: {e}")

print(f"\n📋 Tickers found in Redis: {len(all_tickers)}")
print(f"   {sorted(all_tickers.keys())}")

# ============================================================================
# STEP 2: Build DataFrame like FinRL service does
# ============================================================================
print("\n" + "="*80)
print("📊 STEP 2: Building DataFrame (simulating finrl_integrated_service)")
print("-" * 80)

ticker_data = {}
vixy_value = None

for ticker, data in all_tickers.items():
    try:
        # Handle VIXY
        if ticker == 'VIXY':
            price_data = data.get('price_data', {})
            vixy_value = price_data.get('close', 0)
            ticker_data['VIXY'] = {
                'tic': 'VIXY',
                'open': price_data.get('open', 0),
                'high': price_data.get('high', 0),
                'low': price_data.get('low', 0),
                'close': price_data.get('close', 0),
                'volume': price_data.get('volume', 0),
                'macd': 0,
                'boll_ub': 0,
                'boll_lb': 0,
                'rsi_30': 50,
                'cci_30': 0,
                'dx_30': 0,
                'close_30_sma': price_data.get('close', 0),
                'close_60_sma': price_data.get('close', 0),
            }
            continue
        
        # Handle trading tickers
        price_data = data.get('price_data', {})
        momentum = data.get('momentum_indicators', {})
        volatility = data.get('volatility_indicators', {})
        trend = data.get('trend_indicators', {})
        moving_averages = data.get('moving_averages', {})
        
        ticker_data[ticker] = {
            'tic': ticker,
            'open': price_data.get('open', 0),
            'high': price_data.get('high', 0),
            'low': price_data.get('low', 0),
            'close': price_data.get('close', 0),
            'volume': price_data.get('volume', 0),
            'macd': momentum.get('macd', {}).get('macd_line', 0),
            'boll_ub': volatility.get('boll_ub', 0),
            'boll_lb': volatility.get('boll_lb', 0),
            'rsi_30': momentum.get('rsi_30', 50),
            'cci_30': momentum.get('cci_30', 0),
            'dx_30': trend.get('dx_30', 0),
            'close_30_sma': moving_averages.get('close_30_sma', price_data.get('close', 0)),
            'close_60_sma': moving_averages.get('close_60_sma', price_data.get('close', 0)),
        }
    except Exception as e:
        print(f"⚠️  Error processing {ticker}: {e}")

df = pd.DataFrame(list(ticker_data.values()))

print(f"✅ DataFrame created")
print(f"   Shape: {df.shape}")
print(f"   Columns: {list(df.columns)}")
print(f"   Tickers: {sorted(df['tic'].tolist())}")

# ============================================================================
# STEP 3: Check DataFrame integrity
# ============================================================================
print("\n" + "="*80)
print("📊 STEP 3: DataFrame Integrity Check")
print("-" * 80)

trading_tickers = df[df['tic'] != 'VIXY']
vixy_ticker = df[df['tic'] == 'VIXY']

print(f"\n✅ Trading Tickers: {len(trading_tickers)}")
print(f"   Expected: 30")
print(f"   Status: {'✓ CORRECT' if len(trading_tickers) == 30 else '✗ WRONG!'}")

print(f"\n✅ VIXY Present: {not vixy_ticker.empty}")
if not vixy_ticker.empty:
    print(f"   VIXY close: {vixy_ticker['close'].iloc[0]}")

# Check for required columns
required_cols = ['tic', 'open', 'high', 'low', 'close', 'volume',
                'macd', 'boll_ub', 'boll_lb', 'rsi_30', 'cci_30', 
                'dx_30', 'close_30_sma', 'close_60_sma']

print(f"\n📋 Column Check:")
missing = []
for col in required_cols:
    if col in df.columns:
        print(f"   ✓ {col}")
    else:
        print(f"   ✗ {col} MISSING!")
        missing.append(col)

if missing:
    print(f"\n❌ MISSING COLUMNS: {missing}")
    sys.exit(1)

# ============================================================================
# STEP 4: Simulate paper_trading.py state construction
# ============================================================================
print("\n" + "="*80)
print("📊 STEP 4: Simulating State Vector Construction")
print("-" * 80)

tech_indicator_list = ['macd', 'boll_ub', 'boll_lb', 'rsi_30', 'cci_30', 'dx_30', 'close_30_sma', 'close_60_sma']

# Get 30 trading tickers (exclude VIXY)
trading_ticker_list = sorted([t for t in df['tic'].unique() if t != 'VIXY'])
print(f"\n📋 Trading Ticker List (stockUniverse): {len(trading_ticker_list)} tickers")
print(f"   {trading_ticker_list}")

# Build state vector
price = []
tech = []

for ticker in trading_ticker_list:
    ticker_data_row = df[df['tic'] == ticker]
    if ticker_data_row.empty:
        print(f"   ❌ Missing data for {ticker}!")
        continue
    
    price.append(float(ticker_data_row['close'].iloc[0]))
    
    for indicator in tech_indicator_list:
        tech.append(float(ticker_data_row[indicator].iloc[0]))

price = np.array(price, dtype=np.float32)
tech = np.array(tech, dtype=np.float32)

# Get VIXY for turbulence
vixy_data = df[df['tic'] == 'VIXY']
turbulence_value = float(vixy_data['close'].iloc[0]) if not vixy_data.empty else 0.0
turbulence_bool = 1

# Portfolio (initialized to 0)
stocks = np.zeros(len(trading_ticker_list), dtype=np.float32)

print(f"\n🔢 State Components:")
print(f"   turbulence_bool: {turbulence_bool} (shape: 1)")
print(f"   price: shape {price.shape} (expected: 30)")
print(f"   stocks: shape {stocks.shape} (expected: 30)")
print(f"   tech: shape {tech.shape} (expected: 240 = 30*8)")

# Build final state
scale = np.array(2**-6, dtype=np.float32)
state = np.hstack((
    turbulence_bool,
    price * scale,
    stocks * scale,
    tech * 2**-7,
)).astype(np.float32)

print(f"\n📐 Final State Vector:")
print(f"   Shape: {state.shape}")
print(f"   Expected: (301,)")
print(f"   Status: {'✅ CORRECT!' if state.shape[0] == 301 else '❌ WRONG!'}")

if state.shape[0] != 301:
    print(f"\n❌ OBSERVATION SHAPE MISMATCH!")
    print(f"   Expected: 301")
    print(f"   Got: {state.shape[0]}")
    print(f"   Difference: {state.shape[0] - 301}")
    
    # Detailed breakdown
    print(f"\n🔍 Breakdown:")
    print(f"   turbulence_bool: 1")
    print(f"   price: {len(price)} (should be 30)")
    print(f"   stocks: {len(stocks)} (should be 30)")
    print(f"   tech: {len(tech)} (should be 240 = 30*8)")
    print(f"   Total: 1 + {len(price)} + {len(stocks)} + {len(tech)} = {1 + len(price) + len(stocks) + len(tech)}")

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "="*80)
print("📋 SUMMARY")
print("="*80)

issues = []

if len(trading_tickers) != 30:
    issues.append(f"Wrong number of trading tickers: {len(trading_tickers)} (expected 30)")

if vixy_ticker.empty:
    issues.append("VIXY missing from DataFrame")

if missing:
    issues.append(f"Missing columns: {missing}")

if state.shape[0] != 301:
    issues.append(f"Wrong observation shape: {state.shape[0]} (expected 301)")

if issues:
    print(f"\n❌ ISSUES FOUND ({len(issues)}):")
    for i, issue in enumerate(issues, 1):
        print(f"   {i}. {issue}")
else:
    print(f"\n✅ ALL CHECKS PASSED!")
    print(f"   Data is correctly formatted for FinRL")
    print(f"   Observation shape: {state.shape[0]} ✓")

print("\n" + "="*80 + "\n")
