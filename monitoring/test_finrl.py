#!/usr/bin/env python3
"""
Test all FinRL imports to identify missing dependencies and import errors.
This will help us fix all issues before running the main pipeline.
"""

import sys
from pathlib import Path

# Add paths
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent / "backend" / "finrl_integration"))

print("=" * 80)
print("🧪 TESTING FINRL IMPORTS")
print("=" * 80)
print()

# Test 1: Basic imports
print("✓ Test 1: Basic Python imports")
try:
    import json
    import numpy as np
    import pandas as pd
    from typing import Dict, Any
    from datetime import datetime
    print("  ✅ Basic imports successful")
except Exception as e:
    print(f"  ❌ Basic imports failed: {e}")
    sys.exit(1)

# Test 2: FinRL dependencies
print("\n✓ Test 2: FinRL dependencies")
deps_ok = True
try:
    import alpaca_trade_api
    print("  ✅ alpaca_trade_api")
except Exception as e:
    print(f"  ❌ alpaca_trade_api: {e}")
    deps_ok = False

try:
    import pandas_market_calendars
    print("  ✅ pandas_market_calendars")
except Exception as e:
    print(f"  ❌ pandas_market_calendars: {e}")
    deps_ok = False

try:
    import stockstats
    print("  ✅ stockstats")
except Exception as e:
    print(f"  ❌ stockstats: {e}")
    deps_ok = False

try:
    import gym
    print("  ✅ gym")
except Exception as e:
    print(f"  ❌ gym: {e}")
    deps_ok = False

try:
    import stable_baselines3
    print("  ✅ stable_baselines3")
except Exception as e:
    print(f"  ❌ stable_baselines3: {e}")
    deps_ok = False

try:
    import torch
    print("  ✅ torch")
except Exception as e:
    print(f"  ❌ torch: {e}")
    deps_ok = False

if not deps_ok:
    print("\n⚠️  Some dependencies missing - install them first!")
    sys.exit(1)

# Test 3: FinRL internal modules
print("\n✓ Test 3: FinRL internal modules")

# Check what files exist
finrl_path = Path(__file__).parent / "backend" / "finrl_integration" / "finrl"
print(f"\n  📁 Checking: {finrl_path}")

meta_path = finrl_path / "meta"
if meta_path.exists():
    print(f"  ✅ meta/ directory exists")
    
    # Check data_processors
    dp_path = meta_path / "data_processors"
    if dp_path.exists():
        print(f"  ✅ data_processors/ directory exists")
        print(f"     Files in data_processors/:")
        for f in dp_path.iterdir():
            if f.is_file():
                print(f"       - {f.name}")
    
    # Check paper_trading
    pt_path = meta_path / "paper_trading"
    if pt_path.exists():
        print(f"  ✅ paper_trading/ directory exists")
        print(f"     Files in paper_trading/:")
        for f in pt_path.iterdir():
            if f.is_file():
                print(f"       - {f.name}")

# Test 4: Try importing FinRL modules
print("\n✓ Test 4: Import FinRL processor")
try:
    from finrl.meta.data_processors.processor_alpaca import AlpacaProcessor
    print("  ✅ AlpacaProcessor imported successfully")
except Exception as e:
    print(f"  ❌ AlpacaProcessor import failed: {e}")
    import traceback
    traceback.print_exc()

# Test 5: Try importing paper trading (this is where it fails)
print("\n✓ Test 5: Import paper trading modules")
try:
    print("  → Trying: from finrl.meta.paper_trading.alpaca import PaperTradingAlpaca")
    from finrl.meta.paper_trading.alpaca import PaperTradingAlpaca
    print("  ✅ PaperTradingAlpaca imported successfully")
except Exception as e:
    print(f"  ❌ PaperTradingAlpaca import failed: {e}")
    import traceback
    traceback.print_exc()
    print("\n  🔍 Analyzing the error...")
    
    # Check what common.py is trying to import
    common_file = Path(__file__).parent / "backend" / "finrl_integration" / "finrl" / "meta" / "paper_trading" / "common.py"
    if common_file.exists():
        print(f"\n  📄 Checking imports in common.py...")
        with open(common_file, 'r') as f:
            lines = f.readlines()
            for i, line in enumerate(lines[685:695], start=686):
                if 'import' in line and 'DataProcessor' in line:
                    print(f"     Line {i}: {line.strip()}")

# Test 6: Try importing our wrapper
print("\n✓ Test 6: Import our paper_trading wrapper")
try:
    from paper_trading import PaperTradingJSON, get_trading_decisions
    print("  ✅ paper_trading wrapper imported successfully")
except Exception as e:
    print(f"  ❌ paper_trading wrapper import failed: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 80)
print("📊 TEST SUMMARY")
print("=" * 80)
print("\nNext steps:")
print("1. Fix any missing dependencies")
print("2. Fix import paths in common.py")
print("3. Re-run this test until all pass")
print("4. Then run the main pipeline")
print()
