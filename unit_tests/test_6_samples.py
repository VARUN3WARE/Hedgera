"""
Quick Test: 6 Random Sample Predictions

This demonstrates backend testing. For COMPREHENSIVE backend testing, see test_finrl_base_model.py
which properly tests:
- backend/src/services/finrl_service_finetuned.FinRLServiceWithFineTuning
- _build_state_vector() method (301-dim state construction)  
- run_finrl_model() method (full prediction pipeline)
- model.predict() integration
- Edge cases (subset tickers, single ticker, full 30 tickers)

This file provides a simpler demo of 6 random predictions using the SAME backend methods.
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np

# Add backend to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import ACTUAL backend implementation (NOT reimplementing functions!)
from backend.src.services.finrl_service_finetuned import FinRLServiceWithFineTuning

# Load test data
csv_path = Path(__file__).parent / "trade_data_3days.csv"
test_data = pd.read_csv(csv_path)
test_data['date'] = pd.to_datetime(test_data['date'])

# Initialize backend service with absolute paths (required for standalone testing)
finrl_service = FinRLServiceWithFineTuning(news_producer=None, social_producer=None)
finrl_service.base_model_path = str(project_root / "backend/finrl_integration/agent_ppo")
finrl_service.finetuned_model_path = str(project_root / "backend/finrl_integration/agent_ppo_finetuned")

# Load model using BACKEND METHOD
success = finrl_service.load_model()
if not success:
    print("❌ Failed to load model")
    sys.exit(1)

model_name = Path(finrl_service.current_model_path).name
print(f"\n✅ Testing Backend Implementation")
print(f"   Class: {finrl_service.__class__.__module__}.{finrl_service.__class__.__name__}")
print(f"   Model: {model_name}")

# Get 6 random timestamps
unique_timestamps = sorted(test_data['date'].unique())
random_indices = np.random.choice(len(unique_timestamps), size=6, replace=False)
random_timestamps = [unique_timestamps[i] for i in sorted(random_indices)]

print("\n" + "="*80)
print("6 Random Sample Predictions from Backend")
print("="*80)

for i, timestamp in enumerate(random_timestamps, 1):
    market_data = test_data[test_data['date'] == timestamp].copy()
    
    # ✅ TESTING BACKEND METHOD: run_finrl_model()
    # This internally calls:
    #   - _build_state_vector() to construct 301-dim state
    #   - model.predict() to get actions  
    #   - Decision formatting logic
    decisions = finrl_service.run_finrl_model(market_data)
    
    selected = decisions.get('selected_tickers', [])
    buys = decisions.get('buy_decisions', {})
    sells = decisions.get('sell_decisions', {})
    
    print(f"\nSample {i}: {timestamp}")
    print(f"  Selected: {len(selected)} tickers")
    if selected:
        print(f"  Tickers: {', '.join(selected)}")
        if buys:
            print(f"  BUY: {buys}")
        if sells:
            print(f"  SELL: {sells}")
    else:
        print(f"  All HOLD")

print("\n" + "="*80)
print("Backend Methods Tested:")
print("  ✅ FinRLServiceWithFineTuning.load_model()")
print("  ✅ FinRLServiceWithFineTuning.run_finrl_model()")  
print("  ✅ FinRLServiceWithFineTuning._build_state_vector() (internal)")
print("\n📋 For comprehensive backend testing with 6 test cases, run:")
print("   python test_finrl_base_model.py")
print("="*80)
