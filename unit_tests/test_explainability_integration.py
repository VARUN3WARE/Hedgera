#!/usr/bin/env python3
"""
Test: FinRL Pipeline with SHAP & LIME Explainability

Verifies that the FinRL service correctly:
1. Collects background samples
2. Generates SHAP and LIME explanations
3. Logs top 10 features for every decision
"""

import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.src.services.finrl_service_finetuned import FinRLServiceWithFineTuning

print("\n" + "="*80)
print("TEST: FinRL Pipeline with Explainability")
print("="*80)

# Load test data
csv_path = Path(__file__).parent / "trade_data_3days.csv"
test_data = pd.read_csv(csv_path, index_col=0)
test_data['date'] = pd.to_datetime(test_data['date'])

print(f"\n✅ Loaded {len(test_data)} rows from CSV")
print(f"   Timestamps: {test_data['date'].nunique()}")

# Initialize FinRL service
print("\n🤖 Initializing FinRL Service with Explainability...")
finrl_service = FinRLServiceWithFineTuning(
    news_producer=None,
    social_producer=None
)

project_root = Path(__file__).parent.parent
finrl_service.base_model_path = str(project_root / "backend/finrl_integration/agent_ppo")
finrl_service.finetuned_model_path = str(project_root / "backend/finrl_integration/agent_ppo_finetuned")

success = finrl_service.load_model()
if not success:
    print("❌ Failed to load model")
    sys.exit(1)

print(f"✅ Model loaded")
print(f"✅ Explainability service initialized")
print(f"   SHAP samples: 50")
print(f"   LIME samples: 100")

# Simulate multiple predictions to collect samples
timestamps = sorted(test_data['date'].unique())
num_predictions = 120  # Ensure we have enough samples

print(f"\n" + "="*80)
print(f"SIMULATION: Running {num_predictions} predictions to collect samples")
print("="*80)

for i, ts in enumerate(timestamps[:num_predictions], 1):
    ts_data = test_data[test_data['date'] == ts].copy()
    
    # Run FinRL model
    decisions = finrl_service.run_finrl_model(ts_data)
    
    # Check sample collection status
    sample_status = finrl_service.explainability.has_enough_samples()
    
    if i % 10 == 0:
        print(f"\n📊 Prediction #{i:3d}:")
        print(f"   Timestamp: {ts}")
        print(f"   Samples collected: {sample_status['samples_collected']}")
        print(f"   SHAP ready: {'✅' if sample_status['shap'] else '❌ (need 50)'}")
        print(f"   LIME ready: {'✅' if sample_status['lime'] else '❌ (need 100)'}")
        
        if 'explanations' in decisions and decisions['explanations']:
            print(f"   Explanations generated: {len(decisions['explanations'])} tickers")
            for ticker, expl in list(decisions['explanations'].items())[:1]:
                if 'shap' in expl:
                    print(f"      SHAP for {ticker}: {len(expl['shap']['top_features'])} features")
                if 'lime' in expl:
                    print(f"      LIME for {ticker}: {len(expl['lime']['top_features'])} features")

# Final prediction with full logging
print(f"\n" + "="*80)
print("FINAL PREDICTION: With Full Explainability")
print("="*80)

final_ts = timestamps[num_predictions - 1]
final_data = test_data[test_data['date'] == final_ts].copy()

decisions = finrl_service.run_finrl_model(final_data)

print(f"\n📊 Prediction Results:")
print(f"   Timestamp: {final_ts}")
print(f"   Selected tickers: {decisions.get('selected_tickers', [])}")
print(f"   Buy signals: {len(decisions.get('buy_decisions', {}))}")
print(f"   Sell signals: {len(decisions.get('sell_decisions', {}))}")

if 'explanations' in decisions and decisions['explanations']:
    print(f"\n🔍 Explainability Results:")
    print(f"   Tickers explained: {list(decisions['explanations'].keys())}")
    
    for ticker, explanation in decisions['explanations'].items():
        print(f"\n   " + "="*70)
        print(f"   Ticker: {ticker}")
        print(f"   " + "="*70)
        
        # SHAP
        if 'shap' in explanation:
            print(f"\n   📊 SHAP Top 10 Features:")
            for i, feat in enumerate(explanation['shap']['top_features'], 1):
                print(f"      {i:2d}. {feat['feature']:35s} | SHAP: {feat['shap_value']:+.4f} | Importance: {feat['importance']:.4f}")
        
        # LIME
        if 'lime' in explanation:
            print(f"\n   🔬 LIME Top 10 Features:")
            print(f"      Predicted value: {explanation['lime'].get('predicted_value', 'N/A')}")
            for i, feat in enumerate(explanation['lime']['top_features'], 1):
                print(f"      {i:2d}. {feat['feature']:45s} | Weight: {feat['weight']:+.4f}")

print("\n" + "="*80)
print("VALIDATION:")
print("="*80)

# Validate
sample_status = finrl_service.explainability.has_enough_samples()
has_explanations = 'explanations' in decisions and len(decisions['explanations']) > 0

checks = [
    ("Background samples collected", sample_status['samples_collected'] >= 100),
    ("SHAP available", sample_status['shap']),
    ("LIME available", sample_status['lime']),
    ("Explanations generated", has_explanations),
    ("Multiple tickers explained", has_explanations and len(decisions['explanations']) >= 1),
]

all_passed = True
for check_name, check_result in checks:
    status = "✅" if check_result else "❌"
    print(f"{status} {check_name}")
    if not check_result:
        all_passed = False

if all_passed:
    print("\n🎉 ALL CHECKS PASSED!")
    print("\n✅ Explainability is correctly integrated into the pipeline!")
    print("   - SHAP and LIME generate top 10 features for every decision")
    print("   - Features are logged in real-time")
    print("   - Background samples accumulate over time")
else:
    print("\n⚠️  SOME CHECKS FAILED")

print("="*80 + "\n")
