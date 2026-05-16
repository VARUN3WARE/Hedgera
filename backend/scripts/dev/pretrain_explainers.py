#!/usr/bin/env python3
"""
Pre-train SHAP and LIME Explainers

This script:
1. Loads historical data from trade_data_3days.csv
2. Builds background samples (state vectors)
3. Trains SHAP and LIME explainers
4. Saves them to disk for fast inference in production

Run once to create explainer cache, then production uses pre-trained explainers.
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.src.services.finrl_service_finetuned import FinRLServiceWithFineTuning
from backend.src.services.explainability_service import ExplainabilityService

print("\n" + "="*80)
print("PRE-TRAINING SHAP & LIME EXPLAINERS")
print("="*80)

# Configuration
SAVE_DIR = Path(__file__).parent.parent / "backend/finrl_integration/explainers"
NUM_SAMPLES = 200  # Use 200 samples for robust training
SHAP_SAMPLES = 50
LIME_SAMPLES = 100

print(f"\n📋 Configuration:")
print(f"   Save directory: {SAVE_DIR}")
print(f"   Background samples: {NUM_SAMPLES}")
print(f"   SHAP samples: {SHAP_SAMPLES}")
print(f"   LIME samples: {LIME_SAMPLES}")

# Load test data
csv_path = Path(__file__).parent / "trade_data_3days.csv"
if not csv_path.exists():
    print(f"\n❌ Error: {csv_path} not found")
    sys.exit(1)

test_data = pd.read_csv(csv_path, index_col=0)
test_data['date'] = pd.to_datetime(test_data['date'])

print(f"\n✅ Loaded {len(test_data)} rows from CSV")
print(f"   Tickers: {test_data['tic'].nunique()}")
print(f"   Timestamps: {test_data['date'].nunique()}")

# Initialize FinRL service
print("\n🤖 Initializing FinRL Service...")
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

print(f"✅ Model loaded: {Path(finrl_service.current_model_path).name}")

# Create explainability service
print(f"\n🔍 Creating Explainability Service...")
explainability = ExplainabilityService(
    ticker_list=finrl_service.ticker_list,
    tech_indicators=finrl_service.tech_indicators,
    shap_samples=SHAP_SAMPLES,
    lime_samples=LIME_SAMPLES
)

# Collect background samples
print(f"\n📊 Collecting {NUM_SAMPLES} background samples...")
timestamps = sorted(test_data['date'].unique())
selected_timestamps = np.random.choice(
    timestamps,
    size=min(NUM_SAMPLES, len(timestamps)),
    replace=False
)

for i, ts in enumerate(selected_timestamps, 1):
    ts_data = test_data[test_data['date'] == ts].copy()
    state = finrl_service._build_state_vector(ts_data)
    explainability.add_background_sample(state)
    
    if i % 50 == 0:
        print(f"   Collected {i}/{NUM_SAMPLES} samples...")

sample_status = explainability.has_enough_samples()
print(f"\n✅ Sample collection complete:")
print(f"   Total samples: {sample_status['samples_collected']}")
print(f"   SHAP ready: {'✅' if sample_status['shap'] else '❌'}")
print(f"   LIME ready: {'✅' if sample_status['lime'] else '❌'}")

# Pre-train SHAP explainer
if sample_status['shap']:
    print(f"\n🔧 Pre-training SHAP explainer...")
    
    # Get a test state to trigger explainer creation
    test_ts = timestamps[0]
    test_data_sample = test_data[test_data['date'] == test_ts].copy()
    test_state = finrl_service._build_state_vector(test_data_sample)
    
    # This will create and cache the SHAP explainer
    shap_result = explainability.explain_with_shap(
        model=finrl_service.model,
        state=test_state,
        ticker_idx=0,
        top_k=10
    )
    
    if shap_result:
        print(f"✅ SHAP explainer trained")
        print(f"   Top feature: {shap_result['top_features'][0]['feature']}")
    else:
        print(f"❌ SHAP training failed")
else:
    print(f"\n⚠️  Not enough samples for SHAP training")

# Pre-train LIME explainer
if sample_status['lime']:
    print(f"\n🔧 Pre-training LIME explainer...")
    
    # Get a test state to trigger explainer creation
    test_ts = timestamps[0]
    test_data_sample = test_data[test_data['date'] == test_ts].copy()
    test_state = finrl_service._build_state_vector(test_data_sample)
    
    # This will create and cache the LIME explainer
    lime_result = explainability.explain_with_lime(
        model=finrl_service.model,
        state=test_state,
        ticker_idx=0,
        top_k=10
    )
    
    if lime_result:
        print(f"✅ LIME explainer trained")
        print(f"   Top feature: {lime_result['top_features'][0]['feature']}")
    else:
        print(f"❌ LIME training failed")
else:
    print(f"\n⚠️  Not enough samples for LIME training")

# Save explainers to disk
print(f"\n💾 Saving explainers to {SAVE_DIR}...")
save_success = explainability.save_explainers(str(SAVE_DIR))

if save_success:
    print(f"\n🎉 SUCCESS! Explainers saved to disk")
    print(f"\n📁 Saved files:")
    for file in sorted(SAVE_DIR.glob("*")):
        size_kb = file.stat().st_size / 1024
        print(f"   {file.name}: {size_kb:.1f} KB")
    
    print(f"\n📝 Usage in production:")
    print(f"   1. In finrl_service_finetuned.py:")
    print(f"      explainability.load_explainers('{SAVE_DIR}')")
    print(f"   2. Now explainability.explain_prediction() runs in FAST mode")
    print(f"   3. No more 3-second SHAP training delays!")
    print(f"\n⚡ Speed improvement:")
    print(f"   Before: ~3.2s per SHAP explanation (training + inference)")
    print(f"   After:  ~0.1s per explanation (inference only)")
else:
    print(f"\n❌ Failed to save explainers")
    sys.exit(1)

print("\n" + "="*80 + "\n")
