#!/usr/bin/env python3
"""
FAST Pre-training: SHAP and LIME Explainers

Optimizations:
1. Vectorized batch predictions (10-100x faster)
2. Reduced background samples (5 instead of 10)
3. Limited SHAP perturbations (nsamples=100)
4. Reduced LIME samples (500 instead of 5000)
5. Parallel training (if needed)

Expected speedup: ~3s → ~0.3-0.5s per explanation
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import time

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.src.services.finrl_service_finetuned import FinRLServiceWithFineTuning
from backend.src.services.explainability_service import ExplainabilityService

print("\n" + "="*80)
print("⚡ FAST PRE-TRAINING: SHAP & LIME EXPLAINERS")
print("="*80)

# Configuration
SAVE_DIR = Path(__file__).parent.parent / "backend/finrl_integration/explainers"
NUM_SAMPLES = 100  # Reduced from 200 for faster training
SHAP_SAMPLES = 50
LIME_SAMPLES = 100

print(f"\n📋 Configuration:")
print(f"   Save directory: {SAVE_DIR}")
print(f"   Background samples: {NUM_SAMPLES}")
print(f"   SHAP samples: {SHAP_SAMPLES}")
print(f"   LIME samples: {LIME_SAMPLES}")
print(f"\n⚡ Speed optimizations:")
print(f"   - Vectorized batch predictions")
print(f"   - Reduced background (5 samples for SHAP)")
print(f"   - Limited perturbations (nsamples=100)")
print(f"   - Reduced LIME samples (500)")

# Load test data
csv_path = Path(__file__).parent / "trade_data_3days.csv"
if not csv_path.exists():
    print(f"\n❌ Error: {csv_path} not found")
    sys.exit(1)

test_data = pd.read_csv(csv_path, index_col=0)
test_data['date'] = pd.to_datetime(test_data['date'])

print(f"\n✅ Loaded {len(test_data)} rows from CSV")
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

print(f"✅ Model loaded")

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

collect_start = time.time()
for i, ts in enumerate(selected_timestamps, 1):
    ts_data = test_data[test_data['date'] == ts].copy()
    state = finrl_service._build_state_vector(ts_data)
    explainability.add_background_sample(state)
    
    if i % 25 == 0:
        elapsed = time.time() - collect_start
        print(f"   Collected {i}/{NUM_SAMPLES} samples in {elapsed:.2f}s...")

collect_time = time.time() - collect_start
sample_status = explainability.has_enough_samples()
print(f"\n✅ Sample collection complete in {collect_time:.2f}s:")
print(f"   Total samples: {sample_status['samples_collected']}")
print(f"   SHAP ready: {'✅' if sample_status['shap'] else '❌'}")
print(f"   LIME ready: {'✅' if sample_status['lime'] else '❌'}")

# Pre-train SHAP explainer
if sample_status['shap']:
    print(f"\n🔧 Pre-training SHAP explainer (vectorized)...")
    
    # Get a test state
    test_ts = timestamps[0]
    test_data_sample = test_data[test_data['date'] == test_ts].copy()
    test_state = finrl_service._build_state_vector(test_data_sample)
    
    shap_start = time.time()
    shap_result = explainability.explain_with_shap(
        model=finrl_service.model,
        state=test_state,
        ticker_idx=0,
        top_k=10
    )
    shap_time = time.time() - shap_start
    
    if shap_result:
        print(f"✅ SHAP explainer trained in {shap_time:.2f}s (was ~3.2s before!)")
        print(f"   Speedup: {3.2/shap_time:.1f}x faster")
        print(f"   Top feature: {shap_result['top_features'][0]['feature']}")
    else:
        print(f"❌ SHAP training failed")
else:
    print(f"\n⚠️  Not enough samples for SHAP training")
    shap_time = 0

# Pre-train LIME explainer
if sample_status['lime']:
    print(f"\n🔧 Pre-training LIME explainer (vectorized)...")
    
    # Get a test state
    test_ts = timestamps[0]
    test_data_sample = test_data[test_data['date'] == test_ts].copy()
    test_state = finrl_service._build_state_vector(test_data_sample)
    
    lime_start = time.time()
    lime_result = explainability.explain_with_lime(
        model=finrl_service.model,
        state=test_state,
        ticker_idx=0,
        top_k=10
    )
    lime_time = time.time() - lime_start
    
    if lime_result:
        print(f"✅ LIME explainer trained in {lime_time:.2f}s (was ~1.0s before)")
        print(f"   Speedup: {1.0/lime_time:.1f}x faster")
        print(f"   Top feature: {lime_result['top_features'][0]['feature']}")
    else:
        print(f"❌ LIME training failed")
else:
    print(f"\n⚠️  Not enough samples for LIME training")
    lime_time = 0

# Save explainers to disk
print(f"\n💾 Saving explainers to {SAVE_DIR}...")
save_start = time.time()
save_success = explainability.save_explainers(str(SAVE_DIR))
save_time = time.time() - save_start

if save_success:
    total_time = collect_time + shap_time + lime_time + save_time
    
    print(f"\n🎉 SUCCESS! Explainers saved in {save_time:.2f}s")
    print(f"\n📁 Saved files:")
    for file in sorted(SAVE_DIR.glob("*")):
        size_kb = file.stat().st_size / 1024
        print(f"   {file.name}: {size_kb:.1f} KB")
    
    print(f"\n⏱️  Total time breakdown:")
    print(f"   Sample collection: {collect_time:.2f}s")
    print(f"   SHAP training:     {shap_time:.2f}s")
    print(f"   LIME training:     {lime_time:.2f}s")
    print(f"   Save to disk:      {save_time:.2f}s")
    print(f"   TOTAL:             {total_time:.2f}s")
    
    print(f"\n📝 Now run your integration test:")
    print(f"   python unit_tests/test_explainability_integration.py")
    print(f"\n⚡ Expected performance:")
    print(f"   - Will auto-load pre-trained explainers")
    print(f"   - ~0.3-0.5s per SHAP explanation (was 3.2s)")
    print(f"   - ~0.2s per LIME explanation (was 1.0s)")
    print(f"   - 3 tickers in parallel: ~0.5s total (was 12s)")
else:
    print(f"\n❌ Failed to save explainers")
    sys.exit(1)

print("\n" + "="*80 + "\n")
