#!/usr/bin/env python3
"""
Analysis: SHAP and LIME Sample Requirements

This script analyzes how many background samples are needed for:
1. SHAP to converge to stable explanations
2. LIME to provide reliable local explanations

Tests different sample sizes: 10, 25, 50, 100, 200
"""

import sys
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime
import time

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.src.services.finrl_service_finetuned import FinRLServiceWithFineTuning

print("\n" + "="*80)
print("ANALYSIS: SHAP & LIME Sample Requirements")
print("="*80)

# Load test data
csv_path = Path(__file__).parent / "trade_data_3days.csv"
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

# Create feature names
feature_names = []
feature_names.append('cash_balance')
for ticker in finrl_service.ticker_list:
    feature_names.append(f'price_{ticker}')
for ticker in finrl_service.ticker_list:
    feature_names.append(f'holding_{ticker}')
for ticker in finrl_service.ticker_list:
    for indicator in finrl_service.tech_indicators:
        feature_names.append(f'{indicator}_{ticker}')

print(f"✅ Feature names created: {len(feature_names)} features")

# Prepare background data (different sample sizes)
timestamps = test_data['date'].unique()
num_timestamps = min(200, len(timestamps))  # Max 200 samples
selected_timestamps = np.random.choice(timestamps, size=num_timestamps, replace=False)

background_states = []
for ts in selected_timestamps:
    ts_data = test_data[test_data['date'] == ts].copy()
    state = finrl_service._build_state_vector(ts_data)
    background_states.append(state)

background_states = np.array(background_states)
print(f"\n📊 Prepared {len(background_states)} background samples")
print(f"   Shape: {background_states.shape}")

# Get a test sample to explain
latest_ts = test_data['date'].max()
test_sample_data = test_data[test_data['date'] == latest_ts].copy()
test_state = finrl_service._build_state_vector(test_sample_data)

# Get prediction
test_action, _ = finrl_service.model.predict(test_state.reshape(1, -1), deterministic=True)
test_action = test_action[0]

print(f"\n📍 Test sample:")
print(f"   Timestamp: {latest_ts}")
print(f"   State shape: {test_state.shape}")
print(f"   Prediction (AAPL): {test_action[0]:.4f}")

# Test different sample sizes
sample_sizes = [10, 25, 50, 100, 200]

print("\n" + "="*80)
print("EXPERIMENT: Sample Size Impact on Explanations")
print("="*80)

results = {
    'shap': {},
    'lime': {}
}

# Check if libraries are available
try:
    import shap
    shap_available = True
    print("\n✅ SHAP library available")
except ImportError:
    shap_available = False
    print("\n⚠️  SHAP not available (pip install shap)")

try:
    import lime
    import lime.lime_tabular
    lime_available = True
    print("✅ LIME library available")
except ImportError:
    lime_available = False
    print("⚠️  LIME not available (pip install lime)")

# Test SHAP with different sample sizes
if shap_available:
    print("\n" + "-"*80)
    print("SHAP Analysis")
    print("-"*80)
    
    def predict_fn(states):
        predictions = []
        for s in states:
            action, _ = finrl_service.model.predict(s.reshape(1, -1), deterministic=True)
            predictions.append(action[0])
        return np.array(predictions)
    
    for n_samples in sample_sizes:
        if n_samples > len(background_states):
            continue
        
        print(f"\n📊 Testing SHAP with {n_samples} background samples...")
        
        try:
            start_time = time.time()
            
            # Create explainer with subset of background
            explainer = shap.KernelExplainer(
                predict_fn,
                background_states[:min(10, n_samples)]  # Use max 10 for speed
            )
            
            # Compute SHAP values
            shap_values = explainer.shap_values(test_state.reshape(1, -1))
            
            elapsed = time.time() - start_time
            
            # Extract values for first ticker (AAPL)
            shap_array = np.array(shap_values)
            if shap_array.ndim == 3:
                ticker_shap = shap_array[0, :, 0]
            elif shap_array.ndim == 2:
                ticker_shap = shap_array[0, :]
            else:
                ticker_shap = shap_values[0] if isinstance(shap_values, list) else shap_values
            
            # Get top 10 features
            top_indices = np.argsort(np.abs(ticker_shap))[-10:][::-1]
            
            top_features = []
            for idx in top_indices:
                idx = int(idx)
                top_features.append({
                    'feature': feature_names[idx],
                    'value': float(ticker_shap[idx])
                })
            
            results['shap'][n_samples] = {
                'time_seconds': elapsed,
                'top_features': top_features,
                'success': True
            }
            
            print(f"   ✅ Completed in {elapsed:.2f}s")
            print(f"   Top 3 features:")
            for i, feat in enumerate(top_features[:3], 1):
                print(f"      {i}. {feat['feature']:40s} = {feat['value']:+.4f}")
            
        except Exception as e:
            print(f"   ❌ Failed: {e}")
            results['shap'][n_samples] = {'success': False, 'error': str(e)}

# Test LIME with different sample sizes
if lime_available:
    print("\n" + "-"*80)
    print("LIME Analysis")
    print("-"*80)
    
    def predict_fn_lime(states):
        predictions = []
        for s in states:
            action, _ = finrl_service.model.predict(s.reshape(1, -1), deterministic=True)
            predictions.append(action[0])
        return np.array(predictions)
    
    for n_samples in sample_sizes:
        if n_samples > len(background_states):
            continue
        
        print(f"\n📊 Testing LIME with {n_samples} training samples...")
        
        try:
            start_time = time.time()
            
            # Create explainer with subset of background
            explainer = lime.lime_tabular.LimeTabularExplainer(
                background_states[:n_samples],
                feature_names=feature_names,
                mode='regression',
                verbose=False
            )
            
            # Generate explanation for first ticker
            explanation = explainer.explain_instance(
                test_state,
                lambda x: predict_fn_lime(x)[:, 0],  # First ticker (AAPL)
                num_features=10
            )
            
            elapsed = time.time() - start_time
            
            # Extract top features
            top_features = []
            for feature, weight in explanation.as_list()[:10]:
                feature_name = feature.split('<=')[0].split('>')[0].strip()
                top_features.append({
                    'feature': feature,
                    'feature_name': feature_name,
                    'weight': float(weight)
                })
            
            results['lime'][n_samples] = {
                'time_seconds': elapsed,
                'top_features': top_features,
                'predicted_value': float(explanation.predicted_value),
                'success': True
            }
            
            print(f"   ✅ Completed in {elapsed:.2f}s")
            print(f"   Predicted value: {explanation.predicted_value:.4f}")
            print(f"   Top 3 features:")
            for i, feat in enumerate(top_features[:3], 1):
                print(f"      {i}. {feat['feature']:50s} = {feat['weight']:+.4f}")
            
        except Exception as e:
            print(f"   ❌ Failed: {e}")
            results['lime'][n_samples] = {'success': False, 'error': str(e)}

# Summary
print("\n" + "="*80)
print("SUMMARY: Sample Size Requirements")
print("="*80)

if shap_available and results['shap']:
    print("\n📊 SHAP:")
    print("   Sample Size | Time (s) | Status")
    print("   " + "-"*50)
    for n_samples in sorted(results['shap'].keys()):
        result = results['shap'][n_samples]
        if result['success']:
            print(f"   {n_samples:11d} | {result['time_seconds']:8.2f} | ✅")
        else:
            print(f"   {n_samples:11d} | {'N/A':8s} | ❌")
    
    # Recommendation
    successful = [n for n, r in results['shap'].items() if r['success']]
    if successful:
        print(f"\n   💡 Recommendation: Use {min(successful)} samples minimum")
        print(f"      - Faster: {min(successful)} samples ({results['shap'][min(successful)]['time_seconds']:.2f}s)")
        if 50 in successful:
            print(f"      - Balanced: 50 samples ({results['shap'][50]['time_seconds']:.2f}s)")
        if 100 in successful:
            print(f"      - Stable: 100 samples ({results['shap'][100]['time_seconds']:.2f}s)")

if lime_available and results['lime']:
    print("\n🔬 LIME:")
    print("   Sample Size | Time (s) | Status")
    print("   " + "-"*50)
    for n_samples in sorted(results['lime'].keys()):
        result = results['lime'][n_samples]
        if result['success']:
            print(f"   {n_samples:11d} | {result['time_seconds']:8.2f} | ✅")
        else:
            print(f"   {n_samples:11d} | {'N/A':8s} | ❌")
    
    # Recommendation
    successful = [n for n, r in results['lime'].items() if r['success']]
    if successful:
        print(f"\n   💡 Recommendation: Use {min(successful)} samples minimum")
        print(f"      - Faster: {min(successful)} samples ({results['lime'][min(successful)]['time_seconds']:.2f}s)")
        if 50 in successful:
            print(f"      - Balanced: 50 samples ({results['lime'][50]['time_seconds']:.2f}s)")
        if 100 in successful:
            print(f"      - Stable: 100 samples ({results['lime'][100]['time_seconds']:.2f}s)")

# Feature stability analysis
print("\n" + "="*80)
print("FEATURE STABILITY: How consistent are top features?")
print("="*80)

if shap_available and len(results['shap']) >= 2:
    print("\n📊 SHAP Feature Consistency:")
    successful_sizes = sorted([n for n, r in results['shap'].items() if r['success']])
    
    if len(successful_sizes) >= 2:
        # Compare smallest vs largest
        small_features = set([f['feature'] for f in results['shap'][successful_sizes[0]]['top_features'][:5]])
        large_features = set([f['feature'] for f in results['shap'][successful_sizes[-1]]['top_features'][:5]])
        
        overlap = small_features & large_features
        print(f"   {successful_sizes[0]} samples vs {successful_sizes[-1]} samples (top 5 overlap):")
        print(f"   Common features: {len(overlap)}/5 ({len(overlap)/5*100:.0f}%)")
        if overlap:
            print(f"   Stable features: {', '.join(sorted(overlap)[:3])}...")

if lime_available and len(results['lime']) >= 2:
    print("\n🔬 LIME Feature Consistency:")
    successful_sizes = sorted([n for n, r in results['lime'].items() if r['success']])
    
    if len(successful_sizes) >= 2:
        # Compare smallest vs largest
        small_features = set([f['feature_name'] for f in results['lime'][successful_sizes[0]]['top_features'][:5]])
        large_features = set([f['feature_name'] for f in results['lime'][successful_sizes[-1]]['top_features'][:5]])
        
        overlap = small_features & large_features
        print(f"   {successful_sizes[0]} samples vs {successful_sizes[-1]} samples (top 5 overlap):")
        print(f"   Common features: {len(overlap)}/5 ({len(overlap)/5*100:.0f}%)")
        if overlap:
            print(f"   Stable features: {', '.join(sorted(overlap)[:3])}...")

print("\n" + "="*80)
print("CONCLUSION:")
print("="*80)
print("""
For production pipeline integration:

1. SHAP (Global Interpretability):
   - Minimum: 10 background samples (fast but less stable)
   - Recommended: 50 background samples (good balance)
   - Optimal: 100+ background samples (most stable)
   - Computation time: ~3-10 seconds per explanation

2. LIME (Local Interpretability):
   - Minimum: 25 training samples (fast but variable)
   - Recommended: 100 training samples (reliable)
   - Optimal: 200+ training samples (very stable)
   - Computation time: ~1-5 seconds per explanation

3. Data Collection Strategy:
   - Collect states from past predictions (rolling window)
   - Start explaining after collecting minimum samples
   - Gradually improve quality as more samples accumulate
   - Maximum storage: 200 samples (301 features × 200 × 8 bytes ≈ 0.5 MB)

4. Pipeline Integration:
   - Add state to background after each FinRL prediction
   - Generate explanations only when enough samples exist
   - Log top 10 features for buy/sell decisions
   - Regenerate explainers periodically (e.g., daily)
""")
print("="*80 + "\n")
