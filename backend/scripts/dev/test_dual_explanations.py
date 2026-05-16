#!/usr/bin/env python3
"""
Quick test for dual explanation system (local + global)
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

import numpy as np
from datetime import datetime
from stable_baselines3 import PPO
from src.services.explainability_service import ExplainabilityService

def test_dual_explanations():
    """Test both local and global explanations"""
    print("\n=== Testing Dual Explanation System ===\n")
    
    # Load model
    model_path = "backend/finrl_integration/agent_ppo.zip"
    if not os.path.exists(model_path):
        print(f"❌ Model not found at {model_path}")
        return False
    
    print(f"✅ Loading model from {model_path}")
    model = PPO.load(model_path)
    
    # Auto-load pre-trained explainers
    explainer_dir = "backend/finrl_integration/explainers"
    
    # DOW 30 tickers
    ticker_list = [
        'AAPL', 'AMGN', 'AMZN', 'AXP', 'BA', 'CAT', 'CRM', 'CSCO', 'CVX', 'DIS',
        'DOW', 'GS', 'HD', 'HON', 'IBM', 'INTC', 'JNJ', 'JPM', 'KO', 'MCD',
        'MMM', 'MRK', 'MSFT', 'NKE', 'NVDA', 'PG', 'UNH', 'V', 'VZ', 'WMT'
    ]
    
    # Technical indicators
    tech_indicators = [
        'macd', 'boll_ub', 'boll_lb', 'rsi_30', 'cci_30', 'dx_30',
        'close_30_sma', 'close_60_sma'
    ]
    
    explainer_service = ExplainabilityService(
        ticker_list=ticker_list,
        tech_indicators=tech_indicators
    )
    
    print(f"✅ Loading pre-trained explainers from {explainer_dir}")
    loaded = explainer_service.load_explainers(explainer_dir)
    
    if not loaded:
        print("❌ Failed to load pre-trained explainers")
        return False
    
    print(f"✅ Loaded {len(explainer_service.background_states)} background samples")
    
    # Create test state (301-dim for 30 DOW stocks + 1 cash)
    state = np.random.randn(301)
    
    # Test 1: Local explanations (top 3 per ticker)
    print("\n--- Test 1: Local Explanations (Top 3) ---")
    test_tickers = [0, 10, 20]  # Test 3 different tickers
    
    for ticker_idx in test_tickers:
        print(f"\n  Ticker {ticker_idx}:")
        local_exp = explainer_service.explain_prediction(
            model=model,
            state=state,
            ticker_idx=ticker_idx,
            top_k=3  # Local: top 3
        )
        
        if local_exp:
            # Extract top features from SHAP and LIME results
            if 'shap' in local_exp:
                shap_features = local_exp['shap']['top_features'][:3]
                print(f"    SHAP top 3: {[(f['feature'], f['importance']) for f in shap_features]}")
            
            if 'lime' in local_exp:
                lime_features = local_exp['lime']['top_features'][:3]
                print(f"    LIME top 3: {[(f['feature'], f['importance']) for f in lime_features]}")
        else:
            print(f"    ❌ Failed to generate local explanation")
            return False
    
    print("\n✅ Local explanations working (top 3 per ticker)")
    
    # Test 2: Global explanations (top 10 across all tickers)
    print("\n--- Test 2: Global Explanations (Top 10) ---")
    global_exp = explainer_service.compute_global_importance(
        model=model,
        state=state,
        top_k=10  # Global: top 10
    )
    
    if global_exp:
        print("\n  Top 10 global features:")
        for feature_dict in global_exp['top_features'][:10]:
            rank = feature_dict['rank']
            feature = feature_dict['feature']
            importance = feature_dict['global_importance']
            print(f"    {rank}. {feature}: {importance:.4f}")
        print("\n✅ Global explanations working (top 10 overall)")
    else:
        print("    ❌ Failed to generate global explanation")
        return False
    
    # Test 3: Parallel execution
    print("\n--- Test 3: Parallel Local Explanations ---")
    start = datetime.now()
    parallel_exp = explainer_service.explain_multiple_tickers_parallel(
        model=model,
        state=state,
        ticker_indices=test_tickers,
        top_k=3,
        max_workers=3
    )
    elapsed = (datetime.now() - start).total_seconds()
    
    if parallel_exp and len(parallel_exp) == 3:
        print(f"✅ Parallel execution: {len(parallel_exp)} tickers in {elapsed:.3f}s")
        print(f"  Average: {elapsed/len(parallel_exp):.3f}s per ticker")
    else:
        print("❌ Parallel execution failed")
        return False
    
    # Summary
    print("\n" + "="*50)
    print("✅ ALL TESTS PASSED")
    print("="*50)
    print("\nDual Explanation System:")
    print("  • LOCAL: Top 3 features per ticker (SHAP + LIME)")
    print("  • GLOBAL: Top 10 features across all tickers (SHAP)")
    print("  • Parallel: 3 workers for speed")
    print(f"  • Performance: {elapsed:.3f}s for 3 tickers")
    print()
    
    return True

if __name__ == "__main__":
    success = test_dual_explanations()
    sys.exit(0 if success else 1)
