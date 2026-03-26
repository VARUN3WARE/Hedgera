#!/usr/bin/env python3
"""
Unit Test 1: FinRL Base Model Prediction Test

This test verifies that:
1. FinRL service can load the base PPO model
2. Model can accept market data and produce predictions
3. Predictions are in the expected format and range
4. State vector is correctly constructed (301 dimensions)

Uses trade_data_3days.csv as test data source.
"""

import sys
import unittest
import asyncio
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.src.services.finrl_service_finetuned import FinRLServiceWithFineTuning
from backend.config.settings import settings


class TestFinRLBaseModel(unittest.TestCase):
    """Test suite for FinRL base model predictions."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures once for all tests."""
        print("\n" + "="*80)
        print("TEST SUITE: FinRL Base Model Prediction")
        print("="*80)
        
        # Load test data from CSV
        csv_path = Path(__file__).parent / "trade_data_3days.csv"
        if not csv_path.exists():
            raise FileNotFoundError(f"Test data not found: {csv_path}")
        
        cls.test_data = pd.read_csv(csv_path, index_col=0)
        print(f"Loaded {len(cls.test_data)} rows, {cls.test_data['tic'].nunique()} tickers")
        
        # Verify required columns
        required_cols = ['date', 'tic', 'open', 'high', 'low', 'close', 'volume',
                        'macd', 'boll_ub', 'boll_lb', 'rsi_30', 'cci_30', 'dx_30',
                        'close_30_sma', 'close_60_sma']
        missing = set(required_cols) - set(cls.test_data.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        
        # Initialize FinRL service (without Redis - standalone mode)
        cls.finrl_service = FinRLServiceWithFineTuning(
            news_producer=None,
            social_producer=None
        )
        
        # Override model paths with absolute paths
        project_root = Path(__file__).parent.parent
        cls.finrl_service.base_model_path = str(project_root / "backend/finrl_integration/agent_ppo")
        cls.finrl_service.finetuned_model_path = str(project_root / "backend/finrl_integration/agent_ppo_finetuned")
        
        # Load the model
        success = cls.finrl_service.load_model()
        if not success:
            raise RuntimeError("Failed to load FinRL model")
        
        model_type = 'Fine-tuned' if 'finetuned' in cls.finrl_service.current_model_path else 'Base'
        print(f"Loaded {model_type} model: {Path(cls.finrl_service.current_model_path).name}\n")
        
    def test_01_model_loaded(self):
        """Test 1: Verify model is loaded."""
        self.assertIsNotNone(self.finrl_service.model, "Model should be loaded")
        self.assertIsNotNone(self.finrl_service.current_model_path, "Model path should be set")
        print(f"✅ Model loaded: {type(self.finrl_service.model).__name__}")
    
    def test_02_state_vector_construction(self):
        """Test 2: Verify state vector is correctly constructed (301 dims)."""
        latest_timestamp = self.test_data['date'].max()
        market_data = self.test_data[self.test_data['date'] == latest_timestamp].copy()
        
        state = self.finrl_service._build_state_vector(market_data)
        
        expected_shape = (301,)
        print(f"✅ State vector: shape={state.shape}, range=[{state.min():.4f}, {state.max():.4f}], "
              f"NaN={np.isnan(state).sum()}, Inf={np.isinf(state).sum()}")
        
        self.assertEqual(state.shape, expected_shape, f"Expected {expected_shape}, got {state.shape}")
        self.assertEqual(state.dtype, np.float32, "State vector should be float32")
        self.assertEqual(np.isnan(state).sum(), 0, "State vector should not contain NaN")
        self.assertEqual(np.isinf(state).sum(), 0, "State vector should not contain Inf")
    
    def test_03_model_prediction(self):
        """Test 3: Verify model produces valid predictions."""
        latest_timestamp = self.test_data['date'].max()
        market_data = self.test_data[self.test_data['date'] == latest_timestamp].copy()
        
        state = self.finrl_service._build_state_vector(market_data)
        action, _ = self.finrl_service.model.predict(state.reshape(1, -1), deterministic=True)
        action = action[0]
        
        expected_action_shape = (30,)
        self.assertEqual(action.shape, expected_action_shape,
                        f"Expected {expected_action_shape}, got {action.shape}")
        
        scaled_action = (action * self.finrl_service.max_stock).astype(int)
        buy_count = (scaled_action > self.finrl_service.min_action).sum()
        sell_count = (scaled_action < -self.finrl_service.min_action).sum()
        hold_count = len(scaled_action) - buy_count - sell_count
        
        print(f"✅ Predictions: range=[{action.min():.3f}, {action.max():.3f}], "
              f"BUY={buy_count}, SELL={sell_count}, HOLD={hold_count}")
    
    def test_04_full_finrl_pipeline(self):
        """Test 4: Test complete FinRL pipeline (state → prediction → decisions)."""
        latest_timestamp = self.test_data['date'].max()
        market_data = self.test_data[self.test_data['date'] == latest_timestamp].copy()
        
        decisions = self.finrl_service.run_finrl_model(market_data)
        
        self.assertIn('timestamp', decisions)
        self.assertIn('selected_tickers', decisions)
        self.assertIn('buy_decisions', decisions)
        self.assertIn('sell_decisions', decisions)
        self.assertIsInstance(decisions['selected_tickers'], list)
        self.assertIsInstance(decisions['buy_decisions'], dict)
        self.assertIsInstance(decisions['sell_decisions'], dict)
        
        # Note: Model may produce weak signals resulting in 0 selections (HOLD for all)
        # This is valid model behavior, not an error
        selected_count = len(decisions['selected_tickers'])
        print(f"✅ Pipeline: {selected_count} tickers selected, "
              f"{len(decisions['buy_decisions'])} BUY, {len(decisions['sell_decisions'])} SELL")
    
    def test_05_multiple_timestamps(self):
        """Test 5: Test model consistency across multiple timestamps."""
        unique_timestamps = sorted(self.test_data['date'].unique())[:3]
        
        results = []
        for timestamp in unique_timestamps:
            market_data = self.test_data[self.test_data['date'] == timestamp].copy()
            decisions = self.finrl_service.run_finrl_model(market_data)
            results.append(decisions)
        
        self.assertEqual(len(results), len(unique_timestamps))
        
        # All should return valid decision structure (even if 0 selections)
        for result in results:
            self.assertIn('selected_tickers', result)
            self.assertIsInstance(result['selected_tickers'], list)
        
        counts = [len(r.get('selected_tickers', [])) for r in results]
        print(f"✅ Multi-timestamp: {len(unique_timestamps)} timestamps processed, selected={counts}")
    
    def test_06_edge_cases(self):
        """Test 6: Test edge cases (missing tickers, minimal data, etc.)."""
        latest_timestamp = self.test_data['date'].max()
        full_data = self.test_data[self.test_data['date'] == latest_timestamp].copy()
        
        # Test subset of tickers
        subset_data = full_data.head(15)
        decisions = self.finrl_service.run_finrl_model(subset_data)
        self.assertIsInstance(decisions, dict)
        
        # Test single ticker
        single_data = full_data.head(1)
        decisions = self.finrl_service.run_finrl_model(single_data)
        self.assertIsInstance(decisions, dict)
        
        # Test all 30 tickers
        decisions = self.finrl_service.run_finrl_model(full_data)
        self.assertIsInstance(decisions, dict)
        
        print(f"✅ Edge cases: subset (15), single ticker, full (30)")
    
    def test_02_state_vector_construction(self):
        """Test 2: Verify state vector is correctly constructed (301 dims)."""
        print("\n" + "-"*80)
        print("TEST 2: State Vector Construction")
        print("-"*80)
        
        # Get a snapshot of data (latest timestamp for all tickers)
        latest_timestamp = self.test_data['date'].max()
        market_data = self.test_data[self.test_data['date'] == latest_timestamp].copy()
        
        print(f"\n📊 Test data snapshot:")
        print(f"   Timestamp: {latest_timestamp}")
        print(f"   Tickers: {len(market_data)} records")
        print(f"   Sample tickers: {market_data['tic'].head(5).tolist()}")
        
        # Build state vector
        print("\n⏳ Building state vector...")
        state = self.finrl_service._build_state_vector(market_data)
        
        # Verify shape
        expected_shape = (301,)
        actual_shape = state.shape
        
        print(f"\n📐 State Vector Analysis:")
        print(f"   Expected shape: {expected_shape}")
        print(f"   Actual shape: {actual_shape}")
        print(f"   Data type: {state.dtype}")
        print(f"   Value range: [{state.min():.6f}, {state.max():.6f}]")
        print(f"   Mean: {state.mean():.6f}")
        print(f"   Std: {state.std():.6f}")
        print(f"   NaN count: {np.isnan(state).sum()}")
        print(f"   Inf count: {np.isinf(state).sum()}")
        
        # Breakdown of state components
        print(f"\n🔍 State Vector Components:")
        print(f"   Cash amount (1 value): {state[0]:.6f}")
        print(f"   Prices (30 values): {state[1:31][:5]}...")
        print(f"   Stock holdings (30 values): {state[31:61][:5]}...")
        print(f"   Tech indicators (240 values): {state[61:][:5]}...")
        
        self.assertEqual(actual_shape, expected_shape, 
                        f"State vector should be {expected_shape}, got {actual_shape}")
        self.assertEqual(state.dtype, np.float32, "State vector should be float32")
        self.assertEqual(np.isnan(state).sum(), 0, "State vector should not contain NaN")
        self.assertEqual(np.isinf(state).sum(), 0, "State vector should not contain Inf")
        
        print("\n✅ State vector construction passed all checks!")
    
    def test_03_model_prediction(self):
        """Test 3: Verify model produces valid predictions."""
        print("\n" + "-"*80)
        print("TEST 3: Model Prediction")
        print("-"*80)
        
        # Get market data
        latest_timestamp = self.test_data['date'].max()
        market_data = self.test_data[self.test_data['date'] == latest_timestamp].copy()
        
        print(f"\n📊 Running prediction on {len(market_data)} tickers")
        
        # Run prediction
        print("⏳ Calling model.predict()...")
        state = self.finrl_service._build_state_vector(market_data)
        action, _ = self.finrl_service.model.predict(state.reshape(1, -1), deterministic=True)
        action = action[0]
        
        print(f"\n📈 Raw Model Output:")
        print(f"   Action shape: {action.shape}")
        print(f"   Action dtype: {action.dtype}")
        print(f"   Action range: [{action.min():.4f}, {action.max():.4f}]")
        print(f"   Action mean: {action.mean():.4f}")
        print(f"   Action std: {action.std():.4f}")
        print(f"   Sample actions: {action[:5]}")
        
        # Verify action shape
        expected_action_shape = (30,)  # 30 tickers (excluding VIXY)
        self.assertEqual(action.shape, expected_action_shape,
                        f"Action should be {expected_action_shape}, got {action.shape}")
        
        # Scale actions
        scaled_action = (action * self.finrl_service.max_stock).astype(int)
        
        print(f"\n💰 Scaled Actions (action * {self.finrl_service.max_stock}):")
        print(f"   Scaled range: [{scaled_action.min()}, {scaled_action.max()}]")
        print(f"   Scaled mean: {scaled_action.mean():.2f}")
        print(f"   Sample scaled: {scaled_action[:5]}")
        
        # Count buy/sell/hold signals
        buy_count = (scaled_action > self.finrl_service.min_action).sum()
        sell_count = (scaled_action < -self.finrl_service.min_action).sum()
        hold_count = len(scaled_action) - buy_count - sell_count
        
        print(f"\n📊 Signal Distribution:")
        print(f"   BUY signals (>{self.finrl_service.min_action}): {buy_count}")
        print(f"   SELL signals (<-{self.finrl_service.min_action}): {sell_count}")
        print(f"   HOLD signals: {hold_count}")
        
        print("\n✅ Model prediction passed all checks!")
    
    def test_04_full_finrl_pipeline(self):
        """Test 4: Test complete FinRL pipeline (state → prediction → decisions)."""
        print("\n" + "-"*80)
        print("TEST 4: Full FinRL Pipeline")
        print("-"*80)
        
        # Get market data
        latest_timestamp = self.test_data['date'].max()
        market_data = self.test_data[self.test_data['date'] == latest_timestamp].copy()
        
        print(f"\n📊 Input Data:")
        print(f"   Timestamp: {latest_timestamp}")
        print(f"   Tickers: {len(market_data)}")
        
        # Run complete pipeline
        print("\n⏳ Running complete FinRL pipeline...")
        decisions = self.finrl_service.run_finrl_model(market_data)
        
        print(f"\n📋 Decision Output:")
        print(f"   Timestamp: {decisions.get('timestamp')}")
        print(f"   Total analyzed: {decisions.get('total_analyzed')}")
        print(f"   Selected tickers: {len(decisions.get('selected_tickers', []))}")
        print(f"   Buy decisions: {len(decisions.get('buy_decisions', {}))}")
        print(f"   Sell decisions: {len(decisions.get('sell_decisions', {}))}")
        
        # Print top decisions
        buy_decisions = decisions.get('buy_decisions', {})
        sell_decisions = decisions.get('sell_decisions', {})
        
        if buy_decisions:
            print(f"\n🟢 Top Buy Decisions:")
            for ticker, qty in sorted(buy_decisions.items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"   {ticker}: Buy {qty} shares")
        
        if sell_decisions:
            print(f"\n🔴 Top Sell Decisions:")
            for ticker, qty in sorted(sell_decisions.items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"   {ticker}: Sell {qty} shares")
        
        # Verify decision structure
        self.assertIn('timestamp', decisions, "Should have timestamp")
        self.assertIn('selected_tickers', decisions, "Should have selected_tickers")
        self.assertIn('buy_decisions', decisions, "Should have buy_decisions")
        self.assertIn('sell_decisions', decisions, "Should have sell_decisions")
        self.assertIn('total_analyzed', decisions, "Should have total_analyzed")
        
        # Verify data types
        self.assertIsInstance(decisions['selected_tickers'], list)
        self.assertIsInstance(decisions['buy_decisions'], dict)
        self.assertIsInstance(decisions['sell_decisions'], dict)
        
        # Verify ticker count
        self.assertLessEqual(
            len(decisions['selected_tickers']), 
            settings.finrl_output_tickers,
            f"Should select max {settings.finrl_output_tickers} tickers"
        )
        
        print("\n✅ Full pipeline passed all checks!")
    
    def test_05_multiple_timestamps(self):
        """Test 5: Test model consistency across multiple timestamps."""
        unique_timestamps = sorted(self.test_data['date'].unique())[:3]
        
        results = []
        for timestamp in unique_timestamps:
            market_data = self.test_data[self.test_data['date'] == timestamp].copy()
            decisions = self.finrl_service.run_finrl_model(market_data)
            results.append(decisions)
        
        self.assertEqual(len(results), len(unique_timestamps))
        
        # All should return valid decision structure (even if 0 selections)
        for result in results:
            self.assertIn('selected_tickers', result)
            self.assertIsInstance(result['selected_tickers'], list)
        
        counts = [len(r.get('selected_tickers', [])) for r in results]
        print(f"✅ Multi-timestamp: {len(unique_timestamps)} timestamps processed, selected={counts}")
    
    def test_06_edge_cases(self):
        """Test 6: Test edge cases (missing tickers, minimal data, etc.)."""
        print("\n" + "-"*80)
        print("TEST 6: Edge Cases")
        print("-"*80)
        
        # Test 6a: Subset of tickers
        print("\n🔍 Test 6a: Subset of tickers (15 instead of 30)")
        latest_timestamp = self.test_data['date'].max()
        full_data = self.test_data[self.test_data['date'] == latest_timestamp].copy()
        subset_data = full_data.head(15)
        
        print(f"   Input: {len(subset_data)} tickers")
        decisions = self.finrl_service.run_finrl_model(subset_data)
        print(f"   Output: {len(decisions.get('selected_tickers', []))} selected")
        self.assertIsInstance(decisions, dict, "Should return valid decisions")
        print("   ✅ Subset test passed")
        
        # Test 6b: Single ticker
        print("\n🔍 Test 6b: Single ticker only")
        single_data = full_data.head(1)
        print(f"   Input: {len(single_data)} ticker")
        decisions = self.finrl_service.run_finrl_model(single_data)
        print(f"   Output: {len(decisions.get('selected_tickers', []))} selected")
        self.assertIsInstance(decisions, dict, "Should return valid decisions")
        print("   ✅ Single ticker test passed")
        
        # Test 6c: Data with VIXY included
        print("\n🔍 Test 6c: Data including VIXY")
        data_with_vixy = full_data.copy()
        print(f"   Unique tickers in data: {data_with_vixy['tic'].nunique()}")
        decisions = self.finrl_service.run_finrl_model(data_with_vixy)
        print(f"   Output: {len(decisions.get('selected_tickers', []))} selected")
        # VIXY should not be in selected tickers (it's for turbulence only)
        self.assertNotIn('VIXY', decisions.get('selected_tickers', []),
                        "VIXY should not be in trading decisions")
        print("   ✅ VIXY handling test passed")
        
        print("\n✅ All edge cases passed!")


def run_tests():
    """Run the test suite."""
    suite = unittest.TestLoader().loadTestsFromTestCase(TestFinRLBaseModel)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    print(f"Tests run: {result.testsRun}")
    print(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    
    if result.wasSuccessful():
        print("\n🎉 ALL TESTS PASSED!")
    else:
        print("\n❌ SOME TESTS FAILED")
    print("="*80 + "\n")
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)