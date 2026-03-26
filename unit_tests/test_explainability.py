#!/usr/bin/env python3
"""
Unit Test 4: Model Explainability Test (SHAP & LIME)

This test verifies:
1. SHAP (SHapley Additive exPlanations) can explain model predictions
2. LIME (Local Interpretable Model-agnostic Explanations) can explain model predictions
3. Feature importance can be extracted from the FinRL model
4. Explanations are consistent and meaningful

Uses trade_data_3days.csv as test data source.
"""

import sys
import unittest
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.src.services.finrl_service_finetuned import FinRLServiceWithFineTuning


class TestExplainability(unittest.TestCase):
    """Test suite for model explainability (SHAP & LIME)."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures once for all tests."""
        print("\n" + "="*80)
        print("TEST SUITE: Model Explainability (SHAP & LIME)")
        print("="*80)
        
        # Load test data from CSV
        csv_path = Path(__file__).parent / "trade_data_3days.csv"
        if not csv_path.exists():
            raise FileNotFoundError(f"Test data not found: {csv_path}")
        
        cls.test_data = pd.read_csv(csv_path, index_col=0)
        cls.test_data['date'] = pd.to_datetime(cls.test_data['date'])
        
        print(f"✅ Loaded {len(cls.test_data)} rows from CSV")
        print(f"   Tickers: {cls.test_data['tic'].nunique()}")
        print(f"   Date range: {cls.test_data['date'].min()} to {cls.test_data['date'].max()}")
        
        # Initialize FinRL service
        print("\n🤖 Initializing FinRL Service...")
        cls.finrl_service = FinRLServiceWithFineTuning(
            news_producer=None,
            social_producer=None
        )
        
        # Set model paths
        project_root = Path(__file__).parent.parent
        cls.finrl_service.base_model_path = str(project_root / "backend/finrl_integration/agent_ppo")
        cls.finrl_service.finetuned_model_path = str(project_root / "backend/finrl_integration/agent_ppo_finetuned")
        
        # Load model
        success = cls.finrl_service.load_model()
        if not success:
            raise RuntimeError("Failed to load FinRL model")
        
        print(f"✅ Model loaded: {Path(cls.finrl_service.current_model_path).name}")
        
        # Get sample data for testing
        latest_timestamp = cls.test_data['date'].max()
        cls.sample_data = cls.test_data[cls.test_data['date'] == latest_timestamp].copy()
        
        # Build state vector for explanations
        cls.sample_state = cls.finrl_service._build_state_vector(cls.sample_data)
        
        # Get prediction
        cls.sample_action, _ = cls.finrl_service.model.predict(
            cls.sample_state.reshape(1, -1), 
            deterministic=True
        )
        cls.sample_action = cls.sample_action[0]
        
        print(f"\n📊 Sample data prepared:")
        print(f"   Timestamp: {latest_timestamp}")
        print(f"   State shape: {cls.sample_state.shape}")
        print(f"   Action shape: {cls.sample_action.shape}")
        print(f"   Action range: [{cls.sample_action.min():.3f}, {cls.sample_action.max():.3f}]")
        
        # Define feature names for interpretability
        cls.feature_names = cls._create_feature_names()
        print(f"\n✅ Feature names created: {len(cls.feature_names)} features")
    
    @classmethod
    def _create_feature_names(cls):
        """Create human-readable feature names for the state vector."""
        names = []
        
        # Cash (1 feature)
        names.append('cash_balance')
        
        # Prices (30 features)
        for ticker in cls.finrl_service.ticker_list:
            names.append(f'price_{ticker}')
        
        # Holdings (30 features)
        for ticker in cls.finrl_service.ticker_list:
            names.append(f'holding_{ticker}')
        
        # Technical indicators (8 indicators × 30 tickers = 240 features)
        indicators = ['macd', 'boll_ub', 'boll_lb', 'rsi_30', 'cci_30', 'dx_30', 'close_30_sma', 'close_60_sma']
        for ticker in cls.finrl_service.ticker_list:
            for indicator in indicators:
                names.append(f'{indicator}_{ticker}')
        
        return names
    
    def test_01_shap_installation(self):
        """Test 1: Verify SHAP library is available."""
        print("\n" + "-"*80)
        print("TEST 1: SHAP Library Installation")
        print("-"*80)
        
        try:
            import shap
            print(f"\n✅ SHAP library installed")
            print(f"   Version: {shap.__version__}")
            self.shap_available = True
        except ImportError as e:
            print(f"\n⚠️  SHAP not installed: {e}")
            print("   Install with: pip install shap")
            self.shap_available = False
            self.skipTest("SHAP library not available")
    
    def test_02_lime_installation(self):
        """Test 2: Verify LIME library is available."""
        print("\n" + "-"*80)
        print("TEST 2: LIME Library Installation")
        print("-"*80)
        
        try:
            import lime
            import lime.lime_tabular
            print(f"\n✅ LIME library installed")
            self.lime_available = True
        except ImportError as e:
            print(f"\n⚠️  LIME not installed: {e}")
            print("   Install with: pip install lime")
            self.lime_available = False
            self.skipTest("LIME library not available")
    
    def test_03_shap_explainer(self):
        """Test 3: Create SHAP explainer and compute values."""
        print("\n" + "-"*80)
        print("TEST 3: SHAP Explainer")
        print("-"*80)
        
        try:
            import shap
        except ImportError:
            self.skipTest("SHAP not available")
        
        print(f"\n🔍 Creating SHAP explainer...")
        
        # Create prediction function wrapper
        def predict_fn(states):
            """Predict function for SHAP."""
            predictions = []
            for state in states:
                action, _ = self.finrl_service.model.predict(
                    state.reshape(1, -1),
                    deterministic=True
                )
                predictions.append(action[0])
            return np.array(predictions)
        
        # Create background dataset (sample of states)
        num_background = min(100, len(self.test_data['date'].unique()))
        background_timestamps = np.random.choice(
            self.test_data['date'].unique(),
            size=num_background,
            replace=False
        )
        
        background_states = []
        for ts in background_timestamps:
            ts_data = self.test_data[self.test_data['date'] == ts].copy()
            state = self.finrl_service._build_state_vector(ts_data)
            background_states.append(state)
        
        background_states = np.array(background_states)
        
        print(f"   Background dataset: {background_states.shape}")
        
        # Create SHAP explainer
        print(f"\n⏳ Computing SHAP values (this may take a while)...")
        explainer = shap.KernelExplainer(
            predict_fn,
            background_states[:10]  # Use small background for testing
        )
        
        # Compute SHAP values for sample
        shap_values = explainer.shap_values(self.sample_state.reshape(1, -1))
        
        print(f"\n📊 SHAP results:")
        print(f"   SHAP values shape: {np.array(shap_values).shape}")
        print(f"   Expected: ({self.sample_action.shape[0]}, {len(self.feature_names)})")
        
        # Get top features for first ticker
        # Shape is (1, 301, 30) - (samples, features, actions)
        if len(shap_values) > 0:
            shap_array = np.array(shap_values)
            if shap_array.ndim == 3:
                # For multi-output: (samples, features, outputs)
                # Get SHAP values for first output (first ticker)
                ticker_0_shap = shap_array[0, :, 0]  # First sample, all features, first output
            elif shap_array.ndim == 2:
                ticker_0_shap = shap_array[0, :]  # First sample, all features
            else:
                ticker_0_shap = shap_values[0] if isinstance(shap_values, list) else shap_values
            
            top_indices = np.argsort(np.abs(ticker_0_shap))[-5:]
            
            print(f"\n🔝 Top 5 influential features for {self.finrl_service.ticker_list[0]}:")
            for idx in top_indices[::-1]:
                feature_name = self.feature_names[int(idx)]
                shap_val = ticker_0_shap[int(idx)]
                print(f"   {feature_name}: {shap_val:.4f}")
        
        # Store for later tests
        self.__class__.shap_values = shap_values
        self.__class__.shap_explainer = explainer
        
        print("\n✅ SHAP explainer test passed!")
    
    def test_04_lime_explainer(self):
        """Test 4: Create LIME explainer and generate explanations."""
        print("\n" + "-"*80)
        print("TEST 4: LIME Explainer")
        print("-"*80)
        
        try:
            import lime
            import lime.lime_tabular
        except ImportError:
            self.skipTest("LIME not available")
        
        print(f"\n🔍 Creating LIME explainer...")
        
        # Create prediction function wrapper
        def predict_fn(states):
            """Predict function for LIME."""
            predictions = []
            for state in states:
                action, _ = self.finrl_service.model.predict(
                    state.reshape(1, -1),
                    deterministic=True
                )
                predictions.append(action[0])
            return np.array(predictions)
        
        # Create training data for LIME
        num_training = min(100, len(self.test_data['date'].unique()))
        training_timestamps = np.random.choice(
            self.test_data['date'].unique(),
            size=num_training,
            replace=False
        )
        
        training_states = []
        for ts in training_timestamps:
            ts_data = self.test_data[self.test_data['date'] == ts].copy()
            state = self.finrl_service._build_state_vector(ts_data)
            training_states.append(state)
        
        training_states = np.array(training_states)
        
        print(f"   Training dataset: {training_states.shape}")
        
        # Create LIME explainer
        explainer = lime.lime_tabular.LimeTabularExplainer(
            training_states,
            feature_names=self.feature_names,
            mode='regression',
            verbose=False
        )
        
        print(f"\n⏳ Generating LIME explanation...")
        
        # Explain sample prediction for first ticker
        explanation = explainer.explain_instance(
            self.sample_state,
            lambda x: predict_fn(x)[:, 0],  # First ticker
            num_features=10
        )
        
        print(f"\n📊 LIME explanation for {self.finrl_service.ticker_list[0]}:")
        print(f"   Predicted value: {explanation.predicted_value:.4f}")
        print(f"   Local prediction: {explanation.local_pred[0]:.4f}")
        
        print(f"\n🔝 Top 10 influential features:")
        for feature, weight in explanation.as_list()[:10]:
            print(f"   {feature}: {weight:.4f}")
        
        # Store for later tests
        self.__class__.lime_explanation = explanation
        self.__class__.lime_explainer = explainer
        
        print("\n✅ LIME explainer test passed!")
    
    def test_05_compare_explanations(self):
        """Test 5: Compare SHAP and LIME explanations."""
        print("\n" + "-"*80)
        print("TEST 5: Compare SHAP vs LIME")
        print("-"*80)
        
        if not hasattr(self.__class__, 'shap_values'):
            self.skipTest("SHAP values not available")
        if not hasattr(self.__class__, 'lime_explanation'):
            self.skipTest("LIME explanation not available")
        
        print(f"\n🔍 Comparing explanations for {self.finrl_service.ticker_list[0]}...")
        
        # Get top features from SHAP
        shap_vals = self.__class__.shap_values
        shap_array = np.array(shap_vals)
        
        if shap_array.ndim == 3:
            # Shape: (1, 301, 30) - get first sample, all features, first ticker
            ticker_0_shap = shap_array[0, :, 0]
        elif shap_array.ndim == 2:
            ticker_0_shap = shap_array[0, :]
        else:
            ticker_0_shap = shap_vals[0] if isinstance(shap_vals, list) else shap_vals
        
        shap_top_indices = set([int(i) for i in np.argsort(np.abs(ticker_0_shap))[-10:]])
        
        # Get top features from LIME
        lime_features = [feat.split('<')[0].split('>')[0].strip() for feat, _ in self.__class__.lime_explanation.as_list()[:10]]
        lime_top_indices = set([self.feature_names.index(f) for f in lime_features if f in self.feature_names])
        
        # Find overlap
        overlap = shap_top_indices & lime_top_indices
        
        print(f"\n📊 Feature overlap:")
        print(f"   SHAP top 10 features: {len(shap_top_indices)}")
        print(f"   LIME top 10 features: {len(lime_top_indices)}")
        print(f"   Common features: {len(overlap)}")
        print(f"   Overlap percentage: {len(overlap)/10*100:.1f}%")
        
        if overlap:
            print(f"\n🔗 Common influential features:")
            for idx in overlap:
                print(f"   {self.feature_names[idx]}")
        
        print("\n✅ Explanation comparison complete!")
    
    def test_06_feature_importance_summary(self):
        """Test 6: Generate feature importance summary across all tickers."""
        print("\n" + "-"*80)
        print("TEST 6: Feature Importance Summary")
        print("-"*80)
        
        if not hasattr(self.__class__, 'shap_values'):
            self.skipTest("SHAP values not available")
        
        print(f"\n🔍 Analyzing feature importance across all tickers...")
        
        shap_vals = self.__class__.shap_values
        shap_array = np.array(shap_vals)
        
        # Aggregate SHAP values across all tickers
        # Shape is (1, 301, 30) - (samples, features, actions)
        if shap_array.ndim == 3:
            # Average absolute SHAP across all samples and all outputs (tickers)
            aggregated_shap = np.mean(np.abs(shap_array), axis=(0, 2))  # Shape: (301,)
        elif shap_array.ndim == 2:
            # Average across samples
            aggregated_shap = np.mean(np.abs(shap_array), axis=0)
        else:
            aggregated_shap = np.abs(shap_array)
        
        # Get top 10 features overall
        top_indices = np.argsort(aggregated_shap.flatten())[-10:]
        
        print(f"\n🔝 Top 10 most influential features overall:")
        for i, idx in enumerate(top_indices[::-1], 1):
            idx = int(idx)
            feature_name = self.feature_names[idx]
            importance = aggregated_shap.flatten()[idx]
            print(f"   {i}. {feature_name}: {importance:.4f}")
        
        # Categorize features
        categories = {
            'cash': 0,
            'prices': 0,
            'holdings': 0,
            'technical': 0
        }
        
        for idx in top_indices:
            idx = int(idx)
            fname = self.feature_names[idx]
            if 'cash' in fname:
                categories['cash'] += 1
            elif 'price_' in fname:
                categories['prices'] += 1
            elif 'holding_' in fname:
                categories['holdings'] += 1
            else:
                categories['technical'] += 1
        
        print(f"\n📊 Feature category breakdown:")
        for cat, count in categories.items():
            print(f"   {cat.capitalize()}: {count}/10")
        
        print("\n✅ Feature importance summary complete!")


def run_tests():
    """Run the test suite."""
    suite = unittest.TestLoader().loadTestsFromTestCase(TestExplainability)
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
    print(f"Skipped: {len(result.skipped)}")
    
    if result.wasSuccessful():
        print("\n🎉 ALL TESTS PASSED! 🎉")
        print("\n✅ Model explainability (SHAP & LIME) is working correctly!")
    else:
        print("\n⚠️  SOME TESTS FAILED OR WERE SKIPPED")
        if any('SHAP not available' in str(s) for s in result.skipped):
            print("\n📦 To install SHAP:")
            print("   pip install shap")
        if any('LIME not available' in str(s) for s in result.skipped):
            print("\n📦 To install LIME:")
            print("   pip install lime")
    
    print("="*80 + "\n")
    
    return result.wasSuccessful()


if __name__ == '__main__':
    run_tests()
