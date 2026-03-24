"""
Explainability Service for FinRL Model

Provides SHAP and LIME explanations for FinRL predictions with JSONL logging.
"""

import logging
import numpy as np
import pandas as pd
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import json
import pickle
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

logger = logging.getLogger(__name__)


class ExplainabilityService:
    """Service for generating SHAP and LIME explanations with JSONL logging."""
    
    def __init__(
        self,
        ticker_list: List[str],
        tech_indicators: List[str],
        shap_samples: int = 50,  # Number of background samples for SHAP
        lime_samples: int = 100,  # Number of training samples for LIME
        log_dir: Optional[str] = None,  # Optional JSONL log directory
        explainer_dir: Optional[str] = None,  # Directory to load pre-trained explainers
    ):
        """Initialize explainability service.
        
        Args:
            ticker_list: List of stock tickers (30 tickers)
            tech_indicators: List of technical indicators (8 indicators)
            shap_samples: Number of background samples for SHAP (default 50)
            lime_samples: Number of training samples for LIME (default 100)
            log_dir: Directory for JSONL logs (optional)
            explainer_dir: Directory to load pre-trained explainers (optional)
        """
        logger.info("      📊 Creating feature names...")
        self.ticker_list = ticker_list
        self.tech_indicators = tech_indicators
        self.shap_samples = shap_samples
        self.lime_samples = lime_samples
        
        # Create feature names
        self.feature_names = self._create_feature_names()
        logger.info(f"      ✅ Created {len(self.feature_names)} feature names")
        
        # Cache for background data
        logger.info("      📦 Initializing background cache...")
        self.background_states = []
        self.max_background_size = max(shap_samples, lime_samples)
        
        # Explainers (lazy initialization)
        self.shap_explainer = None
        self.lime_explainer = None
        
        # Thread lock for explainer creation
        self._explainer_lock = threading.Lock()
        
        # JSONL logging
        logger.info("      📝 Setting up JSONL logging...")
        self.log_dir = Path(log_dir) if log_dir else None
        self.log_file = None
        if self.log_dir:
            self.log_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.log_file = self.log_dir / f"explanations_{timestamp}.jsonl"
            logger.info(f"      ✅ JSONL logging: {self.log_file}")
        
        # Check library availability
        logger.info("      🔎 Checking ML libraries...")
        self.shap_available = self._check_shap()
        self.lime_available = self._check_lime()
        
        # Load pre-trained explainers if directory provided
        if explainer_dir:
            logger.info(f"      📂 Loading pre-trained explainers from {explainer_dir}...")
            try:
                if self.load_explainers(explainer_dir):
                    logger.info(f"      ✅ Pre-trained explainers loaded ({len(self.background_states)} samples)")
                else:
                    logger.warning(f"      ⚠️  No pre-trained explainers found in {explainer_dir}")
            except Exception as e:
                logger.error(f"      ❌ Failed to load explainers: {e}")
                logger.exception("Full traceback:")
        
        logger.info("🔍 Explainability Service initialized")
        logger.info(f"   SHAP samples: {shap_samples}")
        logger.info(f"   LIME samples: {lime_samples}")
        logger.info(f"   Features: {len(self.feature_names)}")
        logger.info(f"   SHAP available: {self.shap_available}")
        logger.info(f"   LIME available: {self.lime_available}")
    
    def _check_shap(self) -> bool:
        """Check if SHAP is available."""
        try:
            import shap
            return True
        except ImportError:
            logger.warning("⚠️  SHAP not installed. Run: pip install shap")
            return False
    
    def _check_lime(self) -> bool:
        """Check if LIME is available."""
        try:
            import lime
            import lime.lime_tabular
            return True
        except ImportError:
            logger.warning("⚠️  LIME not installed. Run: pip install lime")
            return False
    
    def _create_feature_names(self) -> List[str]:
        """Create human-readable feature names for the state vector."""
        names = []
        
        # Cash (1 feature)
        names.append('cash_balance')
        
        # Prices (30 features)
        for ticker in self.ticker_list:
            names.append(f'price_{ticker}')
        
        # Holdings (30 features)
        for ticker in self.ticker_list:
            names.append(f'holding_{ticker}')
        
        # Technical indicators (8 indicators × 30 tickers = 240 features)
        for ticker in self.ticker_list:
            for indicator in self.tech_indicators:
                names.append(f'{indicator}_{ticker}')
        
        return names
    
    def save_explainers(self, save_dir: str) -> bool:
        """Save background data to disk (explainers will be recreated on load).
        
        Args:
            save_dir: Directory to save files
        
        Returns:
            True if successful, False otherwise
        """
        try:
            save_path = Path(save_dir)
            save_path.mkdir(parents=True, exist_ok=True)
            
            # Save background states only (explainers will be recreated)
            if len(self.background_states) > 0:
                bg_file = save_path / "background_states.npy"
                np.save(bg_file, np.array(self.background_states))
                logger.info(f"✅ Saved {len(self.background_states)} background states to {bg_file}")
            else:
                logger.warning("⚠️  No background states to save")
            
            # Save metadata
            metadata = {
                'ticker_list': self.ticker_list,
                'tech_indicators': self.tech_indicators,
                'shap_samples': self.shap_samples,
                'lime_samples': self.lime_samples,
                'num_background_samples': len(self.background_states),
                'feature_count': len(self.feature_names),
                'saved_at': datetime.now().isoformat(),
                'note': 'Explainers will be created on first use from background data'
            }
            metadata_file = save_path / "explainer_metadata.json"
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            logger.info(f"✅ Saved metadata to {metadata_file}")
            
            logger.info("📝 Note: Explainers will be created automatically on first use")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to save: {e}")
            return False
    
    def load_explainers(self, load_dir: str) -> bool:
        """Load background data from disk (explainers created on first use).
        
        Args:
            load_dir: Directory containing saved files
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"         🔄 Checking directory: {load_dir}")
            load_path = Path(load_dir)
            if not load_path.exists():
                logger.warning(f"         ⚠️  Directory does not exist: {load_path}")
                return False
            
            # Load metadata first
            logger.info(f"         📄 Looking for metadata...")
            metadata_file = load_path / "explainer_metadata.json"
            if metadata_file.exists():
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                logger.info(f"         ✅ Loaded metadata")
                logger.info(f"            Saved at: {metadata.get('saved_at')}")
                logger.info(f"            Background samples: {metadata.get('num_background_samples')}")
            
            # Load background states
            logger.info(f"         📦 Loading background states...")
            bg_file = load_path / "background_states.npy"
            if bg_file.exists():
                logger.info(f"         🔄 Reading numpy file ({bg_file.stat().st_size / 1024:.1f} KB)...")
                loaded_states = np.load(bg_file)
                logger.info(f"         ✅ Numpy array loaded: shape {loaded_states.shape}")
                
                self.background_states = list(loaded_states)
                logger.info(f"         ✅ Converted to list: {len(self.background_states)} states")
                logger.info(f"         ℹ️  Explainers will be created on first use (fast)")
            else:
                logger.warning(f"         ⚠️  Background states file not found: {bg_file}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to load: {e}")
            return False
    
    def add_background_sample(self, state: np.ndarray):
        """Add a state to the background dataset.
        
        Args:
            state: State vector (301 dimensions)
        """
        self.background_states.append(state.copy())
        
        # Keep only most recent samples
        if len(self.background_states) > self.max_background_size:
            self.background_states = self.background_states[-self.max_background_size:]
    
    def has_enough_samples(self) -> Dict[str, bool]:
        """Check if we have enough samples for explanations.
        
        Returns:
            Dict with 'shap' and 'lime' availability
        """
        return {
            'shap': len(self.background_states) >= self.shap_samples,
            'lime': len(self.background_states) >= self.lime_samples,
            'samples_collected': len(self.background_states)
        }
    
    def explain_with_shap(
        self,
        model,
        state: np.ndarray,
        ticker_idx: int = 0,
        top_k: int = 10
    ) -> Optional[Dict[str, Any]]:
        """Generate SHAP explanation for a prediction.
        
        Args:
            model: Trained PPO model
            state: State vector to explain (301 dimensions)
            ticker_idx: Index of ticker to explain (0-29)
            top_k: Number of top features to return
        
        Returns:
            Dict with SHAP values and top features, or None if not available
        """
        if not self.shap_available:
            return None
        
        if len(self.background_states) < self.shap_samples:
            logger.warning(f"⚠️  Need {self.shap_samples} samples for SHAP, have {len(self.background_states)}")
            return None
        
        try:
            import shap
            
            # Use background samples (ensure we don't exceed available samples)
            max_bg = min(self.shap_samples, len(self.background_states))
            background = np.array(self.background_states[:max_bg])
            
            # Create explainer (cache it) - only if not already loaded (thread-safe)
            with self._explainer_lock:
                if self.shap_explainer is None:
                    # Use all available background samples for better explainer quality
                    explainer_bg_size = len(background)
                    logger.info(f"🔍 Creating SHAP explainer with {explainer_bg_size} background samples...")
                    # Use class method for predictions (can be pickled)
                    self.shap_explainer = shap.KernelExplainer(
                        lambda x: self._predict_batch(model, x),
                        background  # Use all background samples
                    )
                else:
                    logger.info(f"⚡ Using pre-loaded SHAP explainer (fast inference mode)")
            
            # Compute SHAP values - nsamples must be at least 2 less than background
            # SHAP has an internal off-by-one bug where it tries to access index nsamples
            # in an array of size nsamples, so we need some buffer
            max_nsamples = min(50, max(10, len(background) - 2)) if len(background) > 2 else 10
            shap_values = self.shap_explainer.shap_values(
                state.reshape(1, -1),
                nsamples=max_nsamples  # Limit perturbations, ensure within bounds
            )
            
            # Extract values for specific ticker
            shap_array = np.array(shap_values)
            if shap_array.ndim == 3:
                # Shape: (1, 301, 30) - get first sample, all features, specific ticker
                ticker_shap = shap_array[0, :, ticker_idx]
            elif shap_array.ndim == 2:
                ticker_shap = shap_array[0, :]
            else:
                ticker_shap = shap_values[ticker_idx] if isinstance(shap_values, list) else shap_values
            
            # Get top features
            top_indices = np.argsort(np.abs(ticker_shap))[-top_k:][::-1]
            
            top_features = []
            for idx in top_indices:
                idx = int(idx)
                top_features.append({
                    'feature': self.feature_names[idx],
                    'shap_value': float(ticker_shap[idx]),
                    'importance': float(np.abs(ticker_shap[idx]))
                })
            
            return {
                'method': 'SHAP',
                'ticker': self.ticker_list[ticker_idx],
                'ticker_index': ticker_idx,
                'top_features': top_features,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ SHAP explanation failed: {e}")
            return None
    
    def explain_with_lime(
        self,
        model,
        state: np.ndarray,
        ticker_idx: int = 0,
        top_k: int = 10
    ) -> Optional[Dict[str, Any]]:
        """Generate LIME explanation for a prediction.
        
        Args:
            model: Trained PPO model
            state: State vector to explain (301 dimensions)
            ticker_idx: Index of ticker to explain (0-29)
            top_k: Number of top features to return
        
        Returns:
            Dict with LIME explanation and top features, or None if not available
        """
        if not self.lime_available:
            return None
        
        if len(self.background_states) < self.lime_samples:
            logger.warning(f"⚠️  Need {self.lime_samples} samples for LIME, have {len(self.background_states)}")
            return None
        
        try:
            import lime
            import lime.lime_tabular
            
            # Use background samples as training data
            training_data = np.array(self.background_states[:self.lime_samples])
            
            # Create explainer (cache it) - only if not already loaded
            if self.lime_explainer is None:
                logger.info(f"🔍 Creating LIME explainer with {len(training_data)} training samples...")
                self.lime_explainer = lime.lime_tabular.LimeTabularExplainer(
                    training_data,
                    feature_names=self.feature_names,
                    mode='regression',
                    verbose=False
                )
            else:
                logger.info(f"⚡ Using pre-loaded LIME explainer (fast inference mode)")
            
            # Generate explanation for specific ticker
            explanation = self.lime_explainer.explain_instance(
                state,
                lambda x: self._predict_batch(model, x)[:, ticker_idx],
                num_features=top_k,
                num_samples=500  # Reduce from default 5000 for speed
            )
            
            # Extract top features
            top_features = []
            for feature, weight in explanation.as_list()[:top_k]:
                # Parse feature name (may include ranges like "feature <= value")
                feature_name = feature.split('<=')[0].split('>')[0].strip()
                top_features.append({
                    'feature': feature,
                    'feature_name': feature_name,
                    'weight': float(weight),
                    'importance': float(np.abs(weight))
                })
            
            return {
                'method': 'LIME',
                'ticker': self.ticker_list[ticker_idx],
                'ticker_index': ticker_idx,
                'predicted_value': float(explanation.predicted_value),
                'local_prediction': float(explanation.local_pred[0]) if hasattr(explanation, 'local_pred') else None,
                'top_features': top_features,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ LIME explanation failed: {e}")
            return None
    
    def explain_prediction(
        self,
        model,
        state: np.ndarray,
        ticker_idx: int = 0,
        methods: List[str] = ['shap', 'lime'],
        top_k: int = 3  # Changed default to 3 for local explanations
    ) -> Dict[str, Any]:
        """Generate explanations using multiple methods.
        
        Args:
            model: Trained PPO model
            state: State vector to explain (301 dimensions)
            ticker_idx: Index of ticker to explain (0-29)
            methods: List of methods to use ('shap', 'lime')
            top_k: Number of top features to return
        
        Returns:
            Dict with explanations from all methods
        """
        results = {
            'ticker': self.ticker_list[ticker_idx],
            'ticker_index': ticker_idx,
            'samples_available': len(self.background_states),
            'timestamp': datetime.now().isoformat()
        }
        
        # Check if we have enough samples
        sample_status = self.has_enough_samples()
        results['sample_status'] = sample_status
        
        # Generate SHAP explanation
        if 'shap' in methods and sample_status['shap']:
            logger.info(f"🔍 Generating SHAP explanation for {self.ticker_list[ticker_idx]}...")
            shap_result = self.explain_with_shap(model, state, ticker_idx, top_k)
            if shap_result:
                results['shap'] = shap_result
                logger.info(f"✅ SHAP explanation generated")
        
        # Generate LIME explanation
        if 'lime' in methods and sample_status['lime']:
            logger.info(f"🔍 Generating LIME explanation for {self.ticker_list[ticker_idx]}...")
            lime_result = self.explain_with_lime(model, state, ticker_idx, top_k)
            if lime_result:
                results['lime'] = lime_result
                logger.info(f"✅ LIME explanation generated")
        
        return results
    
    def compute_global_importance(
        self,
        model,
        state: np.ndarray,
        top_k: int = 10
    ) -> Dict[str, Any]:
        """Compute global feature importance across all tickers.
        
        Args:
            model: Trained PPO model
            state: State vector (301 dimensions)
            top_k: Number of top features to return
        
        Returns:
            Dict with aggregated feature importance
        """
        try:
            import shap
            
            sample_status = self.has_enough_samples()
            if not sample_status['shap']:
                logger.warning(f"⚠️  Need {self.shap_samples} samples for global importance")
                return None
            
            # Use background samples
            max_bg = min(self.shap_samples, len(self.background_states))
            background = np.array(self.background_states[:max_bg])
            
            # Create explainer if needed
            if self.shap_explainer is None:
                # Use all available background samples for better explainer quality
                explainer_bg_size = len(background)
                logger.info(f"🔍 Creating SHAP explainer for global importance with {explainer_bg_size} samples...")
                self.shap_explainer = shap.KernelExplainer(
                    lambda x: self._predict_batch(model, x),
                    background  # Use all background samples
                )
            
            # Compute SHAP values for all tickers - nsamples must be at least 2 less than background
            max_nsamples = min(50, max(10, len(background) - 2)) if len(background) > 2 else 10
            shap_values = self.shap_explainer.shap_values(
                state.reshape(1, -1),
                nsamples=max_nsamples
            )
            
            # Aggregate across all tickers (average absolute SHAP values)
            shap_array = np.array(shap_values)
            if shap_array.ndim == 3:
                # Shape: (1, 301, 30) - average across all tickers
                aggregated_importance = np.mean(np.abs(shap_array[0, :, :]), axis=1)
            elif shap_array.ndim == 2:
                aggregated_importance = np.abs(shap_array[0, :])
            else:
                aggregated_importance = np.abs(shap_array)
            
            # Get top features
            top_indices = np.argsort(aggregated_importance)[-top_k:][::-1]
            
            top_features = []
            for idx in top_indices:
                idx = int(idx)
                top_features.append({
                    'feature': self.feature_names[idx],
                    'global_importance': float(aggregated_importance[idx]),
                    'rank': len(top_features) + 1
                })
            
            return {
                'method': 'SHAP_Global',
                'description': 'Feature importance aggregated across all 30 tickers',
                'top_features': top_features,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ Global importance failed: {e}")
            return None
    
    def explain_multiple_tickers_parallel(
        self,
        model,
        state: np.ndarray,
        ticker_indices: List[int],
        methods: List[str] = ['shap', 'lime'],
        top_k: int = 10,
        max_workers: int = 3
    ) -> Dict[str, Dict[str, Any]]:
        """Generate explanations for multiple tickers sequentially.
        
        Args:
            model: Trained PPO model
            state: State vector to explain (301 dimensions)
            ticker_indices: List of ticker indices to explain (0-29)
            methods: List of methods to use ('shap', 'lime')
            top_k: Number of top features to return
            max_workers: Ignored (kept for compatibility)
        
        Returns:
            Dict mapping ticker to explanation dict
        """
        results = {}
        
        # Run explanations sequentially to avoid threading issues
        for ticker_idx in ticker_indices:
            try:
                explanation = self.explain_prediction(
                    model, state, ticker_idx, methods, top_k
                )
                if explanation:
                    ticker = self.ticker_list[ticker_idx]
                    results[ticker] = explanation
            except Exception as e:
                logger.error(f"Error explaining ticker {self.ticker_list[ticker_idx]}: {e}")
        
        return results
    
    def log_explanations(
        self,
        explanations: Dict[str, Any],
        logger_fn=None
    ):
        """Log explanations in a readable format.
        
        Args:
            explanations: Explanation dict from explain_prediction()
            logger_fn: Optional custom logger function
        """
        log_fn = logger_fn or logger.info
        
        log_fn("=" * 80)
        log_fn(f"🔍 FEATURE EXPLANATIONS FOR {explanations['ticker']}")
        log_fn("=" * 80)
        
        # SHAP results
        if 'shap' in explanations:
            shap_data = explanations['shap']
            log_fn("\n📊 SHAP (Global Feature Importance):")
            log_fn("-" * 80)
            for i, feat in enumerate(shap_data['top_features'], 1):
                log_fn(f"  {i:2d}. {feat['feature']:40s} | SHAP: {feat['shap_value']:+.4f} | Importance: {feat['importance']:.4f}")
        
        # LIME results
        if 'lime' in explanations:
            lime_data = explanations['lime']
            log_fn("\n🔬 LIME (Local Feature Contribution):")
            log_fn("-" * 80)
            if lime_data.get('predicted_value') is not None:
                log_fn(f"   Predicted value: {lime_data['predicted_value']:.4f}")
            for i, feat in enumerate(lime_data['top_features'], 1):
                log_fn(f"  {i:2d}. {feat['feature']:50s} | Weight: {feat['weight']:+.4f}")
        
        # Sample status
        log_fn("\n📈 Sample Collection Status:")
        log_fn("-" * 80)
        sample_status = explanations['sample_status']
        log_fn(f"   Total samples collected: {sample_status['samples_collected']}")
        log_fn(f"   SHAP ready: {'✅' if sample_status['shap'] else '❌'}")
        log_fn(f"   LIME ready: {'✅' if sample_status['lime'] else '❌'}")
        
        log_fn("=" * 80)
    
    def log_global_importance(
        self,
        global_importance: Dict[str, Any],
        logger_fn=None
    ):
        """Log global feature importance in a readable format.
        
        Args:
            global_importance: Dict from compute_global_importance()
            logger_fn: Optional custom logger function
        """
        if not global_importance:
            return
        
        log_fn = logger_fn or logger.info
        
        log_fn("="*80)
        log_fn("🌍 GLOBAL FEATURE IMPORTANCE (Across All 30 Tickers)")
        log_fn("="*80)
        log_fn(f"Method: {global_importance['method']}")
        log_fn(f"{global_importance['description']}")
        log_fn("\nTop 10 Most Influential Features:")
        log_fn("-"*80)
        
        for feat in global_importance['top_features']:
            log_fn(f"  {feat['rank']:2d}. {feat['feature']:45s} | Importance: {feat['global_importance']:.4f}")
        
        log_fn("="*80)
    
    def _predict_batch(self, model, states):
        """Batch prediction wrapper for SHAP/LIME (can be pickled).
        
        Args:
            model: PPO model
            states: Array of states to predict
        
        Returns:
            Array of actions
        """
        if len(states.shape) == 1:
            states = states.reshape(1, -1)
        actions, _ = model.predict(states, deterministic=True)
        return actions
    
    # ============================================================================
    # JSONL LOGGING METHODS
    # ============================================================================
    
    def log_to_jsonl(
        self,
        ticker: str,
        action: str,
        quantity: int,
        explanation: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log explanation to JSONL file.
        
        Args:
            ticker: Stock ticker
            action: Action (buy/sell/hold)
            quantity: Number of shares
            explanation: Explanation dict from explain_prediction()
            metadata: Additional metadata
        """
        if not self.log_file:
            return
        
        entry = {
            "timestamp": datetime.now().isoformat(),
            "ticker": ticker,
            "action": action,
            "quantity": quantity,
            "explanation": explanation,
            "metadata": metadata or {}
        }
        
        with open(self.log_file, 'a') as f:
            f.write(json.dumps(entry) + '\n')
    
    def get_logged_explanations(self) -> List[Dict[str, Any]]:
        """Get all logged explanations from current session.
        
        Returns:
            List of explanation dicts
        """
        if not self.log_file or not self.log_file.exists():
            return []
        
        explanations = []
        with open(self.log_file, 'r') as f:
            for line in f:
                explanations.append(json.loads(line.strip()))
        
        return explanations
    
    def filter_by_tickers(
        self,
        tickers: List[str],
        log_file: Optional[Path] = None
    ) -> List[Dict[str, Any]]:
        """Filter logged explanations by ticker symbols.
        
        Args:
            tickers: List of tickers to filter
            log_file: Optional specific log file (uses current if None)
        
        Returns:
            Filtered explanations
        """
        file_to_read = log_file or self.log_file
        if not file_to_read or not file_to_read.exists():
            return []
        
        filtered = []
        with open(file_to_read, 'r') as f:
            for line in f:
                exp = json.loads(line.strip())
                if exp.get('ticker') in tickers:
                    filtered.append(exp)
        
        return filtered
    
    def print_decision_report(
        self,
        approved_tickers: List[str],
        log_file: Optional[Path] = None
    ) -> None:
        """Print explanations for decision-approved tickers only.
        
        Args:
            approved_tickers: Tickers approved by decision agent
            log_file: Optional specific log file
        """
        filtered = self.filter_by_tickers(approved_tickers, log_file)
        
        if not filtered:
            logger.warning(f"⚠️  No explanations found for {len(approved_tickers)} approved tickers")
            return
        
        logger.info("=" * 80)
        logger.info("🎯 DECISION AGENT - EXPLAINABILITY REPORT")
        logger.info("=" * 80)
        logger.info(f"Approved Stocks: {len(approved_tickers)}")
        logger.info(f"Explanations Found: {len(filtered)}")
        logger.info("")
        
        for entry in filtered:
            ticker = entry.get('ticker')
            action = entry.get('action', 'unknown')
            quantity = entry.get('quantity', 0)
            explanation = entry.get('explanation', {})
            
            logger.info(f"{'='*80}")
            logger.info(f"🎯 {ticker} - {action.upper()} {quantity} shares")
            logger.info(f"{'='*80}")
            
            # SHAP features
            if 'shap' in explanation:
                shap_feats = explanation['shap'].get('top_features', [])
                logger.info("\n📊 SHAP - Top Features:")
                logger.info("-" * 80)
                for i, f in enumerate(shap_feats, 1):
                    logger.info(f"  {i}. {f['feature']:45s} | Importance: {f['importance']:.4f}")
            
            # LIME features
            if 'lime' in explanation:
                lime_feats = explanation['lime'].get('top_features', [])
                logger.info("\n🔬 LIME - Top Features:")
                logger.info("-" * 80)
                for i, f in enumerate(lime_feats, 1):
                    logger.info(f"  {i}. {f['feature']:50s} | Importance: {f['importance']:.4f}")
            
            logger.info("")
        
        logger.info("=" * 80)
