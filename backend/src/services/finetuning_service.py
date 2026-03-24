"""
Fine-tuning Service

Periodically fine-tunes the PPO model using historical data from MongoDB.
- Runs every 2 hours
- Uses 48h rolling window from MongoDB
- Validates before accepting fine-tuned model
- Saves fine-tuned model for FinRL service to use

Based on production_live_trading.py logic.
"""

import asyncio
import logging
import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional
import pandas as pd
import numpy as np

from stable_baselines3 import PPO
from stable_baselines3.common.vec_env import DummyVecEnv

# Import FinRL environment
import sys
# Add finrl_integration to path for imports
finrl_integration_path = Path(__file__).parent.parent.parent / 'finrl_integration'
sys.path.insert(0, str(finrl_integration_path))
from finrl.meta.env_stock_trading.env_stocktrading import StockTradingEnv
from finrl.config import INDICATORS

from backend.config.settings import settings
from backend.src.services.mongodb_sync_service import MongoDBSyncService

logger = logging.getLogger(__name__)


class FineTuningService:
    """Fine-tunes PPO model periodically using MongoDB historical data."""
    
    def __init__(
        self,
        model_path: str = None,
        finetuned_model_path: str = None,
        mongo_sync_service: MongoDBSyncService = None,
        finetune_interval_hours: int = 2,
        historical_window_hours: int = 48,
        finetune_lr: float = 1e-5,
        finetune_steps: int = 2000,
        validation_split: float = 0.2,
        min_improvement_threshold: float = 0.0,  # Accept if ≥ 0% improvement (no degradation)
        min_days_required: int = 10,
    ):
        """Initialize fine-tuning service."""
        # Remove .zip extension - Stable-Baselines3 adds it automatically
        base_path = model_path or settings.finrl_model_path
        self.model_path = base_path.replace('.zip', '') if base_path.endswith('.zip') else base_path
        self.finetuned_model_path = finetuned_model_path or str(
            Path(self.model_path).parent / "agent_ppo_finetuned"
        )
        
        self.mongo_sync = mongo_sync_service
        self.finetune_interval = finetune_interval_hours
        self.historical_window = historical_window_hours
        
        # Ensure MongoDB connection is ready
        if self.mongo_sync and not self.mongo_sync.collection:
            logger.warning("⚠️  MongoDB sync service not connected, will connect on first use")
        
        # Fine-tuning hyperparameters
        self.finetune_lr = finetune_lr
        self.finetune_steps = finetune_steps
        self.validation_split = validation_split
        self.min_improvement_threshold = min_improvement_threshold  # Minimum improvement % to accept
        # Minimum full daily timestamps required to run fine-tuning
        self.min_days_required = min_days_required
        
        # Model state (for streaming/online learning)
        self.current_model = None  # Continuously updated with fine-tuned versions
        self.last_finetune_time = None
        
        # Config
        self.stock_dim = len(settings.symbols_list)
        self.tech_indicators = INDICATORS
        self.hmax = 100
        self.initial_cash = 1_000_000
        self.transaction_cost_pct = 0.001
        self.reward_scaling = 1e-4
        
        logger.info(f"🔧 Fine-tuning Service initialized")
        logger.info(f"   Base model: {self.model_path}")
        logger.info(f"   Fine-tuned model: {self.finetuned_model_path}")
        logger.info(f"   Interval: {finetune_interval_hours}h")
        logger.info(f"   Historical window: {historical_window_hours}h")
    
    def load_base_model(self):
        """Load the base PPO model (initial state for streaming learning)."""
        try:
            logger.info(f"🤖 Loading base model: {self.model_path}")
            self.current_model = PPO.load(self.model_path)
            logger.info("✅ Base model loaded")
            logger.info("   This will be incrementally improved through fine-tuning")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to load base model: {e}")
            return False
    
    def create_env(self, df: pd.DataFrame) -> DummyVecEnv:
        """Create StockTradingEnv from DataFrame."""
        state_space = 1 + 2 * self.stock_dim + len(self.tech_indicators) * self.stock_dim
        
        df_indexed = df.copy()
        df_indexed = df_indexed.sort_values(['day', 'tic'])
        df_indexed = df_indexed.set_index('day')
        
        env = StockTradingEnv(
            df=df_indexed,
            stock_dim=self.stock_dim,
            hmax=self.hmax,
            initial_amount=self.initial_cash,
            num_stock_shares=[0] * self.stock_dim,
            buy_cost_pct=[self.transaction_cost_pct] * self.stock_dim,
            sell_cost_pct=[self.transaction_cost_pct] * self.stock_dim,
            reward_scaling=self.reward_scaling,
            state_space=state_space,
            action_space=self.stock_dim,
            tech_indicator_list=self.tech_indicators,
            print_verbosity=100000,
        )
        
        return DummyVecEnv([lambda: env])
    
    def evaluate_model(self, model: PPO, df: pd.DataFrame) -> float:
        """Evaluate model on validation data."""
        try:
            env = self.create_env(df)
            obs = env.reset()
            total_reward = 0
            done = False
            
            while not done:
                action, _ = model.predict(obs, deterministic=True)
                obs, reward, done, info = env.step(action)
                total_reward += reward[0]
            
            return total_reward
        except Exception as e:
            logger.error(f"❌ Evaluation error: {e}")
            return 0.0
    
    async def finetune_model(self) -> Dict[str, Any]:
        """Fine-tune the model using MongoDB historical data."""
        logger.info("=" * 80)
        logger.info("🔄 FINE-TUNING MODEL")
        logger.info("=" * 80)
        
        try:
            # Get historical data from MongoDB
            logger.info(f"📂 Fetching {self.historical_window}h of historical data from MongoDB...")
            historical_df = await self.mongo_sync.get_historical_data(self.historical_window)
            
            if historical_df is None or len(historical_df) == 0:
                logger.warning("⚠️  No historical data available in MongoDB")
                return {
                    'success': False,
                    'reason': 'no_historical_data',
                    'timestamp': datetime.utcnow()
                }
            
            # Check minimum data requirement
            unique_dates = sorted(historical_df['date'].unique())

            if len(unique_dates) < self.min_days_required:
                logger.warning(f"⚠️  Insufficient data: {len(unique_dates)} days (need {self.min_days_required})")
                return {
                    'success': False,
                    'reason': f'insufficient_data_{len(unique_dates)}_days',
                    'timestamp': datetime.utcnow()
                }
            
            logger.info(f"✅ Loaded {len(historical_df)} records ({len(unique_dates)} complete timestamps)")
            
            # Split train/validation
            split_idx = max(len(unique_dates) - 2, int(len(unique_dates) * (1 - self.validation_split)))
            
            ft_train = historical_df[historical_df['date'].isin(unique_dates[:split_idx])].copy()
            ft_val = historical_df[historical_df['date'].isin(unique_dates[split_idx:])].copy()
            
            # Reset day indices for each split
            ft_train_dates = sorted(ft_train['date'].unique())
            ft_val_dates = sorted(ft_val['date'].unique())
            
            ft_train_date_to_day = {date: idx for idx, date in enumerate(ft_train_dates)}
            ft_val_date_to_day = {date: idx for idx, date in enumerate(ft_val_dates)}
            
            ft_train['day'] = ft_train['date'].map(ft_train_date_to_day)
            ft_val['day'] = ft_val['date'].map(ft_val_date_to_day)
            
            logger.info(f"📊 Data split: {len(ft_train_dates)} train days, {len(ft_val_dates)} val days")
            
            # Validate completeness (each day must have all stocks)
            for df, name in [(ft_train, 'train'), (ft_val, 'val')]:
                for day_idx in df['day'].unique():
                    day_data = df[df['day'] == day_idx]
                    tickers_in_day = set(day_data['tic'].unique())
                    
                    if len(tickers_in_day) != self.stock_dim:
                        missing = set(settings.symbols_list) - tickers_in_day
                        logger.warning(f"⚠️  Day {day_idx} in {name} missing stocks: {missing}")
                        return {
                            'success': False,
                            'reason': f'incomplete_data_missing_{len(missing)}_stocks',
                            'timestamp': datetime.utcnow()
                        }
            
            # Evaluate current model on validation set
            logger.info("📈 Evaluating current model on validation set...")
            current_score = self.evaluate_model(self.current_model, ft_val)
            logger.info(f"   Current model score: {current_score:.2f}")
            
            # Clone CURRENT model for fine-tuning (streaming/online learning)
            logger.info("📋 Cloning current model for incremental fine-tuning...")
            with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp:
                tmp_path = tmp.name
                self.current_model.save(tmp_path)
                model_ft = PPO.load(tmp_path)
            os.remove(tmp_path)
            
            # Fine-tune with low LR for online learning
            logger.info(f"🎓 Fine-tuning for {self.finetune_steps} steps (lr={self.finetune_lr})...")
            logger.info("   Using streaming ML: Building on previous improvements")
            model_ft.learning_rate = self.finetune_lr
            ft_env = self.create_env(ft_train)
            model_ft.set_env(ft_env)
            
            model_ft.learn(
                total_timesteps=self.finetune_steps,
                reset_num_timesteps=False,
                progress_bar=False
            )
            
            # Evaluate fine-tuned model
            logger.info("📊 Evaluating fine-tuned model on validation set...")
            finetuned_score = self.evaluate_model(model_ft, ft_val)
            logger.info(f"   Fine-tuned score: {finetuned_score:.2f}")
            
            # Decision: accept if improvement ≥ threshold
            improvement_pct = ((finetuned_score - current_score) / current_score * 100) if current_score != 0 else 0
            accepted = improvement_pct >= self.min_improvement_threshold
            
            if accepted:
                logger.info(f"✅ ACCEPTED - Improvement: {improvement_pct:+.2f}%")
                logger.info("   Updating current model (incremental learning)")
                # Update current model to fine-tuned version (continuous improvement)
                self.current_model = model_ft
                # Save fine-tuned model
                self.current_model.save(self.finetuned_model_path)
                logger.info(f"💾 Saved fine-tuned model: {self.finetuned_model_path}")
            else:
                logger.info(f"❌ REJECTED - Improvement {improvement_pct:+.2f}% < threshold {self.min_improvement_threshold}%")
                logger.info("   Keeping current model (no degradation)")
                # Keep current model unchanged (don't accept worse performance)
            
            result = {
                'success': True,
                'accepted': accepted,
                'current_score': float(current_score),
                'finetuned_score': float(finetuned_score),
                'improvement_pct': float(improvement_pct),
                'threshold': float(self.min_improvement_threshold),
                'train_days': len(ft_train_dates),
                'val_days': len(ft_val_dates),
                'timestamp': datetime.utcnow()
            }
            
            logger.info("=" * 80)
            
            return result
        
        except Exception as e:
            logger.error(f"❌ Fine-tuning failed: {e}", exc_info=True)
            return {
                'success': False,
                'reason': str(e),
                'timestamp': datetime.utcnow()
            }
    
    async def start_service(self):
        """Start the fine-tuning service (background task)."""
        logger.info("🚀 Starting Fine-tuning Service")
        
        # Ensure MongoDB is connected
        if self.mongo_sync and not self.mongo_sync.collection:
            logger.info("📂 Connecting to MongoDB...")
            await self.mongo_sync.connect()
        
        # Load base model
        if not self.load_base_model():
            logger.error("❌ Cannot start without base model")
            return
        
        # No initial wait - MongoDB already has historical data from ensure_historical_data.py
        logger.info("💾 Using historical data from MongoDB (no initial wait required)")
        logger.info(f"⏰ Fine-tuning will run every {self.finetune_interval}h")
        
        while True:
            try:
                # Fine-tune
                result = await self.finetune_model()
                
                self.last_finetune_time = datetime.utcnow()
                
                # Wait for next cycle
                logger.info(f"⏰ Next fine-tune in {self.finetune_interval}h...")
                await asyncio.sleep(self.finetune_interval * 3600)
            
            except Exception as e:
                logger.error(f"❌ Service error: {e}", exc_info=True)
                await asyncio.sleep(600)  # Wait 10 minutes on error


def create_finetuning_service(mongo_sync_service: MongoDBSyncService, **kwargs):
    """Create fine-tuning service instance."""
    return FineTuningService(mongo_sync_service=mongo_sync_service, **kwargs)
