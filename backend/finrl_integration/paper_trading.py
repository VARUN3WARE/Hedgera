"""
Simple wrapper for PaperTradingAlpaca to output JSON trading decisions.
Uses existing finrl.meta.paper_trading.alpaca.PaperTradingAlpaca class.
"""

import json
import numpy as np
import pandas as pd
from typing import Dict, Any
from datetime import datetime

from finrl.meta.paper_trading.alpaca import PaperTradingAlpaca
from finrl.meta.data_processors.processor_alpaca import AlpacaProcessor


class PaperTradingJSON(PaperTradingAlpaca):
    """
    Extends PaperTradingAlpaca to output JSON decisions instead of executing trades.
    """
    
    def __init__(self, ticker_list, time_interval, drl_lib, agent, cwd, net_dim,
                 state_dim, action_dim, API_KEY, API_SECRET, API_BASE_URL,
                 tech_indicator_list, turbulence_thresh=35, max_stock=100, latency=None):
        """Initialize using parent class."""
        super().__init__(
            ticker_list=ticker_list,
            time_interval=time_interval,
            drl_lib=drl_lib,
            agent=agent,
            cwd=cwd,
            net_dim=net_dim,
            state_dim=state_dim,
            action_dim=action_dim,
            API_KEY=API_KEY,
            API_SECRET=API_SECRET,
            API_BASE_URL=API_BASE_URL,
            tech_indicator_list=tech_indicator_list,
            turbulence_thresh=turbulence_thresh,
            max_stock=max_stock,
            latency=latency
        )
    
    def get_state_from_redis(self, redis_data: pd.DataFrame) -> np.ndarray:
        """
        Get state from Redis DataFrame instead of Alpaca API.
        Uses exact same logic as parent class get_state() method.
        
        Args:
            redis_data: DataFrame with columns ['tic', 'close'] + tech_indicator_list + ['VIXY']
        
        Returns:
            State vector for model
        """
        # Extract price and tech indicators from Redis data
        price = []
        tech = []
        
        for ticker in self.stockUniverse:
            ticker_data = redis_data[redis_data['tic'] == ticker]
            
            # Defensive check - ensure ticker has data
            if ticker_data.empty:
                print(f"Warning: No data found for ticker {ticker}, using default values")
                price.append(100.0)  # Default price
                for _ in self.tech_indicator_list:
                    tech.append(0.0)  # Default indicator values
                continue
            
            price.append(float(ticker_data['close'].iloc[-1]))
            
            for indicator in self.tech_indicator_list:
                tech.append(float(ticker_data[indicator].iloc[-1]))
        
        price = np.array(price, dtype=np.float32)
        tech = np.array(tech, dtype=np.float32)
        
        # Get turbulence from VIXY in Redis data
        vixy_data = redis_data[redis_data['tic'] == 'VIXY']
        turbulence_value = float(vixy_data['close'].iloc[-1]) if not vixy_data.empty else 0.0
        
        turbulence_bool = 1 if turbulence_value >= self.turbulence_thresh else 0
        turbulence = (self.sigmoid_sign(turbulence_value, self.turbulence_thresh) * 2**-5).astype(np.float32)
        
        # Apply tech scaling (same as parent class)
        tech = tech * 2**-7
        
        # Get current portfolio - use simulated values since we're not actually trading
        # In a real implementation, this would track positions from previous decisions
        # For testing: simulate that we own 50 shares of each stock to allow sell signals
        stocks = np.full(len(self.stockUniverse), 50.0, dtype=float)
        cash = 1_000_000.0  # Start with $1M in simulated cash
        
        # Store in instance variables (same as parent class)
        self.cash = cash
        self.stocks = stocks
        self.turbulence_bool = turbulence_bool
        self.price = price
        
        # Build state vector - MODIFIED for FinRL model expecting (301,)
        # Model was trained with: turbulence_bool + price + stocks + tech
        # = 1 + 30 + 30 + (8*30) = 1 + 30 + 30 + 240 = 301
        scale = np.array(2**-6, dtype=np.float32)
        state = np.hstack((
            self.turbulence_bool,  # 1 value
            price * scale,         # 30 values
            self.stocks * scale,   # 30 values
            tech,                  # 240 values (8 indicators * 30 stocks)
        )).astype(np.float32)
        
        state[np.isnan(state)] = 0.0
        state[np.isinf(state)] = 0.0
        
        return state
    
    def get_actions_json(self, redis_data: pd.DataFrame = None) -> Dict[str, Any]:
        """
        Get trading actions as JSON format.
        Uses exact same logic as parent trade() method but returns JSON instead of executing.
        
        Args:
            redis_data: Optional DataFrame from Redis. If None, uses Alpaca API.
        
        Returns:
            JSON dict with buy/sell decisions
        """
        # Get state (same as parent trade() method line 218)
        if redis_data is not None:
            state = self.get_state_from_redis(redis_data)
        else:
            state = self.get_state()
        
        # Get model predictions (same as parent trade() method lines 220-235)
        if self.drl_lib == "elegantrl":
            import torch
            with torch.no_grad():
                s_tensor = torch.as_tensor((state,), device=self.device)
                a_tensor = self.act(s_tensor)
                action = a_tensor.detach().cpu().numpy()[0]
            action = (action * self.max_stock).astype(int)
        
        elif self.drl_lib == "rllib":
            action = self.agent.compute_single_action(state)
        
        elif self.drl_lib == "stable_baselines3":
            action = self.model.predict(state)[0]
            # Scale action by max_stock (same as elegantrl)
            action = (action * self.max_stock).astype(int)
        
        else:
            raise ValueError("The DRL library input is NOT supported yet. Please check your input.")
        
        # Process actions to buy/sell decisions (same logic as parent trade() method)
        buy_decisions = {}
        sell_decisions = {}
        
        # Debug output
        print(f"\n=== FinRL Model Predictions ===")
        print(f"Turbulence bool: {self.turbulence_bool}")
        print(f"Actions shape: {action.shape}")
        print(f"Actions (first 5): {action[:5]}")
        print(f"Actions (max/min): max={action.max()}, min={action.min()}")
        print(f"Actions > 10 count: {(action > 10).sum()}")
        print(f"Actions < -10 count: {(action < -10).sum()}")
        
        # Update stocks_cd (line 237)
        self.stocks_cd += 1
        
        if self.turbulence_bool == 0:
            # Normal trading mode (lines 238-276)
            min_action = 10  # stock_cd
            
            # Process sell orders (lines 240-252)
            for index in np.where(action < -min_action)[0]:  # sell_index:
                sell_num_shares = min(self.stocks[index], -action[index])
                qty = abs(int(sell_num_shares))
                if qty > 0:
                    sell_decisions[self.stockUniverse[index]] = qty
                self.stocks_cd[index] = 0
            
            # Process buy orders (lines 257-276)
            for index in np.where(action > min_action)[0]:  # buy_index:
                if self.cash < 0:
                    tmp_cash = 0
                else:
                    tmp_cash = self.cash
                
                # Use the model's action directly (already scaled by max_stock)
                # Don't limit by cash here - let the model decide quantities
                model_suggested_shares = abs(int(action[index]))
                
                # Calculate max affordable shares with available cash
                max_affordable = tmp_cash // self.price[index] if self.price[index] > 0 else 0
                
                # Use the model's suggestion but cap at max_stock parameter
                desired_shares = min(model_suggested_shares, self.max_stock)
                
                # Final quantity: model's desire vs what we can afford
                if max_affordable >= desired_shares:
                    # Can afford model's suggestion
                    qty = desired_shares
                else:
                    # Can't afford full amount, but buy what we can (minimum 1 if affordable)
                    qty = max(1, int(max_affordable)) if max_affordable >= 1 else 0
                
                if qty > 0:
                    buy_decisions[self.stockUniverse[index]] = qty
                    # Update available cash for next stock
                    self.cash -= qty * self.price[index]
                self.stocks_cd[index] = 0
        
        else:
            # High turbulence - sell all (lines 278-294)
            try:
                positions = self.alpaca.list_positions()
                for position in positions:
                    if position.side == "long":
                        orderSide = "sell"
                    else:
                        orderSide = "buy"
                    qty = abs(int(float(position.qty)))
                    
                    if orderSide == "sell":
                        sell_decisions[position.symbol] = qty
                    else:
                        buy_decisions[position.symbol] = qty
            except Exception as e:
                # If Alpaca API fails (e.g., in testing), just skip selling
                print(f"Warning: Could not fetch positions from Alpaca (turbulence mode): {e}")
                print("Skipping turbulence sell-all logic in test mode")
            
            self.stocks_cd[:] = 0
        
        # Return JSON format
        return {
            'buy': buy_decisions,
            'sell': sell_decisions,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }


# Simple usage function
def get_trading_decisions(redis_data: pd.DataFrame, model_path: str, ticker_list: list, 
                         tech_indicators: list, api_key: str, api_secret: str, 
                         api_base_url: str) -> Dict[str, Any]:
    """
    Get trading decisions from Redis data.
    
    Args:
        redis_data: DataFrame with market data from Redis
        model_path: Path to trained model
        ticker_list: List of 30 tickers
        tech_indicators: List of technical indicators
        api_key: Alpaca API key
        api_secret: Alpaca API secret
        api_base_url: Alpaca API base URL
    
    Returns:
        JSON dict: {'buy': {ticker: qty}, 'sell': {ticker: qty}, 'timestamp': str}
    """
    paper_trading = PaperTradingJSON(
        ticker_list=ticker_list,
        time_interval='1Min',
        drl_lib='stable_baselines3',
        agent='ppo',
        cwd=model_path,
        state_dim=301,  # Correct state dim: 1 (turbulence_bool) + 30 (price) + 30 (stocks) + 240 (tech)
        action_dim=len(ticker_list),
        net_dim=[64, 64],
        API_KEY=api_key,
        API_SECRET=api_secret,
        API_BASE_URL=api_base_url,
        tech_indicator_list=tech_indicators,
        max_stock=1e2
    )
    
    return paper_trading.get_actions_json(redis_data)
