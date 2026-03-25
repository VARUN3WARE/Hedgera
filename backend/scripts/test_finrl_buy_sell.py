#!/usr/bin/env python3
"""
Enhanced FinRL Test with Buy AND Sell Signals
Generates diverse market conditions to trigger both buy and sell decisions
"""

import asyncio
import sys
import json
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import logging

# Add backend and root to path
backend_path = Path(__file__).parent.parent
root_path = backend_path.parent
sys.path.insert(0, str(backend_path))
sys.path.insert(0, str(root_path))

from src.services.finrl_integrated_service import FinRLIntegratedService
from config.settings import settings

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Get all symbols from settings
ALL_SYMBOLS = settings.symbols_list  # Should include 30 DOW + VIXY


def generate_diverse_market_data(tickers, num_rows_per_ticker=100):
    """
    Generate diverse market data with different conditions to trigger both buy AND sell signals.
    
    Strategy:
    - Some tickers: Uptrend (high RSI, positive MACD) → likely BUY
    - Some tickers: Downtrend (low RSI, negative MACD) → likely SELL
    - Some tickers: Neutral/mixed signals
    """
    all_data = []
    
    base_date = datetime.now() - timedelta(days=num_rows_per_ticker)
    
    # Divide tickers into 3 groups for diverse signals
    num_tickers = len([t for t in tickers if t != 'VIXY'])
    uptrend_tickers = [t for t in tickers[:num_tickers//3] if t != 'VIXY']
    downtrend_tickers = [t for t in tickers[num_tickers//3:2*num_tickers//3] if t != 'VIXY']
    neutral_tickers = [t for t in tickers[2*num_tickers//3:] if t != 'VIXY']
    
    logger.info(f"\n📊 Market Condition Distribution:")
    logger.info(f"   Uptrend (BUY candidates): {len(uptrend_tickers)} tickers")
    logger.info(f"   Downtrend (SELL candidates): {len(downtrend_tickers)} tickers")
    logger.info(f"   Neutral: {len(neutral_tickers)} tickers")
    logger.info(f"   VIXY: 1 ticker\n")
    
    for ticker in tickers:
        logger.info(f"Generating data for {ticker}")
        
        # Determine market condition
        if ticker == 'VIXY':
            # Low VIXY = low market volatility (allow trading)
            base_price = np.random.uniform(18, 22)
            trend = 'stable'
        elif ticker in uptrend_tickers:
            base_price = np.random.uniform(100, 200)
            trend = 'uptrend'
        elif ticker in downtrend_tickers:
            base_price = np.random.uniform(100, 200)
            trend = 'downtrend'
        else:
            base_price = np.random.uniform(100, 200)
            trend = 'neutral'
        
        for i in range(num_rows_per_ticker):
            current_date = base_date + timedelta(days=i)
            
            # Price movement based on trend
            if trend == 'uptrend':
                # Gradual upward movement
                price_change = np.random.normal(0.5, 1.0)  # Positive bias
            elif trend == 'downtrend':
                # Gradual downward movement
                price_change = np.random.normal(-0.5, 1.0)  # Negative bias
            elif trend == 'stable':
                # VIXY - minimal movement
                price_change = np.random.normal(0, 0.5)
            else:
                # Neutral - random walk
                price_change = np.random.normal(0, 1.5)
            
            close_price = base_price + price_change
            open_price = close_price * (1 + np.random.normal(0, 0.01))
            high_price = max(open_price, close_price) * (1 + abs(np.random.normal(0, 0.02)))
            low_price = min(open_price, close_price) * (1 - abs(np.random.normal(0, 0.02)))
            
            # Volume
            volume = np.random.randint(1_000_000, 50_000_000)
            
            # Technical Indicators based on trend
            if trend == 'uptrend':
                # Bullish indicators
                macd = abs(np.random.normal(2, 1))  # Positive MACD
                rsi_30 = np.random.uniform(60, 85)  # High RSI (overbought)
                cci_30 = np.random.normal(50, 50)  # Positive CCI
                dx_30 = np.random.uniform(25, 40)  # Strong trend
                close_30_sma = close_price * 0.95  # Price above MA
                close_60_sma = close_price * 0.93
            elif trend == 'downtrend':
                # Bearish indicators
                macd = -abs(np.random.normal(2, 1))  # Negative MACD
                rsi_30 = np.random.uniform(15, 40)  # Low RSI (oversold)
                cci_30 = np.random.normal(-50, 50)  # Negative CCI
                dx_30 = np.random.uniform(25, 40)  # Strong trend
                close_30_sma = close_price * 1.05  # Price below MA
                close_60_sma = close_price * 1.07
            else:
                # Neutral indicators
                macd = np.random.normal(0, 1.5)
                rsi_30 = np.random.uniform(40, 60)  # Neutral RSI
                cci_30 = np.random.normal(0, 75)
                dx_30 = np.random.uniform(10, 30)  # Weak trend
                close_30_sma = close_price * (1 + np.random.normal(0, 0.02))
                close_60_sma = close_price * (1 + np.random.normal(0, 0.03))
            
            boll_ub = close_price * 1.02
            boll_lb = close_price * 0.98
            
            # Turbulence - keep low for normal trading
            turbulence = abs(np.random.normal(15, 5))
            
            row = {
                'date': current_date.strftime('%Y-%m-%d %H:%M:%S'),
                'tic': ticker,
                'open': round(open_price, 2),
                'high': round(high_price, 2),
                'low': round(low_price, 2),
                'close': round(close_price, 2),
                'volume': volume,
                'macd': round(macd, 4),
                'boll_ub': round(boll_ub, 4),
                'boll_lb': round(boll_lb, 4),
                'rsi_30': round(rsi_30, 4),
                'cci_30': round(cci_30, 4),
                'dx_30': round(dx_30, 4),
                'close_30_sma': round(close_30_sma, 4),
                'close_60_sma': round(close_60_sma, 4),
                'turbulence': round(turbulence, 4)
            }
            
            all_data.append(row)
            base_price = close_price  # Update base for next iteration
    
    df = pd.DataFrame(all_data)
    logger.info(f"\n✅ Generated {len(df)} rows for {len(tickers)} tickers")
    logger.info(f"   Columns: {df.columns.tolist()}")
    logger.info(f"   Shape: {df.shape}")
    
    # Verify data integrity
    logger.info("\n=== Data Validation ===")
    logger.info(f"Unique tickers: {df['tic'].nunique()}")
    logger.info(f"Date range: {df['date'].min()} to {df['date'].max()}")
    
    # Show sample from each trend type
    logger.info("\n=== Sample Data by Trend ===")
    if uptrend_tickers:
        sample_ticker = uptrend_tickers[0]
        logger.info(f"Uptrend ticker ({sample_ticker}):")
        logger.info(df[df['tic'] == sample_ticker][['date', 'close', 'macd', 'rsi_30', 'cci_30']].head(3).to_string())
    
    if downtrend_tickers:
        sample_ticker = downtrend_tickers[0]
        logger.info(f"\nDowntrend ticker ({sample_ticker}):")
        logger.info(df[df['tic'] == sample_ticker][['date', 'close', 'macd', 'rsi_30', 'cci_30']].head(3).to_string())
    
    return df, uptrend_tickers, downtrend_tickers, neutral_tickers


async def test_finrl_buy_sell_signals():
    """Test FinRL with diverse market conditions to get both buy AND sell signals"""
    
    print("\n" + "="*80)
    print("🧪 FINRL TEST - BUY AND SELL SIGNALS")
    print("="*80)
    
    # Get all symbols from .env
    all_tickers = ALL_SYMBOLS
    logger.info(f"\n📊 Using tickers from .env: {len(all_tickers)} total")
    logger.info(f"   Tickers: {all_tickers[:10]}... (showing first 10)")
    
    # Generate diverse market data
    df, uptrend, downtrend, neutral = generate_diverse_market_data(all_tickers, num_rows_per_ticker=100)
    
    # Save to file for inspection
    output_file = Path(__file__).parent / 'dummy_data_buy_sell.csv'
    df.to_csv(output_file, index=False)
    logger.info(f"\n✅ Dummy data saved to: {output_file}")
    
    # Save trend categories
    categories_file = Path(__file__).parent / 'ticker_categories.json'
    categories = {
        'uptrend_tickers': uptrend,
        'downtrend_tickers': downtrend,
        'neutral_tickers': neutral
    }
    with open(categories_file, 'w') as f:
        json.dump(categories, f, indent=2)
    logger.info(f"✅ Ticker categories saved to: {categories_file}")
    
    # Initialize FinRL Service
    logger.info("\n🤖 Initializing FinRL Service...")
    finrl_service = FinRLIntegratedService()
    
    # Test the service
    logger.info("\n🚀 Running FinRL model with diverse market data...")
    logger.info("-"*80)
    
    try:
        # Call the run_finrl_model method (synchronous)
        result = finrl_service.run_finrl_model(df)
        
        print("\n" + "="*80)
        if 'error' in result:
            print("❌ FINRL EXECUTION FAILED")
            print("="*80)
            logger.error(f"Error from FinRL: {result['error']}")
            raise Exception(result['error'])
        else:
            print("✅ FINRL EXECUTION SUCCESSFUL")
            print("="*80)
        
        # Display results
        logger.info(f"\n📈 FinRL Results:")
        logger.info(f"Timestamp: {result['timestamp']}")
        logger.info(f"Total analyzed: {result['total_analyzed']}")
        
        selected_tickers = result.get('selected_tickers', [])
        buy_decisions = result.get('buy_decisions', {})
        sell_decisions = result.get('sell_decisions', {})
        
        logger.info("\n" + "="*80)
        logger.info("=== DECISION SUMMARY ===")
        logger.info("="*80)
        logger.info(f"✅ Buy signals: {len(buy_decisions)} tickers")
        logger.info(f"✅ Sell signals: {len(sell_decisions)} tickers")
        logger.info(f"✅ Total decisions: {len(buy_decisions) + len(sell_decisions)}")
        logger.info(f"✅ Selected top {len(selected_tickers)} tickers")
        
        if buy_decisions:
            logger.info(f"\n📈 BUY DECISIONS ({len(buy_decisions)} tickers):")
            for ticker, shares in sorted(buy_decisions.items()):
                trend_type = "UPTREND" if ticker in uptrend else "NEUTRAL/OTHER"
                logger.info(f"   {ticker:6s} - BUY {shares:3d} shares [{trend_type}]")
        
        if sell_decisions:
            logger.info(f"\n📉 SELL DECISIONS ({len(sell_decisions)} tickers):")
            for ticker, shares in sorted(sell_decisions.items()):
                trend_type = "DOWNTREND" if ticker in downtrend else "NEUTRAL/OTHER"
                logger.info(f"   {ticker:6s} - SELL {shares:3d} shares [{trend_type}]")
        
        if selected_tickers:
            logger.info(f"\n⭐ TOP {len(selected_tickers)} SELECTED TICKERS:")
            for idx, ticker in enumerate(selected_tickers, 1):
                action = "BUY" if ticker in buy_decisions else "SELL"
                shares = buy_decisions.get(ticker) or sell_decisions.get(ticker, 0)
                logger.info(f"   {idx:2d}. {ticker:6s} - {action:4s} {shares:3d} shares")
            
            # Save decisions to file
            decisions_file = Path(__file__).parent / 'finrl_buy_sell_output.json'
            output_data = {
                **result,
                'ticker_categories': categories
            }
            with open(decisions_file, 'w') as f:
                json.dump(output_data, f, indent=2)
            logger.info(f"\n✅ Decisions saved to: {decisions_file}")
            
        else:
            logger.warning("⚠️  No trading decisions generated!")
            logger.warning("This might indicate:")
            logger.warning("  - Model not producing actions")
            logger.warning("  - Turbulence threshold too restrictive")
            logger.warning("  - stocks_cd filtering all tickers")
            logger.warning(f"\nBuy decisions: {buy_decisions}")
            logger.warning(f"Sell decisions: {sell_decisions}")
        
        # Analysis
        logger.info("\n" + "="*80)
        logger.info("=== PERFORMANCE ANALYSIS ===")
        logger.info("="*80)
        
        buy_from_uptrend = sum(1 for t in buy_decisions.keys() if t in uptrend)
        sell_from_downtrend = sum(1 for t in sell_decisions.keys() if t in downtrend)
        
        logger.info(f"Buy signals from uptrend tickers: {buy_from_uptrend}/{len(buy_decisions)}")
        logger.info(f"Sell signals from downtrend tickers: {sell_from_downtrend}/{len(sell_decisions)}")
        
        if len(buy_decisions) > 0:
            buy_accuracy = (buy_from_uptrend / len(buy_decisions)) * 100
            logger.info(f"Buy accuracy (uptrend detection): {buy_accuracy:.1f}%")
        
        if len(sell_decisions) > 0:
            sell_accuracy = (sell_from_downtrend / len(sell_decisions)) * 100
            logger.info(f"Sell accuracy (downtrend detection): {sell_accuracy:.1f}%")
        
    except Exception as e:
        print("\n" + "="*80)
        print("❌ FINRL EXECUTION FAILED")
        print("="*80)
        logger.error(f"Error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        
        # Debug information
        logger.info("\n=== DEBUG INFORMATION ===")
        logger.info(f"Input data shape: {df.shape}")
        logger.info(f"Input columns: {df.columns.tolist()}")
        logger.info(f"Tickers in data: {df['tic'].unique().tolist()}")
        
        raise


if __name__ == "__main__":
    print("\n" + "="*80)
    print("🎯 FinRL Buy & Sell Signal Test")
    print("="*80)
    print("\nThis test will:")
    print("1. Generate diverse market data (uptrend, downtrend, neutral)")
    print("2. Feed data to FinRL model")
    print("3. Observe BOTH buy AND sell decisions")
    print("4. Analyze model's ability to detect trends")
    print("5. Output top tickers with detailed actions")
    print("\n" + "="*80 + "\n")
    
    asyncio.run(test_finrl_buy_sell_signals())
