#!/usr/bin/env python3
"""
Complete FinRL Test with Dummy Data
Tests the full FinRL pipeline with 30 DOW tickers + VIXY
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

# DOW 30 Tickers
DOW_30_TICKERS = [
    'AAPL', 'AMGN', 'AMZN', 'AXP', 'BA', 'CAT', 'CRM', 'CSCO', 'CVX', 'DIS',
    'DOW', 'GS', 'HD', 'HON', 'IBM', 'INTC', 'JNJ', 'JPM', 'KO', 'MCD',
    'MMM', 'MRK', 'MSFT', 'NKE', 'NVDA', 'PG', 'UNH', 'V', 'VZ', 'WMT'
]

def generate_realistic_dummy_data(tickers, num_rows_per_ticker=100):
    """Generate realistic dummy data for all tickers"""
    all_data = []
    
    base_date = datetime.now() - timedelta(days=num_rows_per_ticker)
    
    for ticker in tickers:
        logger.info(f"Generating data for {ticker}")
        
        # Base price varies by ticker
        # For VIXY (volatility index), use lower values (10-30 range) to keep turbulence low
        if ticker == 'VIXY':
            base_price = np.random.uniform(15, 25)  # Low VIXY = low market turbulence
        else:
            base_price = np.random.uniform(50, 300)
        
        for i in range(num_rows_per_ticker):
            current_date = base_date + timedelta(days=i)
            
            # Price with realistic movement
            price_change = np.random.normal(0, 2)
            close_price = base_price + price_change
            open_price = close_price * (1 + np.random.normal(0, 0.01))
            high_price = max(open_price, close_price) * (1 + abs(np.random.normal(0, 0.02)))
            low_price = min(open_price, close_price) * (1 - abs(np.random.normal(0, 0.02)))
            
            # Volume
            volume = np.random.randint(1_000_000, 50_000_000)
            
            # Technical Indicators (realistic values)
            macd = np.random.normal(0, 2)
            boll_ub = close_price * 1.02
            boll_lb = close_price * 0.98
            rsi_30 = np.random.uniform(20, 80)
            cci_30 = np.random.normal(0, 100)
            dx_30 = np.random.uniform(10, 40)
            close_30_sma = close_price * (1 + np.random.normal(0, 0.01))
            close_60_sma = close_price * (1 + np.random.normal(0, 0.02))
            
            # Turbulence (important for FinRL - keep it low to allow trading)
            # Default threshold in FinRL is 35, so keep values below that for normal trading
            turbulence = abs(np.random.normal(15, 5))  # Low turbulence for normal trading
            
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
    logger.info(f"Generated {len(df)} rows for {len(tickers)} tickers")
    logger.info(f"Columns: {df.columns.tolist()}")
    logger.info(f"Shape: {df.shape}")
    
    # Verify data integrity
    logger.info("\n=== Data Validation ===")
    logger.info(f"Unique tickers: {df['tic'].nunique()}")
    logger.info(f"Date range: {df['date'].min()} to {df['date'].max()}")
    logger.info(f"Sample data for AAPL:")
    logger.info(df[df['tic'] == 'AAPL'].head(3).to_string())
    
    return df

async def test_finrl_with_dummy_data():
    """Test FinRL service with dummy data"""
    
    print("\n" + "="*80)
    print("🧪 FINRL COMPLETE TEST WITH DUMMY DATA")
    print("="*80)
    
    # Generate dummy data for 30 DOW + VIXY
    all_tickers = DOW_30_TICKERS + ['VIXY']
    logger.info(f"\n📊 Generating dummy data for {len(all_tickers)} tickers...")
    logger.info(f"Trading tickers (30): {DOW_30_TICKERS[:5]}... (showing first 5)")
    logger.info(f"Volatility ticker: VIXY")
    
    df = generate_realistic_dummy_data(all_tickers, num_rows_per_ticker=100)
    
    # Save to file for inspection
    output_file = Path(__file__).parent / 'dummy_data_generated.csv'
    df.to_csv(output_file, index=False)
    logger.info(f"✅ Dummy data saved to: {output_file}")
    
    # Initialize FinRL Service
    logger.info("\n🤖 Initializing FinRL Service...")
    finrl_service = FinRLIntegratedService()
    
    # Test the service
    logger.info("\n🚀 Running FinRL model with dummy data...")
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
        
        if selected_tickers:
            logger.info(f"\n=== TOP {len(selected_tickers)} SELECTED TICKERS ===")
            for idx, ticker in enumerate(selected_tickers, 1):
                logger.info(f"\n{idx}. {ticker}")
                if ticker in buy_decisions:
                    logger.info(f"   Action: BUY")
                    logger.info(f"   Shares: {buy_decisions[ticker]}")
                elif ticker in sell_decisions:
                    logger.info(f"   Action: SELL")
                    logger.info(f"   Shares: {sell_decisions[ticker]}")
            
            # Save decisions to file
            decisions_file = Path(__file__).parent / 'finrl_decisions_output.json'
            with open(decisions_file, 'w') as f:
                json.dump(result, f, indent=2)
            logger.info(f"\n✅ Decisions saved to: {decisions_file}")
            
            # Summary statistics
            logger.info("\n=== DECISION SUMMARY ===")
            logger.info(f"Buy signals: {len(buy_decisions)} tickers")
            logger.info(f"Sell signals: {len(sell_decisions)} tickers")
            logger.info(f"Selected tickers: {len(selected_tickers)}")
            logger.info(f"Total analyzed: {result['total_analyzed']}")
            
        else:
            logger.warning("⚠️  No trading decisions generated!")
            logger.warning("This might indicate:")
            logger.warning("  - Model not producing actions")
            logger.warning("  - Turbulence threshold too restrictive")
            logger.warning("  - stocks_cd filtering all tickers")
            logger.warning(f"\nBuy decisions: {buy_decisions}")
            logger.warning(f"Sell decisions: {sell_decisions}")
        
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
    print("🎯 Starting FinRL Complete Test")
    print("="*80)
    print("\nThis test will:")
    print("1. Generate realistic dummy data for 30 DOW tickers + VIXY")
    print("2. Feed data to FinRL model")
    print("3. Observe model predictions and trading decisions")
    print("4. Output top 10 tickers with detailed information")
    print("5. Save results for inspection")
    print("\n" + "="*80 + "\n")
    
    asyncio.run(test_finrl_with_dummy_data())
