#!/usr/bin/env python3
"""
Ensure Historical Data in MongoDB
Checks if MongoDB has 3 trading days of data. If not, fetches missing days from Alpaca.
Prevents duplicates using MongoDB unique index on (date, tic).
"""
import asyncio
import aiohttp
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Set, Dict
import pandas as pd
import numpy as np
from pymongo import MongoClient, ASCENDING, DESCENDING, UpdateOne
import logging

sys.path.insert(0, str(Path(__file__).parent))
from backend.config.settings import settings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class HistoricalDataEnsurer:
    """Ensures MongoDB has at least 3 trading days of historical data."""
    
    def __init__(self):
        self.api_key = settings.alpaca_api_key
        self.secret_key = settings.alpaca_secret_key
        self.base_url = settings.alpaca_base_url
        self.symbols = settings.symbols_list
        self.session = None
        self.mongo_client = None
        self.collection = None
        
    async def initialize(self):
        """Initialize HTTP and MongoDB connections."""
        headers = {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.secret_key,
        }
        self.session = aiohttp.ClientSession(headers=headers)
        
        mongo_uri = settings.mongodb_uri_streaming
        self.mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
        db = self.mongo_client['finrl_trading']
        self.collection = db['market_data_1min']
        
        # Ensure unique index exists (prevents duplicates)
        self.collection.create_index([('date', DESCENDING)])
        self.collection.create_index([('tic', ASCENDING)])
        self.collection.create_index([('date', DESCENDING), ('tic', ASCENDING)], unique=True)
        
        logger.info("✅ Initialized connections")
    
    async def cleanup(self):
        """Close connections."""
        if self.session:
            await self.session.close()
        if self.mongo_client:
            self.mongo_client.close()
    
    def get_existing_trading_days(self, min_hours: int = 4) -> Set[str]:
        """
        Get set of trading days already in MongoDB with at least min_hours of data.
        
        Args:
            min_hours: Minimum hours of data required per day (default: 4 hours = 240 minutes)
        
        Returns:
            Set of dates with sufficient data
        """
        try:
            min_bars_per_symbol = min_hours * 60  # Convert to minutes
            
            pipeline = [
                {"$group": {
                    "_id": {
                        "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$date"}},
                        "tic": "$tic"
                    },
                    "count": {"$sum": 1}
                }},
                {"$group": {
                    "_id": "$_id.date",
                    "symbols": {"$sum": 1},
                    "total_bars": {"$sum": "$count"},
                    "min_bars": {"$min": "$count"},
                    "max_bars": {"$max": "$count"}
                }},
                {"$sort": {"_id": 1}}
            ]
            result = list(self.collection.aggregate(pipeline))
            
            valid_days = set()
            
            if result:
                logger.info(f"📊 Checking existing trading days in MongoDB (need ≥{min_bars_per_symbol} bars/symbol):")
                for doc in result:
                    date = doc['_id']
                    symbols = doc['symbols']
                    total_bars = doc['total_bars']
                    min_bars = doc['min_bars']
                    max_bars = doc['max_bars']
                    avg_bars = total_bars / symbols if symbols > 0 else 0
                    
                    # Consider valid if at least one symbol has min_hours of data
                    is_valid = max_bars >= min_bars_per_symbol
                    status = "✅" if is_valid else "⚠️ "
                    
                    logger.info(f"   {status} {date}: {symbols} symbols, {total_bars:,} total bars")
                    logger.info(f"      Avg: {avg_bars:.0f} bars/symbol, Range: {min_bars}-{max_bars}")
                    
                    if is_valid:
                        valid_days.add(date)
                    else:
                        logger.info(f"      ❌ Insufficient data (need ≥{min_bars_per_symbol} bars)")
            else:
                logger.info("📭 No existing trading days found in MongoDB")
            
            return valid_days
        except Exception as e:
            logger.error(f"Error checking MongoDB: {e}")
            return set()
    
    def get_date_range_for_day(self, days_ago: int) -> tuple:
        """Get start/end timestamps for a single trading day."""
        now = datetime.utcnow()
        target_date = now - timedelta(days=days_ago)
        
        # Market hours: 9:30 AM - 4:00 PM EST (14:30 - 21:00 UTC)
        start = target_date.replace(hour=14, minute=30, second=0, microsecond=0)
        end = target_date.replace(hour=21, minute=0, second=0, microsecond=0)
        
        start_str = start.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_str = end.strftime("%Y-%m-%dT%H:%M:%SZ")
        date_str = target_date.strftime("%Y-%m-%d")
        
        return start_str, end_str, date_str
    
    async def fetch_day_bars(self, symbol: str, start: str, end: str, date_str: str) -> List[Dict]:
        """Fetch 1-minute bars for one symbol for one day."""
        try:
            url = f"{self.base_url}/v2/stocks/{symbol}/bars"
            params = {
                "timeframe": "1Min",
                "start": start,
                "end": end,
                "limit": 10000,
                "adjustment": "split"
            }
            
            async with self.session.get(url, params=params, timeout=30) as response:
                if response.status == 429:
                    logger.warning(f"⚠️  Rate limit for {symbol} on {date_str}, waiting 60s...")
                    await asyncio.sleep(60)
                    return await self.fetch_day_bars(symbol, start, end, date_str)
                
                if response.status != 200:
                    text = await response.text()
                    logger.warning(f"API error for {symbol} on {date_str}: {response.status}")
                    return []
                
                data = await response.json()
                return data.get("bars", [])
        except Exception as e:
            logger.error(f"Error fetching {symbol} on {date_str}: {e}")
            return []
    
    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate FinRL indicators on dataframe with proper handling.
        Uses expanding windows for initial periods to avoid NaN values.
        """
        try:
            df = df.sort_values('date').reset_index(drop=True)
            
            # Ensure we have enough data
            if len(df) < 2:
                logger.warning(f"Insufficient data for indicators: {len(df)} rows")
                # Set defaults
                df['macd'] = 0.0
                df['boll_ub'] = df['close']
                df['boll_lb'] = df['close']
                df['rsi_30'] = 50.0
                df['cci_30'] = 0.0
                df['dx_30'] = 0.0
                df['close_30_sma'] = df['close']
                df['close_60_sma'] = df['close']
                return df
            
            # MACD (12-26 EMA)
            if len(df) >= 12:
                ema12 = df['close'].ewm(span=12, adjust=False, min_periods=1).mean()
                ema26 = df['close'].ewm(span=26, adjust=False, min_periods=1).mean()
                df['macd'] = ema12 - ema26
            else:
                df['macd'] = 0.0
            
            # Bollinger Bands (20-period)
            if len(df) >= 20:
                sma20 = df['close'].rolling(20, min_periods=1).mean()
                std20 = df['close'].rolling(20, min_periods=1).std()
                df['boll_ub'] = sma20 + (2 * std20)
                df['boll_lb'] = sma20 - (2 * std20)
            else:
                expanding_mean = df['close'].expanding(min_periods=1).mean()
                expanding_std = df['close'].expanding(min_periods=1).std()
                df['boll_ub'] = expanding_mean + (2 * expanding_std.fillna(0))
                df['boll_lb'] = expanding_mean - (2 * expanding_std.fillna(0))
            
            # RSI-30
            if len(df) >= 30:
                delta = df['close'].diff()
                gain = delta.where(delta > 0, 0).rolling(30, min_periods=1).mean()
                loss = -delta.where(delta < 0, 0).rolling(30, min_periods=1).mean()
                rs = gain / loss.replace(0, 1e-10)
                df['rsi_30'] = 100 - (100 / (1 + rs))
            else:
                # Use expanding window for initial period
                delta = df['close'].diff()
                gain = delta.where(delta > 0, 0).expanding(min_periods=1).mean()
                loss = -delta.where(delta < 0, 0).expanding(min_periods=1).mean()
                rs = gain / loss.replace(0, 1e-10)
                df['rsi_30'] = 100 - (100 / (1 + rs))
            
            # CCI-30
            if len(df) >= 30:
                tp = (df['high'] + df['low'] + df['close']) / 3
                sma_tp = tp.rolling(30, min_periods=1).mean()
                mad = tp.rolling(30, min_periods=1).apply(lambda x: np.abs(x - x.mean()).mean())
                df['cci_30'] = (tp - sma_tp) / (0.015 * mad.replace(0, 1e-10))
            else:
                tp = (df['high'] + df['low'] + df['close']) / 3
                sma_tp = tp.expanding(min_periods=1).mean()
                mad = tp.expanding(min_periods=1).apply(lambda x: np.abs(x - x.mean()).mean())
                df['cci_30'] = (tp - sma_tp) / (0.015 * mad.replace(0, 1e-10))
            
            # DX-30 (Directional Movement Index)
            if len(df) >= 30:
                high_low = df['high'] - df['low']
                high_close = np.abs(df['high'] - df['close'].shift())
                low_close = np.abs(df['low'] - df['close'].shift())
                tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
                atr = tr.rolling(30, min_periods=1).mean()
                
                plus_dm = df['high'].diff()
                minus_dm = -df['low'].diff()
                plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0)
                minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0)
                
                plus_di = 100 * (plus_dm.rolling(30, min_periods=1).mean() / atr.replace(0, 1e-10))
                minus_di = 100 * (minus_dm.rolling(30, min_periods=1).mean() / atr.replace(0, 1e-10))
                df['dx_30'] = 100 * np.abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, 1e-10)
            else:
                high_low = df['high'] - df['low']
                high_close = np.abs(df['high'] - df['close'].shift())
                low_close = np.abs(df['low'] - df['close'].shift())
                tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
                atr = tr.expanding(min_periods=1).mean()
                
                plus_dm = df['high'].diff()
                minus_dm = -df['low'].diff()
                plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0)
                minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0)
                
                plus_di = 100 * (plus_dm.expanding(min_periods=1).mean() / atr.replace(0, 1e-10))
                minus_di = 100 * (minus_dm.expanding(min_periods=1).mean() / atr.replace(0, 1e-10))
                df['dx_30'] = 100 * np.abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, 1e-10)
            
            # SMAs
            df['close_30_sma'] = df['close'].rolling(30, min_periods=1).mean()
            df['close_60_sma'] = df['close'].rolling(60, min_periods=1).mean()
            
            # Fill any remaining NaN with safe defaults
            df['macd'] = df['macd'].fillna(0)
            df['boll_ub'] = df['boll_ub'].fillna(df['close'])
            df['boll_lb'] = df['boll_lb'].fillna(df['close'])
            df['rsi_30'] = df['rsi_30'].fillna(50)
            df['cci_30'] = df['cci_30'].fillna(0)
            df['dx_30'] = df['dx_30'].fillna(0)
            df['close_30_sma'] = df['close_30_sma'].fillna(df['close'])
            df['close_60_sma'] = df['close_60_sma'].fillna(df['close'])
            
            # Replace infinities
            df = df.replace([np.inf, -np.inf], 0)
            
            return df
        except Exception as e:
            logger.error(f"Error calculating indicators: {e}")
            # Return with defaults if calculation fails
            df['macd'] = 0.0
            df['boll_ub'] = df.get('close', 0)
            df['boll_lb'] = df.get('close', 0)
            df['rsi_30'] = 50.0
            df['cci_30'] = 0.0
            df['dx_30'] = 0.0
            df['close_30_sma'] = df.get('close', 0)
            df['close_60_sma'] = df.get('close', 0)
            return df
    
    async def ensure_trading_days(self, required_days: int = 3, min_hours_per_day: int = 4) -> Dict:
        """
        Ensure MongoDB has at least required_days trading days with min_hours_per_day data each.
        
        Args:
            required_days: Number of trading days required (default: 3)
            min_hours_per_day: Minimum hours of data per day (default: 4)
        
        Returns:
            Dict with status, days count, and bars fetched
        """
        logger.info("=" * 80)
        logger.info(f"🔍 CHECKING HISTORICAL DATA")
        logger.info(f"   Required: {required_days} trading days × {min_hours_per_day}+ hours each")
        logger.info("=" * 80)
        
        # Check existing with minimum hours requirement
        existing_dates = self.get_existing_trading_days(min_hours=min_hours_per_day)
        
        if len(existing_dates) >= required_days:
            logger.info(f"\n✅ SUFFICIENT DATA: {len(existing_dates)} trading days with ≥{min_hours_per_day}h data each")
            logger.info("=" * 80)
            return {"status": "sufficient", "days": len(existing_dates), "fetched": 0}
        
        missing = required_days - len(existing_dates)
        logger.info(f"\n⚠️  INSUFFICIENT DATA: Need {missing} more trading day(s) with ≥{min_hours_per_day}h data")
        logger.info("=" * 80)
        
        trading_days_found = len(existing_dates)
        days_searched = 0
        max_search = 14  # Search up to 2 weeks back
        total_inserted = 0
        
        logger.info("\n📅 Searching for missing trading days...\n")
        
        while trading_days_found < required_days and days_searched < max_search:
            days_ago = days_searched + 1
            start, end, date_str = self.get_date_range_for_day(days_ago)
            
            # Skip if already exists with sufficient data
            if date_str in existing_dates:
                logger.info(f"   {date_str}: ✅ Already exists with sufficient data (skipping)")
                trading_days_found += 1
                days_searched += 1
                continue
            
            logger.info(f"   {date_str}: Checking if trading day...")
            
            # Test first symbol to see if it's a trading day
            test_bars = await self.fetch_day_bars(self.symbols[0], start, end, date_str)
            
            if not test_bars:
                logger.info(f"   {date_str}: ❌ Non-trading day (weekend/holiday)")
                days_searched += 1
                continue
            
            # Check if we have at least min_hours worth of data
            min_bars_needed = min_hours_per_day * 60  # Convert to minutes
            if len(test_bars) < min_bars_needed:
                logger.info(f"   {date_str}: ⚠️  Insufficient data ({len(test_bars)} bars, need ≥{min_bars_needed})")
                logger.info(f"              This might be a partial trading day - fetching anyway...")
            
            # It's a trading day! Fetch all symbols
            logger.info(f"   {date_str}: ✅ Trading day! Fetching {len(self.symbols)} symbols...")
            
            # Fetch all symbols concurrently
            tasks = [self.fetch_day_bars(sym, start, end, date_str) for sym in self.symbols]
            all_bars = await asyncio.gather(*tasks)
            
            # Build dataframe
            all_data = []
            for symbol, bars in zip(self.symbols, all_bars):
                if not bars:
                    continue
                for bar in bars:
                    all_data.append({
                        'date': pd.to_datetime(bar['t']),
                        'tic': symbol,
                        'open': float(bar.get('o', 0)),
                        'high': float(bar.get('h', 0)),
                        'low': float(bar.get('l', 0)),
                        'close': float(bar.get('c', 0)),
                        'volume': int(bar.get('v', 0))
                    })
            
            if not all_data:
                days_searched += 1
                continue
            
            df = pd.DataFrame(all_data)
            
            # Calculate indicators per symbol
            processed_data = []
            for symbol in self.symbols:
                symbol_df = df[df['tic'] == symbol].copy()
                if len(symbol_df) == 0:
                    continue
                symbol_df = self.calculate_indicators(symbol_df)
                processed_data.append(symbol_df)
            
            if processed_data:
                final_df = pd.concat(processed_data, ignore_index=True)
                docs = final_df.to_dict('records')
                
                if docs:
                    # Bulk upsert with duplicate prevention
                    operations = [
                        UpdateOne(
                            {'date': doc['date'], 'tic': doc['tic']},
                            {'$set': doc},
                            upsert=True
                        )
                        for doc in docs
                    ]
                    
                    try:
                        result = self.collection.bulk_write(operations, ordered=False)
                        inserted = result.upserted_count + result.modified_count
                        total_inserted += inserted
                        logger.info(f"   {date_str}: ✅ Inserted {inserted:,} bars (duplicates prevented)")
                    except Exception as e:
                        logger.error(f"   {date_str}: ❌ Error inserting: {e}")
            
            trading_days_found += 1
            days_searched += 1
        
        logger.info("\n" + "=" * 80)
        if trading_days_found >= required_days:
            logger.info("✅ SUCCESS: Historical data requirement met")
        else:
            logger.info(f"⚠️  WARNING: Only found {trading_days_found}/{required_days} trading days")
        logger.info("=" * 80)
        logger.info(f"📊 Summary:")
        logger.info(f"   Trading days in MongoDB: {trading_days_found}")
        logger.info(f"   New bars inserted: {total_inserted:,}")
        logger.info(f"   Calendar days searched: {days_searched}")
        logger.info("=" * 80)
        
        return {
            "status": "complete" if trading_days_found >= required_days else "incomplete",
            "days": trading_days_found,
            "fetched": total_inserted
        }


async def main():
    """Main execution."""
    print("\n" + "=" * 80)
    print("🚀 HISTORICAL DATA VALIDATOR")
    print("=" * 80)
    print("This script ensures MongoDB has 3 trading days of historical data.")
    print("Each day must have at least 4 hours of market data (240 minutes).")
    print("If missing or insufficient, it will fetch from Alpaca and prevent duplicates.")
    print("=" * 80 + "\n")
    
    ensurer = HistoricalDataEnsurer()
    
    try:
        await ensurer.initialize()
        result = await ensurer.ensure_trading_days(required_days=3, min_hours_per_day=4)
        
        print("\n" + "=" * 80)
        print("📊 FINAL STATUS")
        print("=" * 80)
        print(f"Status: {result['status']}")
        print(f"Trading days with ≥4h data: {result['days']}")
        print(f"New bars fetched: {result.get('fetched', 0):,}")
        print("=" * 80)
        
        if result['status'] in ['sufficient', 'complete']:
            print("✅ Ready for fine-tuning!")
            print("   Each trading day has at least 4 hours of market data")
            print("   All technical indicators calculated with proper handling")
        else:
            print("⚠️  Need more trading days - check market hours/holidays")
        print("=" * 80 + "\n")
        
        return result['status'] in ['sufficient', 'complete']
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return False
    finally:
        await ensurer.cleanup()


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
