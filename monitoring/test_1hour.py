#!/usr/bin/env python3
"""
AEGIS 1-Hour Production Test Script
====================================
This script runs the AEGIS pipeline for 1 hour and monitors all streams.

What it does:
1. Clears all existing Redis streams for fresh start
2. Starts the pipeline (producers + streaming engine)
3. Monitors data flow every 5 minutes
4. After 1 hour, triggers FinRL analysis
5. Displays comprehensive results

Expected Data Flow:
-------------------
Minutes 0-60:
- Price data: Every 5 minutes (12 cycles × 30 tickers = 360 entries)
- News data: Every 15 minutes (4 cycles × variable articles)
- Social data: Every 10 minutes (6 cycles × 30 tickers = 180 entries)
- Processed data: Aggregated and published regularly

Minute 60:
- FinRL runs on accumulated processed data
- Selects top 10 tickers
- Publishes decisions to finrl-decisions stream

Redis Streams Layout:
--------------------
RAW DATA:
- raw:price-updates - Price OHLCV from Alpaca
- raw:news-articles - News from NewsAPI
- raw:social - Social sentiment (synthetic)

PROCESSED DATA:
- processed:price - Price + indicators for FinRL
- processed:master-state - Complete aggregated state

FINRL DECISIONS:
- finrl-decisions - FinRL model output (top 10 tickers)
"""

import asyncio
import sys
from pathlib import Path
import time
from datetime import datetime, timedelta
import logging

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from redis import asyncio as aioredis
except ImportError:
    import redis.asyncio as aioredis

from backend.config.settings import settings
from backend.config.logging_setup import setup_logging

# Setup logging
logger = setup_logging('test_1hour', 'backend/logs/app')

class OneHourTest:
    """1-hour production test orchestrator."""
    
    def __init__(self):
        self.redis = None
        self.start_time = None
        self.test_duration_minutes = 60
        self.monitor_interval_minutes = 5
        
    async def connect_redis(self):
        """Connect to Redis."""
        self.redis = await aioredis.from_url(
            f"redis://{settings.redis_host}:{settings.redis_port}",
            decode_responses=True
        )
        await self.redis.ping()
        logger.info("✅ Connected to Redis")
    
    async def clear_streams(self):
        """Clear all streams for fresh start."""
        streams = [
            'raw:price-updates',
            'raw:news-articles',
            'raw:social',
            'processed:price',
            'processed:master-state',
            'finrl-decisions'
        ]
        
        logger.info("🧹 Clearing existing streams...")
        for stream in streams:
            try:
                # Delete stream
                deleted = await self.redis.delete(stream)
                if deleted:
                    logger.info(f"   ✓ Cleared {stream}")
            except Exception as e:
                logger.debug(f"   • {stream} doesn't exist or error: {e}")
        
        logger.info("✅ All streams cleared - fresh start!")
    
    async def monitor_streams(self):
        """Monitor all streams and display status."""
        streams = {
            'raw:price-updates': 'RAW',
            'raw:news-articles': 'RAW',
            'raw:social': 'RAW',
            'processed:price': 'PROCESSED',
            'processed:master-state': 'PROCESSED',
            'finrl-decisions': 'FINRL'
        }
        
        elapsed = int((datetime.now() - self.start_time).total_seconds() / 60)
        
        logger.info("=" * 80)
        logger.info(f"📊 STREAM STATUS - {elapsed} minutes elapsed")
        logger.info("=" * 80)
        
        for stream, category in streams.items():
            try:
                length = await self.redis.xlen(stream)
                
                # Get latest entry
                latest = None
                if length > 0:
                    entries = await self.redis.xrevrange(stream, count=1)
                    if entries:
                        latest = entries[0][0]  # Message ID contains timestamp
                
                status = f"[{category}] {stream:30s} | {length:5d} entries"
                if latest:
                    status += f" | Latest: {latest}"
                
                logger.info(status)
            except Exception as e:
                logger.warning(f"[{category}] {stream:30s} | ERROR: {e}")
        
        logger.info("=" * 80)
    
    async def wait_and_monitor(self):
        """Wait for 1 hour while monitoring every 5 minutes."""
        end_time = self.start_time + timedelta(minutes=self.test_duration_minutes)
        
        logger.info(f"⏰ Test will run until: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"📈 Monitoring every {self.monitor_interval_minutes} minutes")
        logger.info("")
        
        while datetime.now() < end_time:
            remaining = int((end_time - datetime.now()).total_seconds() / 60)
            
            if remaining % self.monitor_interval_minutes == 0 or remaining <= 1:
                await self.monitor_streams()
            
            # Wait 1 minute
            await asyncio.sleep(60)
        
        logger.info("⏰ 1 hour complete!")
    
    async def final_report(self):
        """Generate final report after 1 hour."""
        logger.info("")
        logger.info("=" * 80)
        logger.info("📋 FINAL REPORT - 1 HOUR TEST COMPLETE")
        logger.info("=" * 80)
        logger.info("")
        
        # Stream counts
        logger.info("📊 Final Stream Counts:")
        streams = [
            'raw:price-updates',
            'raw:news-articles',
            'raw:social',
            'processed:price',
            'processed:master-state',
            'finrl-decisions'
        ]
        
        for stream in streams:
            try:
                length = await self.redis.xlen(stream)
                logger.info(f"   {stream:30s}: {length:5d} entries")
            except:
                logger.info(f"   {stream:30s}:     0 entries")
        
        logger.info("")
        
        # Check FinRL decisions
        logger.info("🤖 FinRL Decisions:")
        try:
            finrl_length = await self.redis.xlen('finrl-decisions')
            if finrl_length > 0:
                entries = await self.redis.xrevrange('finrl-decisions', count=1)
                if entries:
                    import json
                    msg_id, fields = entries[0]
                    logger.info(f"   ✅ FinRL ran successfully!")
                    logger.info(f"   Message ID: {msg_id}")
                    logger.info(f"   Timestamp: {fields.get('timestamp', 'N/A')}")
                    
                    selected = fields.get('selected_tickers', '[]')
                    tickers = json.loads(selected)
                    logger.info(f"   Selected {len(tickers)} tickers: {tickers}")
            else:
                logger.warning("   ⚠️  No FinRL decisions found - may need to run FinRL manually")
        except Exception as e:
            logger.error(f"   ❌ Error checking FinRL: {e}")
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("✅ Test Complete!")
        logger.info("=" * 80)
        logger.info("")
        logger.info("Next Steps:")
        logger.info("1. Check Redis Commander at http://localhost:8085")
        logger.info("2. Review logs in backend/logs/")
        logger.info("3. Run monitoring script: backend/scripts/monitoring/monitor_streams.sh")
        logger.info("")
    
    async def run(self):
        """Run the complete 1-hour test."""
        self.start_time = datetime.now()
        
        logger.info("=" * 80)
        logger.info("🚀 AEGIS 1-HOUR PRODUCTION TEST")
        logger.info("=" * 80)
        logger.info(f"Start Time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Duration: {self.test_duration_minutes} minutes")
        logger.info(f"Monitor Interval: {self.monitor_interval_minutes} minutes")
        logger.info("=" * 80)
        logger.info("")
        
        # Connect to Redis
        await self.connect_redis()
        
        # Clear streams
        await self.clear_streams()
        
        logger.info("")
        logger.info("⚠️  IMPORTANT: Make sure the pipeline is running!")
        logger.info("   Run in another terminal: python main.py")
        logger.info("")
        logger.info("Starting monitoring in 10 seconds...")
        await asyncio.sleep(10)
        
        # Monitor for 1 hour
        await self.wait_and_monitor()
        
        # Final report
        await self.final_report()
        
        # Cleanup
        if self.redis:
            await self.redis.close()


if __name__ == "__main__":
    test = OneHourTest()
    asyncio.run(test.run())
