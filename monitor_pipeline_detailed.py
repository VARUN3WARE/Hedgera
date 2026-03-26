#!/usr/bin/env python3
"""
Real-time monitoring script for AEGIS Trading Pipeline.
Shows complete data flow from fetching to FinRL output.

Usage:
    python monitor_pipeline_detailed.py [--session-dir PATH]
"""

import asyncio
import json
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any
import argparse

sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from redis import asyncio as aioredis
except ImportError:
    import redis.asyncio as aioredis

from backend.config.settings import settings


class PipelineMonitor:
    """Real-time monitor for pipeline data flow."""
    
    def __init__(self, session_dir: str = None):
        """Initialize monitor."""
        if session_dir is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            session_dir = f"backend/logs/monitor_{timestamp}"
        
        self.session_dir = Path(session_dir)
        self.session_dir.mkdir(parents=True, exist_ok=True)
        
        self.redis_client = None
        self.monitoring = True
        
        # Statistics
        self.stats = {
            "raw_price_count": 0,
            "raw_news_count": 0,
            "raw_social_count": 0,
            "processed_count": 0,
            "finrl_decisions_count": 0,
            "tickers_seen": set(),
            "start_time": datetime.now()
        }
    
    async def connect(self):
        """Connect to Redis."""
        self.redis_client = await aioredis.from_url(
            f"redis://{settings.redis_host}:{settings.redis_port}",
            decode_responses=True
        )
        await self.redis_client.ping()
        print("✅ Connected to Redis")
    
    async def monitor_stream(self, stream_name: str, callback):
        """Monitor a specific Redis stream."""
        last_id = '$'  # Start from latest
        
        while self.monitoring:
            try:
                # Read new entries from stream
                response = await self.redis_client.xread(
                    {stream_name: last_id},
                    count=100,
                    block=1000  # Block for 1 second
                )
                
                if response:
                    for stream, messages in response:
                        for message_id, fields in messages:
                            last_id = message_id
                            await callback(stream_name, message_id, fields)
            
            except Exception as e:
                print(f"❌ Error monitoring {stream_name}: {e}")
                await asyncio.sleep(1)
    
    async def handle_raw_price(self, stream_name: str, message_id: str, fields: Dict[str, Any]):
        """Handle raw price data."""
        try:
            data = json.loads(fields.get('data', '{}'))
            symbol = data.get('symbol', 'UNKNOWN')
            
            self.stats["raw_price_count"] += 1
            self.stats["tickers_seen"].add(symbol)
            
            # Log to file
            log_file = self.session_dir / "raw_price_stream.jsonl"
            with open(log_file, 'a') as f:
                entry = {
                    "timestamp": datetime.now().isoformat(),
                    "message_id": message_id,
                    "symbol": symbol,
                    "price": data.get('price', 0),
                    "open": data.get('open', 0),
                    "high": data.get('high', 0),
                    "low": data.get('low', 0),
                    "close": data.get('close', 0),
                    "volume": data.get('volume', 0)
                }
                f.write(json.dumps(entry) + '\n')
            
            print(f"📥 RAW PRICE | {symbol}: ${data.get('close', 0):.2f} | Vol: {data.get('volume', 0):,} | Total: {self.stats['raw_price_count']}")
        
        except Exception as e:
            print(f"❌ Error handling raw price: {e}")
    
    async def handle_processed(self, stream_name: str, message_id: str, fields: Dict[str, Any]):
        """Handle processed data."""
        try:
            data = json.loads(fields.get('data', '{}'))
            metadata = data.get('metadata', {})
            ticker = metadata.get('ticker', 'UNKNOWN')
            
            self.stats["processed_count"] += 1
            
            # Extract indicators
            indicators = data.get('indicators', {})
            price_data = data.get('price_data', {})
            
            # Log to file
            log_file = self.session_dir / "processed_stream.jsonl"
            with open(log_file, 'a') as f:
                entry = {
                    "timestamp": datetime.now().isoformat(),
                    "message_id": message_id,
                    "ticker": ticker,
                    "price": price_data.get('close', 0),
                    "macd": indicators.get('macd', 0),
                    "rsi_30": indicators.get('rsi_30', 0),
                    "cci_30": indicators.get('cci_30', 0),
                    "boll_ub": indicators.get('boll_ub', 0),
                    "boll_lb": indicators.get('boll_lb', 0),
                    "close_30_sma": indicators.get('close_30_sma', 0),
                    "close_60_sma": indicators.get('close_60_sma', 0)
                }
                f.write(json.dumps(entry) + '\n')
            
            print(f"⚙️  PROCESSED | {ticker}: ${price_data.get('close', 0):.2f} | RSI: {indicators.get('rsi_30', 0):.1f} | MACD: {indicators.get('macd', 0):.2f} | Total: {self.stats['processed_count']}")
        
        except Exception as e:
            print(f"❌ Error handling processed data: {e}")
    
    async def handle_finrl_decisions(self, stream_name: str, message_id: str, fields: Dict[str, Any]):
        """Handle FinRL decisions."""
        try:
            data = json.loads(fields.get('data', '{}'))
            
            self.stats["finrl_decisions_count"] += 1
            
            # Log to file
            log_file = self.session_dir / "finrl_decisions_stream.jsonl"
            with open(log_file, 'a') as f:
                f.write(json.dumps({
                    "timestamp": datetime.now().isoformat(),
                    "message_id": message_id,
                    "data": data
                }, indent=2) + '\n')
            
            # Create detailed report
            report_file = self.session_dir / f"finrl_decision_{self.stats['finrl_decisions_count']:03d}_{datetime.now().strftime('%H%M%S')}.txt"
            with open(report_file, 'w') as f:
                f.write("=" * 100 + "\n")
                f.write(f"FinRL DECISION #{self.stats['finrl_decisions_count']}\n")
                f.write(f"Timestamp: {datetime.now().isoformat()}\n")
                f.write("=" * 100 + "\n\n")
                
                f.write(f"Model Used: {data.get('model_used', 'unknown')}\n")
                f.write(f"Selected Tickers: {data.get('selected_tickers', [])}\n\n")
                
                f.write("DECISIONS:\n")
                f.write("-" * 100 + "\n")
                
                for ticker, decision in data.get('decisions', {}).items():
                    f.write(f"\n{ticker}:\n")
                    f.write(f"  Action: {decision.get('action', 'N/A')}\n")
                    f.write(f"  Shares: {decision.get('shares', 0)}\n")
                    f.write(f"  Price: ${decision.get('price', 0):.2f}\n")
                    f.write(f"  Signal Strength: {decision.get('signal_strength', 0):.2f}\n")
            
            print("\n" + "=" * 100)
            print(f"🤖 FinRL DECISION #{self.stats['finrl_decisions_count']}")
            print("=" * 100)
            print(f"Selected: {data.get('selected_tickers', [])}")
            print(f"Model: {data.get('model_used', 'unknown')}")
            print(f"Report saved: {report_file.name}")
            print("=" * 100 + "\n")
        
        except Exception as e:
            print(f"❌ Error handling FinRL decisions: {e}")
    
    async def print_stats_periodically(self):
        """Print statistics every 30 seconds."""
        while self.monitoring:
            await asyncio.sleep(30)
            
            runtime = (datetime.now() - self.stats['start_time']).total_seconds()
            
            # Get stream lengths
            raw_price_len = await self.redis_client.xlen('raw:price-updates')
            processed_len = await self.redis_client.xlen('processed:price')
            finrl_len = await self.redis_client.xlen('finrl-decisions')
            
            # Get historical data range
            historical_range = "N/A"
            try:
                first_msgs = await self.redis_client.xrange('raw:price-updates', count=1)
                last_msgs = await self.redis_client.xrevrange('raw:price-updates', count=1)
                
                if first_msgs and last_msgs:
                    first_data = json.loads(first_msgs[0][1].get('data', '{}'))
                    last_data = json.loads(last_msgs[0][1].get('data', '{}'))
                    
                    first_ts = first_data.get('timestamp', 'N/A')
                    last_ts = last_data.get('timestamp', 'N/A')
                    
                    if first_ts != 'N/A' and last_ts != 'N/A':
                        from datetime import datetime as dt
                        try:
                            first_dt = dt.fromisoformat(first_ts.replace('Z', '+00:00'))
                            last_dt = dt.fromisoformat(last_ts.replace('Z', '+00:00'))
                            days = (last_dt - first_dt).total_seconds() / 86400
                            historical_range = f"{first_ts[:10]} → {last_ts[:10]} ({days:.1f} days)"
                        except:
                            historical_range = f"{first_ts} → {last_ts}"
            except:
                pass
            
            # Check MongoDB status
            mongodb_status = "❌ Not Connected"
            mongodb_count = 0
            try:
                from pymongo import MongoClient
                import os
                from dotenv import load_dotenv
                
                load_dotenv()
                mongo_uri = os.getenv('MONGODB_URI_STREAMING', 'mongodb://localhost:27017/')
                
                client = MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)
                db = client['finrl_trading']
                collection = db['market_data_1min']
                
                mongodb_count = collection.count_documents({})
                mongodb_status = f"✅ Connected ({mongodb_count:,} docs)"
                
                client.close()
            except:
                pass
            
            print("\n" + "=" * 100)
            print("📊 PIPELINE STATISTICS")
            print("=" * 100)
            print(f"Runtime: {runtime/60:.1f} minutes")
            print(f"")
            print(f"REDIS STREAMS:")
            print(f"  Raw Price Stream: {raw_price_len:,} entries")
            print(f"  Processed Stream: {processed_len:,} entries") 
            print(f"  FinRL Decisions: {finrl_len} runs")
            print(f"  Historical Range: {historical_range}")
            print(f"")
            print(f"REAL-TIME MONITORING (this session):")
            print(f"  Price Updates Captured: {self.stats['raw_price_count']}")
            print(f"  Processed Data Captured: {self.stats['processed_count']}")
            print(f"  FinRL Decisions Captured: {self.stats['finrl_decisions_count']}")
            print(f"  Unique Tickers Seen: {len(self.stats['tickers_seen'])} {sorted(list(self.stats['tickers_seen']))[:10]}")
            print(f"")
            print(f"MONGODB STATUS:")
            print(f"  {mongodb_status}")
            print(f"")
            print(f"Session Directory: {self.session_dir}")
            print("=" * 100 + "\n")
    
    async def run(self):
        """Run the monitor."""
        print("\n" + "=" * 100)
        print("🔍 AEGIS PIPELINE MONITOR - DETAILED DATA FLOW")
        print("=" * 100)
        print(f"Session Directory: {self.session_dir}")
        print(f"Monitoring Redis: {settings.redis_host}:{settings.redis_port}")
        print("=" * 100 + "\n")
        
        await self.connect()
        
        # Start monitoring tasks
        tasks = [
            asyncio.create_task(self.monitor_stream('raw:price-updates', self.handle_raw_price)),
            asyncio.create_task(self.monitor_stream('processed:price', self.handle_processed)),
            asyncio.create_task(self.monitor_stream('finrl-decisions', self.handle_finrl_decisions)),
            asyncio.create_task(self.print_stats_periodically())
        ]
        
        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            print("\n👋 Stopping monitor...")
            self.monitoring = False
            
            # Wait for tasks to finish
            await asyncio.sleep(1)
            
            # Print final stats
            print("\n" + "=" * 100)
            print("📊 FINAL STATISTICS")
            print("=" * 100)
            print(f"Raw Price Updates: {self.stats['raw_price_count']}")
            print(f"Processed Data: {self.stats['processed_count']}")
            print(f"FinRL Decisions: {self.stats['finrl_decisions_count']}")
            print(f"Unique Tickers: {len(self.stats['tickers_seen'])}")
            print(f"Session saved to: {self.session_dir}")
            print("=" * 100 + "\n")


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Monitor AEGIS Trading Pipeline')
    parser.add_argument('--session-dir', help='Custom session directory path')
    args = parser.parse_args()
    
    monitor = PipelineMonitor(session_dir=args.session_dir)
    await monitor.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!\n")