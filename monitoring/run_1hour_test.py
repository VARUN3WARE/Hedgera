#!/usr/bin/env python3
"""
1-Hour Production Test Runner
Runs the AEGIS pipeline for exactly 1 hour with full monitoring
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime
import signal

sys.path.insert(0, str(Path(__file__).parent))

from backend.src.orchestration.pipeline_main import AegisPipeline


def print_production_banner():
    banner = """
    ╔═══════════════════════════════════════════════════════════╗
    ║      AEGIS TRADING SYSTEM - 1 HOUR PRODUCTION TEST       ║
    ╚═══════════════════════════════════════════════════════════╝
    
    📅 Test Duration: 60 minutes
    🎯 Objective: Verify complete data pipeline + FinRL integration
    
    Timeline:
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    Minutes 0-60:  Collect data (Price, News, Social)
                   Process with technical indicators
                   Publish to processed streams
    
    Minute 60:     FinRL analyzes all processed data
                   Selects top 10 tickers
                   Publishes decisions
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    Press Ctrl+C to stop
    """
    print(banner)
    print(f"⏰ Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⏰ Expected FinRL Run: {(datetime.now().replace(second=0, microsecond=0)).strftime('%Y-%m-%d %H:%M:%S')} + 60 minutes\n")


async def main():
    print_production_banner()
    
    try:
        pipeline = AegisPipeline()
        await pipeline.start_all()
    except KeyboardInterrupt:
        print("\n\n" + "=" * 70)
        print("🛑 Pipeline stopped by user")
        print("=" * 70 + "\n")
    except Exception as e:
        print(f"\n❌ Error: {e}\n")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Test ended\n")
