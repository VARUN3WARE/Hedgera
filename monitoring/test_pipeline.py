#!/usr/bin/env python3
"""
Test Pipeline Runner - Short wait times for testing
For testing only - uses 2 minutes instead of 1 hour
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from backend.src.orchestration.pipeline_main import AegisPipeline
from backend.src.services.finrl_integrated_service import FinRLIntegratedService


def print_banner():
    banner = """
    ╔═══════════════════════════════════════════════════════════╗
    ║           AEGIS TRADING SYSTEM - TEST MODE               ║
    ╚═══════════════════════════════════════════════════════════╝
    
    ⚠️  TEST MODE: Wait times reduced for testing
    
    Workflow:
    1. Collect data for 30 tickers (Price, News, Social)
    2. Process and calculate indicators in real-time
    3. Wait 2 MINUTES (test mode) for data accumulation
    4. Run FinRL model → Select top 10 tickers
    5. Store decisions in Redis
    
    Press Ctrl+C to stop
    """
    print(banner)


class TestAegisPipeline(AegisPipeline):
    """Test version with shorter wait times."""
    
    def setup(self):
        """Setup with test configuration."""
        super().setup()
        
        # Override FinRL service with shorter wait time
        self.finrl_service = FinRLIntegratedService(
            redis_host="localhost",
            redis_port=6379,
            collection_time_minutes=2,  # 2 minutes for testing
            run_interval_hours=1  # Run every hour in test mode
        )
        self.finrl_service.set_logging(self.log_paths['finrl_decisions_log'])
        print("\n⚠️  TEST MODE: FinRL will run after 2 minutes of data collection\n")


async def main():
    print_banner()
    
    try:
        pipeline = TestAegisPipeline()
        await pipeline.start_all()
    except KeyboardInterrupt:
        print("\n👋 Goodbye!\n")
    except Exception as e:
        print(f"\n❌ Error: {e}\n")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Test stopped\n")
