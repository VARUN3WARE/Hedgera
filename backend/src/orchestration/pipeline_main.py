"""
Main Pipeline Orchestrator with Data Logging
Implements workflow: Data collection (1 hour) → FinRL → 10 tickers selection
"""

import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from backend.src.producers.price_producer_impl import PriceProducer
from backend.src.producers.news_producer_impl import NewsProducer
from backend.src.producers.social_producer_impl import SocialProducer
from backend.src.pathway_engine.streaming_engine_pathway import PathwayStreamingEngine
from backend.src.services.finrl_integrated_service import FinRLIntegratedService
from backend.src.utils.trigger_checker import TriggerChecker
from backend.config.settings import settings
from backend.config.logging_setup import setup_logging

logger = logging.getLogger(__name__)


class AegisPipeline:
    def __init__(self):
        self.producers = []
        self.engine = None
        self.finrl_service = None
        self.trigger_checker = None
        self.tasks = []
        self.log_paths = None
    
    def setup(self):
        """Setup logging and components."""
        # Setup logging
        self.log_paths = setup_logging()
        logger.info("=" * 100)
        logger.info("🚀 AEGIS TRADING SYSTEM - PIPELINE INITIALIZATION")
        logger.info("=" * 100)
        logger.info(f"📁 Log directory: {self.log_paths['log_dir']}")
        logger.info(f"📝 Session: {self.log_paths['session_time']}")
        logger.info(f"📊 Raw data log: {self.log_paths['raw_data_log']}")
        logger.info(f"📈 Processed data log: {self.log_paths['processed_data_log']}")
        logger.info(f"🤖 FinRL decisions log: {self.log_paths['finrl_decisions_log']}")
        logger.info("=" * 100)
    
    async def start_all(self):
        """Start all pipeline components."""
        self.setup()
        
        logger.info("\n" + "=" * 100)
        logger.info("WORKFLOW DESCRIPTION")
        logger.info("=" * 100)
        logger.info("1️⃣  Data Producers fetch data for 30 tickers:")
        logger.info("   • Price data (OHLCV) from Alpaca - every 5 minutes (ALL 30 tickers)")
        logger.info("   • News articles from NewsAPI - DISABLED until FinRL selects 10 tickers")
        logger.info("   • Social sentiment (synthetic) - DISABLED until FinRL selects 10 tickers")
        logger.info("")
        logger.info("2️⃣  Streaming Engine processes data:")
        logger.info("   • Aggregates raw data by ticker")
        logger.info("   • Calculates technical indicators (MACD, RSI, Bollinger, etc.)")
        logger.info("   • Publishes processed data every 5 seconds")
        logger.info("")
        logger.info("3️⃣  FinRL Service (runs after 60 minutes of data collection):")
        logger.info("   • Waits for 60 minutes to collect sufficient price data")
        logger.info("   • Fetches all processed data (30 tickers with indicators)")
        logger.info("   • Runs FinRL paper trading model")
        logger.info(f"   • Selects top {settings.finrl_output_tickers} tickers")
        logger.info("   • Publishes decisions to Redis")
        logger.info(f"   • ACTIVATES news/social producers for selected {settings.finrl_output_tickers} tickers")
        logger.info("")
        logger.info("4️⃣  After FinRL Selection:")
        logger.info(f"   • News producer: Fetches news every 5 minutes for {settings.finrl_output_tickers} selected tickers")
        logger.info(f"   • Social producer: Generates sentiment every 5 minutes for {settings.finrl_output_tickers} selected tickers")
        logger.info(f"   • Price producer: Continues fetching for all 30 tickers (for next FinRL cycle)")
        logger.info("")
        logger.info("5️⃣  Output:")
        logger.info(f"   • {settings.finrl_output_tickers} selected tickers stored in finrl-decisions stream")
        logger.info("   • Ready for agentic workflow consumption")
        logger.info("=" * 100 + "\n")
        
        # Initialize producers
        logger.info("📡 Initializing Data Producers...")
        price_producer = PriceProducer()
        news_producer = NewsProducer()
        social_producer = SocialProducer()
        self.producers = [price_producer, news_producer, social_producer]
        logger.info(f"   ✅ {len(self.producers)} producers initialized")
        logger.info(f"   ℹ️  Price: Active (all 30 tickers)")
        logger.info(f"   ℹ️  News: Disabled (will activate after FinRL selects 10 tickers)")
        logger.info(f"   ℹ️  Social: Disabled (will activate after FinRL selects 10 tickers)")
        
        # Initialize Pathway streaming engine
        logger.info("\n⚙️  Initializing Pathway Streaming Engine...")
        self.engine = PathwayStreamingEngine(
            redis_host=settings.redis_host,
            redis_port=settings.redis_port,
            redis_password=getattr(settings, 'redis_password', '')
        )
        self.engine.set_logging(
            self.log_paths['raw_data_log'],
            self.log_paths['processed_data_log']
        )
        logger.info("   ✅ Pathway streaming engine initialized with automatic temporal processing")
        logger.info("   ℹ️  Using Pathway for: window management, indicators, multi-stream correlation")
        
        # Initialize FinRL service with producer references
        logger.info("\n🤖 Initializing FinRL Service...")
        self.finrl_service = FinRLIntegratedService(
            redis_host=settings.redis_host,
            redis_port=settings.redis_port,
            collection_time_minutes=60,  # Wait 60 minutes for production (change to 2 for testing)
            run_interval_hours=2,  # Run every 2 hours
            news_producer=news_producer,  # Pass reference to activate after ticker selection
            social_producer=social_producer  # Pass reference to activate after ticker selection
        )
        self.finrl_service.set_logging(self.log_paths['finrl_decisions_log'])
        logger.info("   ✅ FinRL service initialized")
        
        # Initialize Trigger Checker (for early FinRL triggering)
        logger.info("\n🔍 Initializing Trigger Checker...")
        self.trigger_checker = TriggerChecker(
            redis_host=settings.redis_host,
            redis_port=settings.redis_port
        )
        logger.info("   ✅ Trigger checker initialized")
        
        logger.info("\n" + "=" * 100)
        logger.info("✅ ALL COMPONENTS INITIALIZED - STARTING EXECUTION")
        logger.info("=" * 100 + "\n")
        
        # Start all components
        try:
            producer_tasks = [asyncio.create_task(p.run()) for p in self.producers]
            engine_task = asyncio.create_task(self.engine.run())
            finrl_task = asyncio.create_task(self.finrl_service.start_service())
            trigger_task = asyncio.create_task(self._monitor_trigger())
            
            self.tasks = producer_tasks + [engine_task, finrl_task, trigger_task]
            
            logger.info("🟢 Pipeline is now running...")
            logger.info("   Press Ctrl+C to stop\n")
            
            await asyncio.gather(*self.tasks)
            
        except KeyboardInterrupt:
            logger.info("\n" + "=" * 100)
            logger.info("🛑 SHUTDOWN SIGNAL RECEIVED")
            logger.info("=" * 100)
            await self.shutdown()
        except Exception as e:
            logger.error(f"\n❌ Pipeline error: {e}", exc_info=True)
            await self.shutdown()
    
    async def _monitor_trigger(self):
        """Monitor for major breakouts every 5 minutes (after 60 min delay)."""
        logger.info("🔍 Trigger monitor task started")
        logger.info("   Waiting 60 minutes before first trigger check...")
        
        await self.trigger_checker.connect()
        
        # Wait 60 minutes before starting trigger checks (same as FinRL)
        await asyncio.sleep(3600)
        logger.info("🟢 Trigger monitor now active - checking every 5 minutes")
        
        while True:
            try:
                await asyncio.sleep(300)  # Check every 5 minutes
                
                # Check for major breakout
                triggered = await self.trigger_checker.check_trigger()
                
                if triggered:
                    logger.info("\n" + "=" * 100)
                    logger.info("🚨 MAJOR BREAKOUT DETECTED! Running FinRL immediately (outside 2-hour schedule)")
                    logger.info("=" * 100 + "\n")
                    
                    try:
                        # Run FinRL immediately on current data (no wait)
                        await self.finrl_service.run_finrl_immediate()
                        logger.info("✅ Immediate FinRL execution completed!")
                        logger.info("   Original 2-hour schedule continues as planned\n")
                    except Exception as e:
                        logger.error(f"❌ Error in immediate FinRL execution: {e}", exc_info=True)
                
            except asyncio.CancelledError:
                logger.info("🛑 Trigger monitor stopped")
                break
            except Exception as e:
                logger.error(f"❌ Error in trigger monitor: {e}", exc_info=True)
                await asyncio.sleep(135)  # Continue monitoring even after error
    
    async def shutdown(self):
        """Gracefully shutdown all components."""
        logger.info("🛑 Shutting down pipeline components...")
        
        for producer in self.producers:
            producer.stop()
        
        for task in self.tasks:
            task.cancel()
        
        if self.engine:
            await self.engine.close()
        
        if self.trigger_checker:
            await self.trigger_checker.close()
        
        logger.info("=" * 100)
        logger.info("✅ PIPELINE SHUTDOWN COMPLETE")
        logger.info(f"📁 Logs saved to: {self.log_paths['log_dir']}")
        logger.info("=" * 100)


async def main():
    """Main entry point."""
    pipeline = AegisPipeline()
    await pipeline.start_all()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")