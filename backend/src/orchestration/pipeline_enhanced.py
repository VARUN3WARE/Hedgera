"""
Enhanced Pipeline Orchestrator with MongoDB Sync and Fine-tuning

New Architecture:
1. Data Producers → Redis Streams (unchanged)
2. Streaming Engine → Processes data (unchanged)
3. MongoDB Sync Service → Syncs Redis to MongoDB (NEW)
4. Fine-tuning Service → Fine-tunes model every 2h (NEW)
5. FinRL Service → Uses fine-tuned model with scaled predictions (UPDATED)
"""

import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from backend.src.producers.price_producer_impl import PriceProducer
from backend.src.producers.news_producer_impl import NewsProducer
from backend.src.producers.social_producer_impl import SocialProducer
from backend.src.engine.streaming_engine import StreamingEngine
from backend.src.services.mongodb_sync_service import MongoDBSyncService
from backend.src.services.finetuning_service import FineTuningService
from backend.src.services.finrl_service_finetuned import FinRLServiceWithFineTuning
from backend.src.utils.trigger_checker import TriggerChecker
from backend.config.settings import settings
from backend.config.logging_setup import setup_logging

logger = logging.getLogger(__name__)


class AegisPipelineEnhanced:
    """Enhanced pipeline with MongoDB persistence and model fine-tuning."""
    
    def __init__(self, quick_mode: bool = False):
        """
        Initialize enhanced pipeline.
        
        Args:
            quick_mode: If True, use quick test settings (5 min FinRL, 10 min fine-tune)
        """
        self.quick_mode = quick_mode
        self.producers = []
        self.engine = None
        self.mongodb_sync = None
        self.finetuning_service = None
        self.finrl_service = None
        self.trigger_checker = None
        self.tasks = []
        self.log_paths = None
    
    def setup(self):
        """Setup logging and components."""
        self.log_paths = setup_logging()
        logger.info("=" * 100)
        logger.info("🚀 AEGIS TRADING SYSTEM - ENHANCED PIPELINE WITH FINE-TUNING")
        if self.quick_mode:
            logger.info("⚡ QUICK MODE ENABLED - Using test settings")
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
        logger.info("ENHANCED WORKFLOW DESCRIPTION")
        logger.info("=" * 100)
        logger.info("1️⃣  Data Producers → Redis Streams (UNCHANGED)")
        logger.info("   • Price: Alpaca API → raw:price-updates every 5 min")
        logger.info("   • News: Disabled until FinRL selects tickers")
        logger.info("   • Social: Disabled until FinRL selects tickers")
        logger.info("")
        logger.info("2️⃣  Streaming Engine → Process & Calculate Indicators (UNCHANGED)")
        logger.info("   • Consumes: raw:price-updates, raw:news-articles, raw:social")
        logger.info("   • Calculates: MACD, RSI, Bollinger, CCI, DX, SMAs")
        logger.info("   • Publishes: processed:price every 5 seconds")
        logger.info("")
        logger.info("3️⃣  MongoDB Sync Service → Historical Storage (NEW)")
        logger.info("   • Syncs: processed:price → MongoDB every 60 seconds")
        logger.info("   • Retention: PERMANENT (stores all data)")
        logger.info("   • Purpose: Historical data for fine-tuning & analytics")
        logger.info("")
        logger.info("4️⃣  Fine-tuning Service → Model Improvement (NEW)")
        logger.info("   • Runs: Every 2 hours")
        logger.info("   • Data: 48h historical from MongoDB")
        logger.info("   • Validates: Train/val split with rollback threshold")
        logger.info("   • Saves: Fine-tuned model if performance improves")
        logger.info("")
        logger.info("5️⃣  FinRL Service → Trading Decisions (UPDATED)")
        logger.info("   • Model: Uses fine-tuned model if available")
        logger.info("   • Predictions: Scaled actions (action * MAX_STOCK)")
        logger.info("   • Logic: Buy (>10), Sell (<-10), Hold (else)")
        logger.info(f"   • Output: Top {settings.finrl_output_tickers} tickers to finrl-decisions")
        logger.info("   • Activates: News/social producers for selected tickers")
        logger.info("")
        logger.info("6️⃣  Final Output:")
        logger.info("   • Redis: finrl-decisions stream + finrl:latest key")
        logger.info("   • MongoDB: Persistent historical data for analytics")
        logger.info("   • Logs: JSONL files for debugging")
        logger.info("=" * 100 + "\n")
        
        # Initialize producers
        logger.info("📡 Initializing Data Producers...")
        price_producer = PriceProducer()
        news_producer = NewsProducer()
        social_producer = SocialProducer()
        self.producers = [price_producer, news_producer, social_producer]
        logger.info(f"   ✅ {len(self.producers)} producers initialized")
        
        # Initialize streaming engine
        logger.info("\n⚙️  Initializing Streaming Engine...")
        self.engine = StreamingEngine()
        self.engine.set_logging(
            self.log_paths['raw_data_log'],
            self.log_paths['processed_data_log']
        )
        logger.info("   ✅ Streaming engine initialized")
        
        # Configure sync interval based on quick_mode
        sync_interval = 10 if self.quick_mode else 60
        
        # Initialize MongoDB sync service (NEW)
        logger.info("\n💾 Initializing MongoDB Sync Service...")
        self.mongodb_sync = MongoDBSyncService(
            redis_host=settings.redis_host,
            redis_port=settings.redis_port,
            sync_interval_seconds=sync_interval,
        )
        logger.info("   ✅ MongoDB sync service initialized")
        
        # Initialize fine-tuning service (NEW)
        logger.info("\n🎓 Initializing Fine-tuning Service...")
        self.finetuning_service = FineTuningService(
            mongo_sync_service=self.mongodb_sync,
            finetune_interval_hours=2,
            historical_window_hours=48,
            min_days_required=3  # Always 3 days (ensured by historical_data.py)
        )
        logger.info("   ✅ Fine-tuning service initialized")
        
        # Initialize FinRL service with fine-tuning support (UPDATED)
        logger.info("\n🤖 Initializing FinRL Service (Fine-tuned)...")
        self.finrl_service = FinRLServiceWithFineTuning(
            news_producer=news_producer,
            social_producer=social_producer
        )
        self.finrl_service.set_logging(self.log_paths['finrl_decisions_log'])
        logger.info("   ✅ FinRL service initialized")
        
        # Initialize Trigger Checker (for early FinRL triggering)
        logger.info("\n🔍 Initializing Trigger Checker...")
        self.trigger_checker = TriggerChecker()
        logger.info("   ✅ Trigger checker initialized")
        
        logger.info("\n" + "=" * 100)
        logger.info("✅ ALL COMPONENTS INITIALIZED - STARTING EXECUTION")
        logger.info("=" * 100 + "\n")
        
        # Start all components
        try:
            # Original components (unchanged)
            producer_tasks = [asyncio.create_task(p.run()) for p in self.producers]
            engine_task = asyncio.create_task(self.engine.run())
            
            # New components
            mongodb_sync_task = asyncio.create_task(self.mongodb_sync.start_service())
            finetuning_task = asyncio.create_task(self.finetuning_service.start_service())
            
            # Updated FinRL service
            finrl_task = asyncio.create_task(self.finrl_service.start_service())
            
            # Trigger monitor task
            trigger_task = asyncio.create_task(self._monitor_trigger())
            
            self.tasks = producer_tasks + [
                engine_task, 
                mongodb_sync_task, 
                finetuning_task, 
                finrl_task,
                trigger_task
            ]
            
            logger.info("🟢 Enhanced Pipeline is now running...")
            logger.info("   Components:")
            logger.info(f"   • {len(self.producers)} Data Producers")
            logger.info("   • 1 Streaming Engine")
            logger.info("   • 1 MongoDB Sync Service")
            logger.info("   • 1 Fine-tuning Service")
            logger.info("   • 1 FinRL Service")
            logger.info("   • 1 Trigger Checker")
            logger.info("")
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
        """Monitor for major breakouts every 5 minutes."""
        logger.info("🔍 Trigger monitor task started")
        
        await self.trigger_checker.connect()
        
        # Small delay to let initial data flow
        await asyncio.sleep(60)
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
        logger.info("🛑 Shutting down enhanced pipeline components...")
        
        # Stop producers
        for producer in self.producers:
            producer.stop()
        
        # Cancel all async tasks
        for task in self.tasks:
            task.cancel()
        
        # Close connections
        if self.engine:
            await self.engine.close()
        
        if self.mongodb_sync:
            self.mongodb_sync.close()
        
        if self.trigger_checker:
            await self.trigger_checker.close()
        
        logger.info("=" * 100)
        logger.info("✅ ENHANCED PIPELINE SHUTDOWN COMPLETE")
        logger.info(f"📁 Logs: {self.log_paths['log_dir']}")
        logger.info("=" * 100)


async def main():
    """Main entry point."""
    pipeline = AegisPipelineEnhanced()
    await pipeline.start_all()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
