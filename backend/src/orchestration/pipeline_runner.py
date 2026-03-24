"""
Main orchestrator for running all pipeline components.
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.src.producers.price_producer_impl import PriceProducer
from backend.src.producers.news_producer_impl import NewsProducer
from backend.src.producers.social_producer_impl import SocialProducer
from backend.src.pathway_engine.streaming_engine_pathway import PathwayStreamingEngine
from backend.src.services.finrl_service_impl import FinRLService
from backend.config.settings import settings

logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Orchestrates all pipeline components."""
    
    def __init__(self):
        self.producers = []
        self.engine = None
        self.finrl_service = None
        self.tasks = []
    
    async def start_all(self):
        """Start all pipeline components."""
        logger.info("=" * 80)
        logger.info("🚀 Starting AEGIS Trading System Pipeline")
        logger.info("=" * 80)
        
        # Initialize producers
        logger.info("\n📡 Initializing Data Producers...")
        price_producer = PriceProducer()
        news_producer = NewsProducer()
        social_producer = SocialProducer()
        
        self.producers = [price_producer, news_producer, social_producer]
        
        # Initialize Pathway streaming engine
        logger.info("\n⚙️  Initializing Pathway Streaming Engine...")
        self.engine = PathwayStreamingEngine(
            redis_host=settings.redis_host,
            redis_port=settings.redis_port,
            redis_password=getattr(settings, 'redis_password', '')
        )
        logger.info("   ✅ Pathway streaming engine initialized")
        logger.info("   ℹ️  Using automatic temporal processing for indicators")
        
        # Initialize FinRL service
        logger.info("\n🤖 Initializing FinRL Service...")
        self.finrl_service = FinRLService(
            redis_host=settings.redis_host,
            redis_port=settings.redis_port,
            run_interval=settings.finrl_run_interval
        )
        
        logger.info("\n" + "=" * 80)
        logger.info("✅ All components initialized. Starting execution...")
        logger.info("=" * 80 + "\n")
        
        # Start all components
        try:
            # Create tasks for all components
            producer_tasks = [asyncio.create_task(p.run()) for p in self.producers]
            engine_task = asyncio.create_task(self.engine.run())
            finrl_task = asyncio.create_task(self.finrl_service.start_service())
            
            self.tasks = producer_tasks + [engine_task, finrl_task]
            
            # Wait for all tasks
            await asyncio.gather(*self.tasks)
            
        except KeyboardInterrupt:
            logger.info("\n🛑 Shutdown signal received...")
            await self.shutdown()
        except Exception as e:
            logger.error(f"❌ Pipeline error: {e}", exc_info=True)
            await self.shutdown()
    
    async def shutdown(self):
        """Gracefully shutdown all components."""
        logger.info("🛑 Shutting down pipeline components...")
        
        # Stop producers
        for producer in self.producers:
            producer.stop()
        
        # Cancel all tasks
        for task in self.tasks:
            task.cancel()
        
        # Close engine
        if self.engine:
            await self.engine.close()
        
        logger.info("✅ Pipeline shutdown complete")


async def main():
    """Main entry point."""
    orchestrator = PipelineOrchestrator()
    await orchestrator.start_all()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Goodbye!")
