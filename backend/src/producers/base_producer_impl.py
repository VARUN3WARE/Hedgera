"""Abstract base producer for all data producers."""
import asyncio
import json
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import logging
from backend.src.core.redis_client import RedisStreamClient
from backend.config.settings import settings

logger = logging.getLogger(__name__)


class BaseProducer(ABC):
    """
    Abstract base class for all producers.
    
    Producers are lightweight, disposable services that:
    1. Fetch data from an external source
    2. Publish it immediately to a Redis Stream using XADD
    3. Repeat at a defined interval
    """
    
    def __init__(
        self,
        stream_name: str,
        fetch_interval: float,
        name: Optional[str] = None,
    ):
        """
        Initialize producer.
        
        Args:
            stream_name: Redis stream to publish to
            fetch_interval: Seconds between fetch operations
            name: Producer name for logging
        """
        self.stream_name = stream_name
        self.fetch_interval = fetch_interval
        self.name = name or self.__class__.__name__
        self.running = False
        self._publish_count = 0
        self._error_count = 0
        
        # Initialize Redis client
        self.redis_client = RedisStreamClient(
            host=settings.redis_host,
            port=settings.redis_port,
            password=settings.redis_password
        )
    
    async def run(self):
        """Main execution loop."""
        logger.info(f"🚀 Starting {self.name} (stream: {self.stream_name}, interval: {self.fetch_interval}s)")
        
        try:
            # Initialize producer-specific resources
            await self.initialize()
            
            self.running = True
            
            # Main fetch-publish loop
            while self.running:
                try:
                    # Fetch data from external source
                    data = await self.fetch_data()
                    
                    if data:
                        # Handle batch data (for price producer with multiple symbols)
                        if "batch" in data:
                            for item in data["batch"]:
                                await self._publish(item)
                                self._publish_count += 1
                        else:
                            # Single item publish
                            await self._publish(data)
                            self._publish_count += 1
                        
                        if self._publish_count % 10 == 0:
                            logger.info(
                                f"📊 {self.name}: Published {self._publish_count} messages "
                                f"(errors: {self._error_count})"
                            )
                    
                except Exception as e:
                    self._error_count += 1
                    logger.error(f"❌ {self.name} error: {e}", exc_info=True)
                    
                    # Stop if too many errors
                    if self._error_count > 50:
                        logger.critical(f"🛑 {self.name} exceeded error threshold, stopping")
                        self.running = False
                        break
                
                # Wait before next fetch
                await asyncio.sleep(self.fetch_interval)
        
        except Exception as e:
            logger.error(f"❌ {self.name} fatal error: {e}", exc_info=True)
            raise
        
        finally:
            await self.cleanup()
            logger.info(f"✅ {self.name} stopped (published: {self._publish_count}, errors: {self._error_count})")
    
    async def _publish(self, data: Dict[str, Any]):
        """Publish data to Redis Stream."""
        try:
            # Wrap data in "data" field as JSON string for consumer compatibility
            message = {"data": json.dumps(data)}
            # Run synchronous Redis operation in executor to avoid blocking
            await asyncio.to_thread(
                self.redis_client.xadd,
                self.stream_name,
                message,
                maxlen=10000
            )
        except Exception as e:
            logger.error(f"Failed to publish to {self.stream_name}: {e}", exc_info=True)
            raise
    
    def stop(self):
        """Stop the producer."""
        logger.info(f"Stopping {self.name}...")
        self.running = False
    
    @abstractmethod
    async def initialize(self):
        """Initialize producer resources (HTTP sessions, etc.)."""
        pass
    
    @abstractmethod
    async def fetch_data(self) -> Optional[Dict[str, Any]]:
        """
        Fetch data from external source.
        
        Returns:
            Data dictionary or None if no data available
        """
        pass
    
    async def cleanup(self):
        """Cleanup resources (HTTP sessions, etc.)."""
        pass
