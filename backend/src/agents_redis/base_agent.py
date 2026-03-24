"""Base agent class for consuming from Redis streams."""

import asyncio
import json
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import redis.asyncio as redis


class BaseRedisAgent(ABC):
    """Base class for agents consuming from Redis streams."""

    def __init__(
        self,
        redis_url: str,
        stream_key: str,
        consumer_group: str,
        consumer_name: str,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Initialize the base Redis agent.

        Args:
            redis_url: Redis connection URL
            stream_key: Redis stream key to consume from
            consumer_group: Consumer group name
            consumer_name: Consumer name within the group
            logger: Optional logger instance
        """
        self.redis_url = redis_url
        self.stream_key = stream_key
        self.consumer_group = consumer_group
        self.consumer_name = consumer_name
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        self.redis_client: Optional[redis.Redis] = None
        self.running = False

    async def connect(self):
        """Connect to Redis and create consumer group if needed."""
        self.redis_client = await redis.from_url(
            self.redis_url, decode_responses=True
        )
        
        try:
            # Create consumer group if it doesn't exist
            await self.redis_client.xgroup_create(
                self.stream_key,
                self.consumer_group,
                id="0",
                mkstream=True,
            )
            self.logger.info(f"Created consumer group {self.consumer_group}")
        except redis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                self.logger.info(f"Consumer group {self.consumer_group} already exists")
            else:
                raise

    async def disconnect(self):
        """Disconnect from Redis."""
        if self.redis_client:
            await self.redis_client.close()
            self.logger.info("Disconnected from Redis")

    @abstractmethod
    async def process_message(self, message_id: str, data: Dict[str, Any]):
        """
        Process a single message from the stream.

        Args:
            message_id: Redis stream message ID
            data: Message data dictionary
        """
        pass

    async def consume(self, batch_size: int = 10, block_ms: int = 5000):
        """
        Consume messages from the Redis stream.

        Args:
            batch_size: Number of messages to fetch per batch
            block_ms: Milliseconds to block waiting for messages
        """
        if not self.redis_client:
            raise RuntimeError("Not connected to Redis. Call connect() first.")

        self.running = True
        self.logger.info(f"Starting to consume from {self.stream_key}")

        while self.running:
            try:
                # Read messages from the stream
                messages = await self.redis_client.xreadgroup(
                    self.consumer_group,
                    self.consumer_name,
                    {self.stream_key: ">"},
                    count=batch_size,
                    block=block_ms,
                )

                if messages:
                    for stream, stream_messages in messages:
                        for message_id, data in stream_messages:
                            try:
                                # Process the message
                                await self.process_message(message_id, data)
                                
                                # Acknowledge the message
                                await self.redis_client.xack(
                                    self.stream_key,
                                    self.consumer_group,
                                    message_id,
                                )
                            except Exception as e:
                                self.logger.error(
                                    f"Error processing message {message_id}: {e}",
                                    exc_info=True,
                                )

            except asyncio.CancelledError:
                self.logger.info("Consumer cancelled")
                break
            except Exception as e:
                self.logger.error(f"Error in consume loop: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def stop(self):
        """Stop consuming messages."""
        self.running = False
        self.logger.info("Stopping consumer")

    async def run(self, batch_size: int = 10, block_ms: int = 5000):
        """
        Connect to Redis and start consuming messages.

        Args:
            batch_size: Number of messages to fetch per batch
            block_ms: Milliseconds to block waiting for messages
        """
        try:
            await self.connect()
            await self.consume(batch_size, block_ms)
        finally:
            await self.disconnect()
