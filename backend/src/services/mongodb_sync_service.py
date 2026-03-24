"""
MongoDB Sync Service

Syncs processed data from Redis streams to MongoDB for:
1. Historical data storage (48h rolling window)
2. Fine-tuning data preparation
3. Data persistence

Runs as background service alongside the pipeline.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List
import json
import pandas as pd

try:
    from redis import asyncio as aioredis
except ImportError:
    import redis.asyncio as aioredis

from pymongo import MongoClient, ASCENDING, DESCENDING
from backend.config.settings import settings

logger = logging.getLogger(__name__)


class MongoDBSyncService:
    """Syncs Redis stream data to MongoDB for historical storage and fine-tuning."""
    
    def __init__(
        self,
        mongo_uri: str = None,
        mongo_db: str = None,
        mongo_collection: str = None,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        sync_interval_seconds: int = 60,  # Sync every minute
    ):
        """Initialize MongoDB sync service."""
        # Use settings for MongoDB configuration (cloud URI from .env)
        self.mongo_uri = mongo_uri or settings.mongodb_uri_streaming
        self.mongo_db_name = mongo_db or settings.mongodb_db_name
        self.mongo_collection_name = mongo_collection or settings.mongodb_collection_name
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.sync_interval = sync_interval_seconds
        
        # Connections
        self.redis_client = None
        self.mongo_client = None
        self.collection = None
        
        # Tracking
        self.last_synced_id = {}  # Track last synced message ID per stream
        
        logger.info(f"🔄 MongoDB Sync Service initialized")
        logger.info(f"   MongoDB: {self.mongo_uri[:50]}.../{self.mongo_db_name}/{self.mongo_collection_name}")
        logger.info(f"   Redis: {redis_host}:{redis_port}")
        logger.info(f"   Sync interval: {sync_interval_seconds}s")
        logger.info(f"   Retention: PERMANENT (no auto-cleanup)")
    
    async def connect(self):
        """Connect to Redis and MongoDB."""
        # Redis
        self.redis_client = await aioredis.from_url(
            f"redis://{self.redis_host}:{self.redis_port}",
            decode_responses=True
        )
        await self.redis_client.ping()
        logger.info("✅ Connected to Redis")
        
        # MongoDB
        self.mongo_client = MongoClient(self.mongo_uri)
        db = self.mongo_client[self.mongo_db_name]
        self.collection = db[self.mongo_collection_name]
        
        # Create indexes for efficient queries (FinRL format)
        self.collection.create_index([('date', DESCENDING)])
        self.collection.create_index([('tic', ASCENDING)])
        self.collection.create_index([('date', DESCENDING), ('tic', ASCENDING)], unique=True)
        
        logger.info("✅ Connected to MongoDB")
        logger.info(f"   Total documents: {self.collection.count_documents({})}")
    
    async def sync_from_redis(self):
        """Sync new data from Redis processed stream to MongoDB."""
        try:
            stream_name = 'processed:price'
            
            # Get last synced ID for this stream
            last_id = self.last_synced_id.get(stream_name, '0-0')
            
            # Read new messages from Redis
            messages = await self.redis_client.xread(
                {stream_name: last_id},
                count=1000,  # Batch size
                block=1000  # Wait 1 second for new messages
            )
            
            if not messages:
                return 0
            
            synced_count = 0
            
            for stream, stream_messages in messages:
                for message_id, fields in stream_messages:
                    try:
                        # Parse message data
                        data = json.loads(fields.get('data', '{}'))
                        
                        # Extract components
                        metadata = data.get('metadata', {})
                        price_data = data.get('price_data', {})
                        momentum = data.get('momentum_indicators', {})
                        volatility = data.get('volatility_indicators', {})
                        trend = data.get('trend_indicators', {})
                        moving_averages = data.get('moving_averages', {})
                        
                        # Get ticker - try 'ticker' first, then 'company_name'
                        ticker = metadata.get('ticker') or metadata.get('company_name')
                        if not ticker:
                            logger.debug(f"Skipping message {message_id}: missing ticker")
                            self.last_synced_id[stream] = message_id
                            continue
                        
                        # Get timestamp - try 'timestamp' first, then use current time with 'date'
                        timestamp = metadata.get('timestamp')
                        if not timestamp:
                            # Use the date field + current time if no timestamp
                            date_str = metadata.get('date')
                            if date_str:
                                timestamp = f"{date_str}T{datetime.utcnow().strftime('%H:%M:%S')}"
                            else:
                                timestamp = datetime.utcnow().isoformat()
                        
                        # Parse timestamp
                        parsed_date = pd.to_datetime(timestamp)
                        if pd.isna(parsed_date):
                            logger.debug(f"Skipping message {message_id}: invalid timestamp")
                            self.last_synced_id[stream] = message_id
                            continue
                        
                        # Build MongoDB document in EXACT FinRL format
                        # Column order: date, tic, open, high, low, close, volume, 
                        #               macd, boll_ub, boll_lb, rsi_30, cci_30, dx_30, close_30_sma, close_60_sma
                        document = {
                            'date': parsed_date,
                            'tic': ticker,
                            'open': price_data.get('open', 0),
                            'high': price_data.get('high', 0),
                            'low': price_data.get('low', 0),
                            'close': price_data.get('close', 0),
                            'volume': price_data.get('volume', 0),
                            'macd': momentum.get('macd', {}).get('macd_line', 0),
                            'boll_ub': volatility.get('boll_ub', 0),
                            'boll_lb': volatility.get('boll_lb', 0),
                            'rsi_30': momentum.get('rsi_30', 50),
                            'cci_30': momentum.get('cci_30', 0),
                            'dx_30': trend.get('dx_30', 0),
                            'close_30_sma': moving_averages.get('close_30_sma', price_data.get('close', 0)),
                            'close_60_sma': moving_averages.get('close_60_sma', price_data.get('close', 0)),
                        }
                        
                        # Ensure timezone-naive datetime for MongoDB
                        if document['date'].tz is not None:
                            document['date'] = document['date'].tz_localize(None)
                        
                        # Insert or update (upsert based on date + tic for FinRL compatibility)
                        self.collection.update_one(
                            {'date': document['date'], 'tic': document['tic']},
                            {'$set': document},
                            upsert=True
                        )
                        
                        synced_count += 1
                    
                    except Exception as e:
                        logger.warning(f"⚠️  Error syncing message {message_id}: {e}")
                        continue
                    
                    # Update last synced ID
                    self.last_synced_id[stream] = message_id
            
            if synced_count > 0:
                logger.info(f"✅ Synced {synced_count} records from Redis to MongoDB")
            
            return synced_count
        
        except Exception as e:
            logger.error(f"❌ Sync error: {e}", exc_info=True)
            return 0
    
    async def get_historical_data(self, hours: int = 48) -> pd.DataFrame:
        """
        Get historical data from MongoDB.
        
        Args:
            hours: Number of hours to fetch (default: 48 hours for fine-tuning)
        
        Returns:
            DataFrame with historical market data
        """
        try:
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)
            
            cursor = self.collection.find({
                'date': {'$gte': cutoff_time}
            }).sort('date', ASCENDING)
            
            data = list(cursor)
            
            if len(data) == 0:
                logger.warning(f"⚠️  No data found in MongoDB for last {hours}h")
                return pd.DataFrame()
            
            df = pd.DataFrame(data)
            
            # Clean up
            if '_id' in df.columns:
                df = df.drop(columns=['_id'])
            
            # Ensure date is datetime
            df['date'] = pd.to_datetime(df['date'])
            
            # Filter to only complete timestamps (all 30 stocks)
            stock_dim = len(settings.symbols_list)
            timestamp_counts = df.groupby('date')['tic'].count()
            complete_timestamps = timestamp_counts[timestamp_counts == stock_dim].index
            df = df[df['date'].isin(complete_timestamps)]
            
            # Create day index
            df = df.sort_values(['date', 'tic']).reset_index(drop=True)
            unique_dates = sorted(df['date'].unique())
            date_to_day = {date: idx for idx, date in enumerate(unique_dates)}
            df['day'] = df['date'].map(date_to_day)
            
            logger.info(f"📊 Retrieved {len(df)} records ({len(unique_dates)} complete timestamps) from MongoDB")
            
            return df
        
        except Exception as e:
            logger.error(f"❌ Error retrieving historical data: {e}", exc_info=True)
            return pd.DataFrame()
    
    async def start_service(self):
        """Start the sync service (background task)."""
        logger.info("🚀 Starting MongoDB Sync Service")
        
        await self.connect()
        
        while True:
            try:
                # Sync new data
                await self.sync_from_redis()
                
                # Wait for next sync
                await asyncio.sleep(self.sync_interval)
            
            except Exception as e:
                logger.error(f"❌ Service error: {e}", exc_info=True)
                await asyncio.sleep(60)  # Wait 1 minute on error
    
    def close(self):
        """Close connections."""
        if self.mongo_client:
            self.mongo_client.close()
            logger.info("MongoDB connection closed")


def create_mongodb_sync_service(**kwargs):
    """Create MongoDB sync service instance."""
    return MongoDBSyncService(**kwargs)
