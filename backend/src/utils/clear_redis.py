#!/usr/bin/env python3
"""
Clear Redis Script
Deletes all data from Redis streams used by the AEGIS trading pipeline.
Use this to reset streaming data before running fresh backfill.
"""
import redis
import os
from dotenv import load_dotenv

def clear_redis():
    """Clear all data from Redis streams"""
    try:
        # Load environment variables
        load_dotenv()
        redis_host = os.getenv('REDIS_HOST', 'localhost')
        redis_port = int(os.getenv('REDIS_PORT', 6379))
        
        print("=" * 60)
        print("🗑️  CLEARING REDIS STREAMS")
        print("=" * 60)
        
        # Connect to Redis
        print(f"📡 Connecting to Redis at {redis_host}:{redis_port}...")
        client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        
        # Test connection
        client.ping()
        print("✅ Connected to Redis")
        
        # Define all streams to clear
        streams = [
            "raw:price-updates",
            "raw:news-articles",
            "raw:social",
            "processed:price",
            "processed:news",
            "processed:social",
            "processed:master-state",
            "finrl:predictions",
            "agent:results",
            "debate:results"
        ]
        
        print("\n📊 Checking stream lengths...")
        total_messages = 0
        stream_info = {}
        
        for stream in streams:
            try:
                length = client.xlen(stream)
                stream_info[stream] = length
                total_messages += length
                if length > 0:
                    print(f"   {stream}: {length:,} messages")
            except Exception as e:
                print(f"   {stream}: Not found or error ({e})")
        
        if total_messages == 0:
            print("\n✅ All Redis streams are already empty")
            return True
        
        print(f"\n📊 Total messages: {total_messages:,}")
        
        # Confirm deletion
        print(f"\n⚠️  WARNING: About to delete {total_messages:,} messages from {len(streams)} streams")
        response = input("Type 'yes' to confirm deletion: ")
        
        if response.lower() != 'yes':
            print("❌ Deletion cancelled")
            return False
        
        # Clear each stream
        print("\n🗑️  Clearing streams...")
        cleared_count = 0
        
        for stream in streams:
            if stream_info.get(stream, 0) > 0:
                try:
                    # XTRIM to length 0 (deletes all messages)
                    client.xtrim(stream, maxlen=0)
                    cleared_count += stream_info[stream]
                    print(f"   ✅ Cleared {stream}: {stream_info[stream]:,} messages")
                except Exception as e:
                    print(f"   ❌ Error clearing {stream}: {e}")
        
        print("\n" + "=" * 60)
        print("✅ REDIS CLEARED SUCCESSFULLY")
        print("=" * 60)
        print(f"📊 Total messages deleted: {cleared_count:,}")
        print(f"📊 Streams cleared: {len([s for s in streams if stream_info.get(s, 0) > 0])}")
        print("=" * 60)
        
        return True
        
    except redis.ConnectionError as e:
        print(f"\n❌ Could not connect to Redis: {e}")
        print("💡 Make sure Redis is running: docker-compose up -d redis")
        return False
    except Exception as e:
        print(f"\n❌ Error clearing Redis: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = clear_redis()
    exit(0 if success else 1)
