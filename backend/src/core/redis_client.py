"""Redis Client with Stream Support"""
import redis
from typing import Optional, Dict, Any, List
import json


class RedisClient:
    """Basic Redis client."""
    def __init__(self, host='localhost', port=6379, db=0):
        self.client = redis.Redis(host=host, port=port, db=db, decode_responses=True)
    
    def get(self, key):
        return self.client.get(key)
    
    def set(self, key, value):
        return self.client.set(key, value)


class RedisStreamClient:
    """Redis client with stream support for producers."""
    
    def __init__(self, host='localhost', port=6379, db=0, password: Optional[str] = None):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.client = redis.Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=True
        )
    
    def ping(self) -> bool:
        """Check Redis connection."""
        try:
            return self.client.ping()
        except Exception:
            return False
    
    def xadd(self, stream_name: str, fields: Dict[str, Any], maxlen: Optional[int] = None) -> str:
        """Add entry to Redis stream."""
        # Convert dict values to JSON strings
        serialized_fields = {}
        for key, value in fields.items():
            if isinstance(value, (dict, list)):
                serialized_fields[key] = json.dumps(value)
            else:
                serialized_fields[key] = str(value)
        
        return self.client.xadd(
            stream_name,
            serialized_fields,
            maxlen=maxlen
        )
    
    def xlen(self, stream_name: str) -> int:
        """Get stream length."""
        return self.client.xlen(stream_name)
    
    def xrange(self, stream_name: str, start: str = '-', end: str = '+', count: Optional[int] = None):
        """Read stream range."""
        return self.client.xrange(stream_name, start, end, count)
    
    def xrevrange(self, stream_name: str, start: str = '+', end: str = '-', count: Optional[int] = None):
        """Read stream in reverse."""
        return self.client.xrevrange(stream_name, start, end, count)
    
    def delete(self, *keys):
        """Delete keys."""
        if keys:
            return self.client.delete(*keys)
        return 0
    
    def close(self):
        """Close connection."""
        if self.client:
            self.client.close()

