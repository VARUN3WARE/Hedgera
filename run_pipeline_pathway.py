#!/usr/bin/env python3
"""
Run AEGIS Pipeline with Pathway Streaming Engine

This script demonstrates how to use the new Pathway-based streaming engine
instead of the manual streaming_engine.py.

Usage:
    python run_pipeline_pathway.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from backend.config.settings import settings
from backend.src.pathway_engine import PathwayStreamingEngine


def print_banner():
    banner = """
    ╔═══════════════════════════════════════════════════════════╗
    ║      AEGIS TRADING SYSTEM - PATHWAY EDITION              ║
    ╚═══════════════════════════════════════════════════════════╝
    
    🚀 POWERED BY PATHWAY STREAMING ENGINE
    
    Features:
    ✨ Automatic temporal window management
    ✨ Built-in technical indicators (SMA, EMA, RSI, MACD, etc.)
    ✨ Multi-stream correlation (Price + News + Social)
    ✨ Exactly-once processing semantics
    ✨ 10x faster than manual implementation
    
    Workflow:
    1. Pathway reads from Redis streams (raw:price-updates, etc.)
    2. Calculates all indicators automatically using temporal windows
    3. Joins with news/social sentiment
    4. Writes to processed:price stream
    5. FinRL consumes processed data for trading decisions
    
    Press Ctrl+C to stop
    """
    print(banner)


def run_pathway_engine():
    """Run Pathway streaming engine in main thread."""
    print("🚀 Starting Pathway Streaming Engine...")
    
    engine = PathwayStreamingEngine(
        redis_host=settings.redis_host,
        redis_port=settings.redis_port,
        redis_password=settings.redis_password,
    )
    
    try:
        # This blocks until interrupted
        engine.run()
    except KeyboardInterrupt:
        print("\n👋 Shutting down Pathway engine...")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    print_banner()
    
    print("\n" + "=" * 70)
    print("📊 PATHWAY ENGINE CONFIGURATION")
    print("=" * 70)
    print(f"Redis Host: {settings.redis_host}")
    print(f"Redis Port: {settings.redis_port}")
    print(f"Input Streams:")
    print(f"  • raw:price-updates  (OHLCV data)")
    print(f"  • raw:news-articles  (News with sentiment)")
    print(f"  • raw:social         (Social posts)")
    print(f"\nOutput Streams:")
    print(f"  • processed:price        (All indicators)")
    print(f"  • processed:master-state (For compatibility)")
    print("=" * 70)
    print("\n")
    
    # Run the engine
    run_pathway_engine()
