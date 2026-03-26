#!/bin/bash
# Quick test of the updated 60-minute workflow
# This tests with 2-minute collection for faster verification

echo "════════════════════════════════════════════════════════════"
echo "Testing 60-Minute Workflow (2-min test mode)"
echo "════════════════════════════════════════════════════════════"
echo ""

# Stop any running pipeline
echo "1. Stopping existing pipeline..."
pkill -f "run_pipeline.py" 2>/dev/null
sleep 2

# Clear Redis
echo "2. Clearing Redis..."
redis-cli FLUSHALL >/dev/null 2>&1

# Update collection time to 2 minutes for testing
echo "3. Creating test pipeline script..."
cat > /tmp/test_60min_pipeline.py << 'EOF'
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from backend.src.orchestration.pipeline_main import AegisPipeline
import asyncio

async def main():
    pipeline = AegisPipeline()
    # Override collection time to 2 minutes for testing
    await pipeline.start_all()

if __name__ == "__main__":
    asyncio.run(main())
EOF

echo "4. Starting test pipeline..."
echo "   • Collection time: 2 minutes (for testing)"
echo "   • Price fetch: Every 5 minutes (all 30 tickers)"
echo "   • News/Social: Disabled until FinRL runs"
echo ""

cd /Users/rahulraj/PathwayPS
nohup python3 /tmp/test_60min_pipeline.py > /tmp/test_pipeline.log 2>&1 &
TEST_PID=$!

echo "5. Pipeline started (PID: $TEST_PID)"
echo ""
echo "════════════════════════════════════════════════════════════"
echo "Monitoring Timeline:"
echo "════════════════════════════════════════════════════════════"
echo "0:00 - Price producer active (30 tickers)"
echo "0:00 - News producer DISABLED (waiting for FinRL)"
echo "0:00 - Social producer DISABLED (waiting for FinRL)"
echo "2:00 - FinRL runs and selects 10 tickers"
echo "2:01 - News/Social producers ACTIVATED (10 tickers only)"
echo "════════════════════════════════════════════════════════════"
echo ""

# Monitor for 30 seconds
echo "6. Initial status (first 30 seconds)..."
sleep 30
echo ""
echo "Recent logs:"
tail -30 /tmp/test_pipeline.log | grep -E "producer|DISABLED|Activated|FinRL"
echo ""

echo "════════════════════════════════════════════════════════════"
echo "Test Running!"
echo "════════════════════════════════════════════════════════════"
echo ""
echo "To monitor real-time:"
echo "  tail -f /tmp/test_pipeline.log | grep -E 'producer|FinRL|Activated|News|Social'"
echo ""
echo "To check after 2 minutes:"
echo "  grep -E 'Activated|selected tickers' /tmp/test_pipeline.log"
echo ""
echo "To stop test:"
echo "  kill $TEST_PID"
echo ""
