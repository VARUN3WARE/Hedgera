#!/bin/bash
# Quick status check for FinRL test

echo "🔍 AEGIS Pipeline Status"
echo "========================"
echo ""

# Get current time
echo "⏰ Current time: $(date '+%H:%M:%S')"
echo ""

# Check streams
echo "📊 Redis Streams:"
echo "   Raw price updates:  $(redis-cli XLEN raw:price-updates)"
echo "   Processed data:     $(redis-cli XLEN processed:master-state)"
echo "   FinRL decisions:    $(redis-cli XLEN finrl-decisions)"
echo ""

# Check if FinRL has decisions
decision_count=$(redis-cli XLEN finrl-decisions 2>/dev/null || echo "0")
if [ "$decision_count" != "0" ]; then
    echo "✅ FinRL Decision Found!"
    echo ""
    echo "Latest decision:"
    redis-cli XREVRANGE finrl-decisions + - COUNT 1
    echo ""
else
    echo "⏳ Waiting for FinRL decision..."
    echo ""
    # Show when FinRL will run
    tail -200 /Users/rahulraj/PathwayPS/pipeline_output.log | grep "complete at" | tail -1
    echo ""
fi

# Show recent FinRL activity
echo "📝 Recent FinRL logs:"
tail -50 /Users/rahulraj/PathwayPS/pipeline_output.log | grep -i finrl | tail -5
