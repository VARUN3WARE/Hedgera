#!/bin/bash
# Monitor FinRL 10-minute test run
# Shows countdown and checks for FinRL decision output

echo "======================================"
echo "🔍 FinRL 10-Minute Test Monitor"
echo "======================================"
echo ""

# Calculate end time (10 minutes from now)
if [[ "$OSTYPE" == "darwin"* ]]; then
    end_time=$(date -v+10M "+%H:%M:%S")
else
    end_time=$(date -d '+10 minutes' "+%H:%M:%S")
fi

echo "⏰ FinRL will run at approximately: $end_time"
echo ""

# Monitor for 11 minutes (10 min wait + 1 min buffer)
for i in {1..11}; do
    current_time=$(date "+%H:%M:%S")
    echo "[$current_time] Minute $i/11 - Checking status..."
    
    # Check if FinRL decision exists
    decision_count=$(redis-cli XLEN finrl-decisions 2>/dev/null || echo "0")
    
    if [ "$decision_count" != "0" ]; then
        echo ""
        echo "✅ FinRL DECISION FOUND!"
        echo "====================================="
        echo "📊 Decision count: $decision_count"
        echo ""
        echo "Latest decision:"
        redis-cli XREVRANGE finrl-decisions + - COUNT 1
        echo ""
        echo "✅ Test PASSED - FinRL ran successfully!"
        exit 0
    fi
    
    # Show stream status
    raw_count=$(redis-cli XLEN raw:price-updates 2>/dev/null || echo "0")
    processed_count=$(redis-cli XLEN processed:master-state 2>/dev/null || echo "0")
    
    echo "   📥 Raw data: $raw_count | 📊 Processed: $processed_count | 🤖 FinRL decisions: $decision_count"
    
    # Check logs for FinRL activity
    if tail -20 /Users/rahulraj/PathwayPS/pipeline_output.log 2>/dev/null | grep -q "FinRL"; then
        echo "   📝 FinRL activity detected in logs"
        tail -5 /Users/rahulraj/PathwayPS/pipeline_output.log | grep "FinRL" | head -1
    fi
    
    echo ""
    sleep 60
done

echo ""
echo "⚠️  11 minutes elapsed - checking final status..."
decision_count=$(redis-cli XLEN finrl-decisions 2>/dev/null || echo "0")

if [ "$decision_count" != "0" ]; then
    echo "✅ FinRL decision found!"
    redis-cli XREVRANGE finrl-decisions + - COUNT 1
    exit 0
else
    echo "❌ No FinRL decision found after 11 minutes"
    echo ""
    echo "Checking error logs..."
    tail -50 /Users/rahulraj/PathwayPS/backend/logs/app/finrl_service_errors.log 2>/dev/null || echo "No error log found"
    exit 1
fi
