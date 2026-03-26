#!/bin/bash
# Continuous Pipeline Monitoring Script
# Monitors AEGIS pipeline until FinRL runs

echo "🔍 AEGIS Pipeline Monitoring"
echo "============================="
echo "Started at: $(date)"
echo ""
echo "Monitoring pipeline... Press Ctrl+C to stop"
echo ""

# Function to display stream counts
show_status() {
    local elapsed=$1
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "⏱️  Time Elapsed: ${elapsed} minutes"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    echo "📊 RAW DATA STREAMS:"
    printf "  %-25s %s\n" "raw:price-updates:" "$(redis-cli XLEN raw:price-updates 2>/dev/null || echo 0) entries"
    printf "  %-25s %s\n" "raw:news-articles:" "$(redis-cli XLEN raw:news-articles 2>/dev/null || echo 0) entries"
    printf "  %-25s %s\n" "raw:social:" "$(redis-cli XLEN raw:social 2>/dev/null || echo 0) entries"
    echo ""
    echo "⚙️  PROCESSED STREAMS:"
    printf "  %-25s %s\n" "processed:price:" "$(redis-cli XLEN processed:price 2>/dev/null || echo 0) entries"
    printf "  %-25s %s\n" "processed:master-state:" "$(redis-cli XLEN processed:master-state 2>/dev/null || echo 0) entries"
    echo ""
    echo "🤖 FINRL DECISIONS:"
    local finrl_count=$(redis-cli XLEN finrl-decisions 2>/dev/null || echo 0)
    printf "  %-25s %s\n" "finrl-decisions:" "$finrl_count entries"
    
    if [ "$finrl_count" -gt 0 ]; then
        echo ""
        echo "✅ FinRL HAS RUN! Fetching decision..."
        redis-cli XREVRANGE finrl-decisions + - COUNT 1 2>/dev/null | grep -A1 "selected_tickers" | tail -1 | cut -d'"' -f2
    fi
    
    echo ""
    echo "📝 Recent Log Entries:"
    tail -3 test_run.log 2>/dev/null | sed 's/^/  /'
    echo ""
}

# Monitor every minute
start_time=$(date +%s)
iteration=0

while true; do
    current_time=$(date +%s)
    elapsed=$(( ($current_time - $start_time) / 60 ))
    
    show_status $elapsed
    
    # Check if FinRL has run
    finrl_count=$(redis-cli XLEN finrl-decisions 2>/dev/null || echo 0)
    if [ "$finrl_count" -gt 0 ]; then
        echo ""
        echo "🎉 FinRL DECISION DETECTED! Pipeline test complete!"
        echo ""
        echo "Final Summary:"
        show_status $elapsed
        break
    fi
    
    # Sleep for 1 minute
    sleep 60
    iteration=$((iteration + 1))
done

echo ""
echo "✅ Monitoring complete at: $(date)"
echo ""
