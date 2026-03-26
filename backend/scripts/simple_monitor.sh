#!/bin/bash
# Simple Production Test Monitor
# Monitors pipeline every minute until FinRL runs

echo "🚀 AEGIS Production Test Monitor"
echo "=================================="
echo "Started: $(date '+%H:%M:%S')"
echo ""

for i in {1..70}; do
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "⏱️  Minute: $i | Time: $(date '+%H:%M:%S')"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    raw_price=$(redis-cli XLEN raw:price-updates 2>/dev/null || echo 0)
    processed=$(redis-cli XLEN processed:price 2>/dev/null || echo 0)
    finrl=$(redis-cli XLEN finrl-decisions 2>/dev/null || echo 0)
    
    printf "  raw:price-updates:  %4d\n" $raw_price
    printf "  processed:price:    %4d\n" $processed
    printf "  finrl-decisions:    %4d\n" $finrl
    
    if [ "$finrl" -gt 0 ]; then
        echo ""
        echo "🎉 FINRL DECISION DETECTED!"
        echo ""
        redis-cli XREVRANGE finrl-decisions + - COUNT 1 | head -20
        break
    fi
    
    echo ""
    sleep 60
done

echo ""
echo "✅ Monitoring complete: $(date '+%H:%M:%S')"
