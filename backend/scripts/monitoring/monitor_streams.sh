#!/bin/bash
# Redis Streams Monitoring Script
# Monitor all AEGIS Redis streams for data flow verification

echo "🔍 AEGIS Redis Streams Monitoring"
echo "=================================="
echo ""

# Connect to Redis and show stream info
redis-cli -h localhost -p 6379 <<EOF

ECHO "📊 RAW DATA STREAMS"
ECHO "==================="
ECHO ""

ECHO "🔹 raw:price-updates"
XLEN raw:price-updates
XINFO STREAM raw:price-updates
ECHO ""

ECHO "🔹 raw:news-articles"
XLEN raw:news-articles
XINFO STREAM raw:news-articles
ECHO ""

ECHO "🔹 raw:social"
XLEN raw:social
XINFO STREAM raw:social
ECHO ""

ECHO "📊 PROCESSED DATA STREAMS"
ECHO "========================="
ECHO ""

ECHO "🔹 processed:price"
XLEN processed:price
XINFO STREAM processed:price
ECHO ""

ECHO "🔹 processed:master-state"
XLEN processed:master-state
XINFO STREAM processed:master-state
ECHO ""

ECHO "🤖 FINRL DECISION STREAM"
ECHO "========================"
ECHO ""

ECHO "🔹 finrl-decisions"
XLEN finrl-decisions
XINFO STREAM finrl-decisions
ECHO ""

ECHO "📋 SAMPLE DATA FROM EACH STREAM"
ECHO "================================"
ECHO ""

ECHO "🔹 Latest raw:price-updates (1 entry):"
XREVRANGE raw:price-updates + - COUNT 1
ECHO ""

ECHO "🔹 Latest raw:news-articles (1 entry):"
XREVRANGE raw:news-articles + - COUNT 1
ECHO ""

ECHO "🔹 Latest raw:social (1 entry):"
XREVRANGE raw:social + - COUNT 1
ECHO ""

ECHO "🔹 Latest processed:price (1 entry):"
XREVRANGE processed:price + - COUNT 1
ECHO ""

ECHO "🔹 Latest processed:master-state (1 entry):"
XREVRANGE processed:master-state + - COUNT 1
ECHO ""

ECHO "🔹 Latest finrl-decisions (1 entry):"
XREVRANGE finrl-decisions + - COUNT 1
ECHO ""

EOF

echo ""
echo "✅ Monitoring complete!"
echo ""
echo "To monitor in real-time, run:"
echo "  watch -n 5 'redis-cli XLEN raw:price-updates && redis-cli XLEN processed:price && redis-cli XLEN finrl-decisions'"
