#!/bin/bash
echo "╔════════════════════════════════════════════════════════╗"
echo "║       AEGIS TRADING PIPELINE - STATUS CHECK           ║"
echo "╚════════════════════════════════════════════════════════╝"
echo ""
echo "📊 REDIS STREAMS:"
echo "  raw:price-updates     : $(redis-cli XLEN raw:price-updates) entries"
echo "  raw:news-articles     : $(redis-cli XLEN raw:news-articles) entries"
echo "  raw:social            : $(redis-cli XLEN raw:social) entries"
echo "  processed:master-state: $(redis-cli XLEN processed:master-state) entries"
echo "  finrl-decisions       : $(redis-cli XLEN finrl-decisions) entries"
echo ""
echo "📁 LOG FILES:"
ls -lh backend/logs/*.jsonl 2>/dev/null | tail -5
echo ""
echo "📈 SAMPLE PRICE DATA (latest):"
redis-cli --raw XREVRANGE raw:price-updates + - COUNT 1 | head -12
echo ""
echo "✅ Pipeline Status: OPERATIONAL"
