#!/bin/bash

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# Get configuration - strip comments and whitespace
# FinRL wait time: 60 minutes (3600 seconds) - hardcoded in pipeline_main.py
FINRL_WAIT=3600
FINRL_WAIT_MIN=$((FINRL_WAIT / 60))

# Get fetch interval from .env
FETCH_INTERVAL=$(grep "^PRODUCER_FETCH_INTERVAL=" .env | cut -d'=' -f2 | cut -d'#' -f1 | tr -d ' ' | head -1)
if [ -z "$FETCH_INTERVAL" ] || [ "$FETCH_INTERVAL" -eq 0 ] 2>/dev/null; then
    FETCH_INTERVAL=300  # Default 5 minutes
fi
FETCH_INTERVAL_MIN=$((FETCH_INTERVAL / 60))

# Check if pipeline is running
if ! pgrep -f "run_pipeline.py" > /dev/null; then
    echo -e "${RED}✗ Pipeline not running!${NC}"
    echo ""
    echo -e "Start with: ${GREEN}./backend/scripts/start_pipeline.sh${NC}"
    exit 1
fi

PIPELINE_PID=$(pgrep -f "run_pipeline.py")

# Get start time from log
if [ -f "pipeline_output.log" ]; then
    # Try to extract from log first
    START_TIME=$(head -30 pipeline_output.log | grep -E "Pipeline starts|Starting" | head -1 | cut -d' ' -f1-2)
    if [ -z "$START_TIME" ]; then
        # Fallback to file modification time
        START_TIME=$(stat -f "%Sm" -t "%Y-%m-%d %H:%M:%S" pipeline_output.log 2>/dev/null || stat -c "%y" pipeline_output.log 2>/dev/null | cut -d'.' -f1)
    fi
else
    START_TIME=$(date '+%Y-%m-%d %H:%M:%S')
fi

# Calculate expected FinRL time (try multiple date command formats)
if date -v+1S > /dev/null 2>&1; then
    # macOS
    FINRL_TIME=$(date -v+${FINRL_WAIT}S -j -f "%Y-%m-%d %H:%M:%S" "$START_TIME" +"%H:%M:%S" 2>/dev/null)
else
    # Linux
    FINRL_TIME=$(date -d "$START_TIME + $FINRL_WAIT seconds" +"%H:%M:%S" 2>/dev/null)
fi

if [ -z "$FINRL_TIME" ]; then
    FINRL_TIME="N/A"
fi

# Header
clear
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${CYAN}                🔍 AEGIS Pipeline - Live Monitor (60+ Min Test)               ${NC}"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo -e "${BOLD}Pipeline Info:${NC}"
echo -e "  PID: ${GREEN}$PIPELINE_PID${NC} | Started: ${GREEN}$START_TIME${NC} | FinRL Expected: ${YELLOW}$FINRL_TIME${NC}"
echo ""
echo -e "${BOLD}Configuration:${NC}"
echo -e "  Price Fetch: Every ${CYAN}${FETCH_INTERVAL_MIN} min${NC} | FinRL Wait: ${CYAN}${FINRL_WAIT_MIN} min${NC}"
echo ""
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo -e "${YELLOW}Monitoring... (Updates every 60 seconds. Press Ctrl+C to exit)${NC}"
echo ""

# Function to format large numbers
format_number() {
    if [ "$1" -ge 1000 ]; then
        echo "$(($1 / 1000))k+"
    else
        echo "$1"
    fi
}

# Function to get percentage bar
get_bar() {
    local current=$1
    local total=$2
    local width=20
    
    if [ $total -eq 0 ]; then
        echo "[                    ] 0%"
        return
    fi
    
    local percent=$((current * 100 / total))
    local filled=$((percent * width / 100))
    local empty=$((width - filled))
    
    printf "["
    printf "${GREEN}%${filled}s${NC}" | tr ' ' '█'
    printf "%${empty}s" | tr ' ' '░'
    printf "] ${CYAN}%3d%%${NC}" $percent
}

# Initialize counters
ITERATION=0
START_EPOCH=$(date +%s)

# Monitor loop
while true; do
    ITERATION=$((ITERATION + 1))
    CURRENT_TIME=$(date '+%H:%M:%S')
    CURRENT_EPOCH=$(date +%s)
    ELAPSED=$((CURRENT_EPOCH - START_EPOCH))
    ELAPSED_MIN=$((ELAPSED / 60))
    
    # Check if pipeline still running
    if ! pgrep -f "run_pipeline.py" > /dev/null; then
        echo ""
        echo -e "${RED}✗ Pipeline stopped unexpectedly!${NC}"
        echo ""
        echo -e "Check logs: ${YELLOW}tail -50 pipeline_output.log${NC}"
        exit 1
    fi
    
    # Get stream counts
    RAW_PRICE=$(redis-cli XLEN raw:price-updates 2>/dev/null || echo '0')
    RAW_NEWS=$(redis-cli XLEN raw:news-articles 2>/dev/null || echo '0')
    RAW_SOCIAL=$(redis-cli XLEN raw:social 2>/dev/null || echo '0')
    PROCESSED=$(redis-cli XLEN processed:master-state 2>/dev/null || echo '0')
    FINRL_DEC=$(redis-cli XLEN finrl-decisions 2>/dev/null || echo '0')
    
    # Calculate progress
    PROGRESS_BAR=$(get_bar $ELAPSED_MIN $FINRL_WAIT_MIN)
    
    # Calculate expected data points (avoid division by zero)
    if [ $FETCH_INTERVAL_MIN -gt 0 ]; then
        EXPECTED_PRICE=$((ELAPSED_MIN / FETCH_INTERVAL_MIN * 30))
        if [ $EXPECTED_PRICE -eq 0 ]; then EXPECTED_PRICE=30; fi
    else
        EXPECTED_PRICE=30
    fi
    
    # Status indicators
    if [ $FINRL_DEC -gt 0 ]; then
        FINRL_STATUS="${GREEN}✓ COMPLETED${NC}"
        FINRL_INDICATOR="🎯"
    elif [ $ELAPSED_MIN -ge $FINRL_WAIT_MIN ]; then
        FINRL_STATUS="${YELLOW}⏳ RUNNING${NC}"
        FINRL_INDICATOR="🔄"
    else
        REMAINING=$((FINRL_WAIT_MIN - ELAPSED_MIN))
        FINRL_STATUS="${BLUE}⏰ ${REMAINING}min remaining${NC}"
        FINRL_INDICATOR="⏳"
    fi
    
    # Clear previous update (keep header)
    if [ $ITERATION -gt 1 ]; then
        tput cuu 12  # Move cursor up 12 lines
        tput ed      # Clear to end of screen
    fi
    
    # Display update
    echo -e "${BOLD}┌─ Update #$ITERATION ─────────────────────────────────────────────────────────────────┐${NC}"
    echo -e "${BOLD}│${NC} ${BLUE}Time:${NC} $CURRENT_TIME | ${BLUE}Elapsed:${NC} ${CYAN}${ELAPSED_MIN}${NC}/${FINRL_WAIT_MIN} min $PROGRESS_BAR"
    echo -e "${BOLD}└──────────────────────────────────────────────────────────────────────────────────────┘${NC}"
    echo ""
    
    # Data streams
    echo -e "${BOLD}📊 Data Streams:${NC}"
    printf "   %-25s ${CYAN}%6s${NC} entries" "Raw Price Updates" "$(format_number $RAW_PRICE)"
    if [ $RAW_PRICE -ge $EXPECTED_PRICE ]; then
        echo -e " ${GREEN}✓${NC}"
    else
        echo -e " ${YELLOW}(expected: $EXPECTED_PRICE)${NC}"
    fi
    
    printf "   %-25s ${CYAN}%6s${NC} entries" "Processed Master State" "$(format_number $PROCESSED)"
    if [ $PROCESSED -gt 0 ]; then
        echo -e " ${GREEN}✓${NC}"
    else
        echo -e " ${YELLOW}⏳${NC}"
    fi
    
    printf "   %-25s ${CYAN}%6s${NC} entries" "Raw News Articles" "$(format_number $RAW_NEWS)"
    if [ $FINRL_DEC -gt 0 ]; then
        if [ $RAW_NEWS -gt 0 ]; then
            echo -e " ${GREEN}✓ (10 tickers)${NC}"
        else
            echo -e " ${YELLOW}⏳${NC}"
        fi
    else
        echo -e " ${BLUE}(post-FinRL)${NC}"
    fi
    
    printf "   %-25s ${CYAN}%6s${NC} entries" "Raw Social Data" "$(format_number $RAW_SOCIAL)"
    if [ $FINRL_DEC -gt 0 ]; then
        if [ $RAW_SOCIAL -gt 0 ]; then
            echo -e " ${GREEN}✓ (10 tickers)${NC}"
        else
            echo -e " ${YELLOW}⏳${NC}"
        fi
    else
        echo -e " ${BLUE}(post-FinRL)${NC}"
    fi
    
    echo ""
    
    # FinRL Status
    echo -e "${BOLD}🤖 FinRL Status:${NC}"
    echo -e "   $FINRL_INDICATOR Status: $FINRL_STATUS"
    
    if [ $FINRL_DEC -gt 0 ]; then
        # Get the actual decision
        DECISION=$(redis-cli XREVRANGE finrl-decisions + - COUNT 1 2>/dev/null)
        if [ ! -z "$DECISION" ]; then
            SELECTED=$(echo "$DECISION" | grep -o '"selected_tickers":\[[^]]*\]' | grep -o '\[.*\]' | tr -d '[]"' | tr ',' ' ')
            if [ ! -z "$SELECTED" ]; then
                echo -e "   ${GREEN}✓ Selected Tickers:${NC} ${CYAN}$SELECTED${NC}"
            fi
        fi
    fi
    
    echo ""
    
    # Recent activity
    echo -e "${BOLD}📝 Recent Activity:${NC}"
    if [ -f "pipeline_output.log" ]; then
        tail -3 pipeline_output.log | sed 's/^/   /' | sed "s/INFO/${GREEN}INFO${NC}/g" | sed "s/ERROR/${RED}ERROR${NC}/g" | sed "s/WARNING/${YELLOW}WARNING${NC}/g"
    fi
    
    echo ""
    
    # Wait 60 seconds
    sleep 60
done