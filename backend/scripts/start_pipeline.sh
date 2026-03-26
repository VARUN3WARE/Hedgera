#!/bin/bash
# filepath: backend/scripts/start_pipeline.sh

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

clear

echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${CYAN}          🚀 AEGIS Trading Pipeline - Production Start        ${NC}"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# Navigate to project root
cd "$PROJECT_ROOT" || exit 1

# Check if pipeline is already running
if pgrep -f "run_pipeline.py" > /dev/null; then
    echo -e "${YELLOW}⚠️  Warning: Pipeline is already running!${NC}"
    echo ""
    echo -e "   PID: ${GREEN}$(pgrep -f run_pipeline.py)${NC}"
    echo ""
    read -p "   Stop existing pipeline and restart? (y/n) " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}   Stopping existing pipeline...${NC}"
        pkill -f run_pipeline.py
        sleep 2
    else
        echo -e "${RED}   Aborted. Use './backend/scripts/stop_and_clean.sh' first.${NC}"
        exit 1
    fi
fi

# Pre-flight checks
echo -e "${BLUE}📋 Pre-flight Checks:${NC}"
echo ""

# Check Python
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}   ✗ Python3 not found${NC}"
    exit 1
fi
echo -e "${GREEN}   ✓ Python3: $(python3 --version)${NC}"

# Check Redis
if ! redis-cli ping &> /dev/null; then
    echo -e "${RED}   ✗ Redis not running${NC}"
    echo -e "${YELLOW}   Starting Redis...${NC}"
    redis-server --daemonize yes
    sleep 2
    if ! redis-cli ping &> /dev/null; then
        echo -e "${RED}   Failed to start Redis. Start manually: redis-server${NC}"
        exit 1
    fi
fi
echo -e "${GREEN}   ✓ Redis: $(redis-cli ping)${NC}"

# Check Redis Commander
if ! docker ps | grep redis-commander &> /dev/null; then
    echo -e "${YELLOW}   ⚠ Redis Commander not running${NC}"
    echo -e "${YELLOW}   Starting Redis Commander...${NC}"
    docker run --rm --name redis-commander \
      -d \
      -p 8085:8081 \
      --add-host host.docker.internal:host-gateway \
      -e REDIS_HOSTS=local:host.docker.internal:6379 \
      rediscommander/redis-commander:latest &> /dev/null
    sleep 2
fi
if docker ps | grep redis-commander &> /dev/null; then
    echo -e "${GREEN}   ✓ Redis Commander: http://localhost:8085${NC}"
else
    echo -e "${YELLOW}   ⚠ Redis Commander not available (optional)${NC}"
fi

# Check environment variables
if [ ! -f ".env" ]; then
    echo -e "${RED}   ✗ .env file not found${NC}"
    exit 1
fi
echo -e "${GREEN}   ✓ Environment: .env loaded${NC}"

# Check configuration
echo ""
echo -e "${BLUE}⚙️  Configuration:${NC}"
echo ""

# Extract values and strip comments/whitespace
FETCH_INTERVAL=$(grep "^PRODUCER_FETCH_INTERVAL=" .env | cut -d'=' -f2 | cut -d'#' -f1 | tr -d ' ' | head -1)
if [ -z "$FETCH_INTERVAL" ]; then
    FETCH_INTERVAL=300  # Default 5 minutes
fi
echo -e "   Price Fetch Interval: ${CYAN}${FETCH_INTERVAL}s ($(($FETCH_INTERVAL / 60)) minutes)${NC}"

# FinRL wait time: 60 minutes by default (hardcoded in pipeline_main.py)
FINRL_WAIT=3600
echo -e "   FinRL Collection Time: ${CYAN}${FINRL_WAIT}s ($(($FINRL_WAIT / 60)) minutes)${NC}"

# Count tickers from SYMBOLS variable
TICKERS=$(grep "^SYMBOLS=" .env | cut -d'=' -f2 | tr ',' '\n' | wc -l | tr -d ' ')
if [ -z "$TICKERS" ] || [ "$TICKERS" -eq 0 ]; then
    TICKERS=30  # Default
fi
echo -e "   Total Tickers: ${CYAN}${TICKERS}${NC}"

echo ""
echo -e "${BLUE}📊 Expected Timeline (60+ Minute Test):${NC}"
echo ""
START_TIME=$(date +"%H:%M:%S")
FINRL_TIME=$(date -v+${FINRL_WAIT}S +"%H:%M:%S" 2>/dev/null || date -d "+${FINRL_WAIT} seconds" +"%H:%M:%S" 2>/dev/null)

echo -e "   ${GREEN}$START_TIME${NC} │ Pipeline starts"
echo -e "   ${GREEN}$START_TIME${NC} │ ├─ Price data fetch (30 tickers)"
echo -e "   ${GREEN}$START_TIME${NC} │ ├─ Data processing begins"
echo -e "   ${GREEN}$START_TIME${NC} │ └─ Indicators calculated"
echo -e "            │"
echo -e "   Every 5 min │ Price fetch → Process → Update"
echo -e "            │"
echo -e "   ${YELLOW}$FINRL_TIME${NC} │ FinRL runs (after 60 minutes)"
echo -e "            │ ├─ Analyzes all collected data"
echo -e "            │ ├─ Selects top 10 tickers"
echo -e "            │ └─ Publishes decisions"
echo -e "            │"
echo -e "   Post-FinRL  │ News/Social active (10 tickers only)"
echo ""

# Backup previous logs
if [ -f "pipeline_output.log" ]; then
    TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
    mv pipeline_output.log "backend/logs/pipeline_${TIMESTAMP}.log"
    echo -e "${YELLOW}   Previous log backed up to: backend/logs/pipeline_${TIMESTAMP}.log${NC}"
    echo ""
fi

# Start the pipeline
echo -e "${GREEN}🚀 Starting Pipeline...${NC}"
echo ""

nohup python3 run_pipeline.py > pipeline_output.log 2>&1 &
PIPELINE_PID=$!

# Wait and verify
sleep 3

if ps -p $PIPELINE_PID > /dev/null; then
    echo -e "${GREEN}✅ Pipeline Started Successfully!${NC}"
    echo ""
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}                    Pipeline Information                      ${NC}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo -e "   ${BLUE}Process ID:${NC}       ${GREEN}$PIPELINE_PID${NC}"
    echo -e "   ${BLUE}Started At:${NC}       ${GREEN}$(date '+%Y-%m-%d %H:%M:%S')${NC}"
    echo -e "   ${BLUE}Log File:${NC}         ${GREEN}pipeline_output.log${NC}"
    echo -e "   ${BLUE}Test Duration:${NC}    ${GREEN}60+ minutes${NC}"
    echo -e "   ${BLUE}FinRL Expected:${NC}   ${GREEN}$FINRL_TIME${NC}"
    echo ""
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}                    Monitoring Commands                       ${NC}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo -e "   ${YELLOW}View Live Logs:${NC}"
    echo -e "   ${CYAN}tail -f pipeline_output.log${NC}"
    echo ""
    echo -e "   ${YELLOW}Monitor Progress:${NC}"
    echo -e "   ${CYAN}./backend/scripts/monitor_live.sh${NC}"
    echo ""
    echo -e "   ${YELLOW}Quick Status:${NC}"
    echo -e "   ${CYAN}./backend/scripts/quick_status.sh${NC}"
    echo ""
    echo -e "   ${YELLOW}Redis Commander:${NC}"
    echo -e "   ${CYAN}http://localhost:8085${NC}"
    echo ""
    echo -e "   ${YELLOW}Stop Pipeline:${NC}"
    echo -e "   ${CYAN}./backend/scripts/stop_and_clean.sh${NC}"
    echo ""
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    
    # Show first few log lines
    echo -e "${BLUE}📝 Initial Log Output:${NC}"
    echo ""
    sleep 2
    tail -15 pipeline_output.log | sed 's/^/   /'
    echo ""
    echo -e "${GREEN}Pipeline is running in the background. Monitor with the commands above.${NC}"
    echo ""
    
else
    echo -e "${RED}✗ Failed to start pipeline${NC}"
    echo ""
    echo -e "${YELLOW}Check the log file for errors:${NC}"
    cat pipeline_output.log
    exit 1
fi