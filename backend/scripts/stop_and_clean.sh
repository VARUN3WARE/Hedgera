#!/bin/bash
# filepath: backend/scripts/stop_and_clean.sh

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

clear

echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${CYAN}            🛑 AEGIS Pipeline - Stop & Clean                   ${NC}"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# Check if pipeline is running
PIPELINE_PIDS=$(pgrep -f "run_pipeline.py")

if [ -z "$PIPELINE_PIDS" ]; then
    echo -e "${YELLOW}⚠️  No pipeline process found running${NC}"
    echo ""
else
    echo -e "${BLUE}📊 Found Pipeline Process(es):${NC}"
    echo ""
    for PID in $PIPELINE_PIDS; do
        RUNTIME=$(ps -p $PID -o etime= | tr -d ' ')
        echo -e "   PID: ${GREEN}$PID${NC} | Runtime: ${CYAN}$RUNTIME${NC}"
    done
    echo ""
    
    # Show final stats before stopping
    echo -e "${BLUE}📈 Final Statistics:${NC}"
    echo ""
    echo -e "   Raw Price Updates:    ${CYAN}$(redis-cli XLEN raw:price-updates 2>/dev/null || echo '0')${NC}"
    echo -e "   Raw News Articles:    ${CYAN}$(redis-cli XLEN raw:news-articles 2>/dev/null || echo '0')${NC}"
    echo -e "   Raw Social Data:      ${CYAN}$(redis-cli XLEN raw:social 2>/dev/null || echo '0')${NC}"
    echo -e "   Processed Master:     ${CYAN}$(redis-cli XLEN processed:master-state 2>/dev/null || echo '0')${NC}"
    echo -e "   FinRL Decisions:      ${CYAN}$(redis-cli XLEN finrl-decisions 2>/dev/null || echo '0')${NC}"
    echo ""
    
    # Ask for confirmation
    read -p "Stop pipeline and clean all data? (y/n) " -n 1 -r
    echo ""
    echo ""
    
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}Aborted. Pipeline still running.${NC}"
        exit 0
    fi
    
    echo -e "${YELLOW}Stopping pipeline...${NC}"
    pkill -f run_pipeline.py
    sleep 2
    
    # Force kill if still running
    REMAINING=$(pgrep -f "run_pipeline.py")
    if [ ! -z "$REMAINING" ]; then
        echo -e "${YELLOW}Force killing remaining processes...${NC}"
        pkill -9 -f run_pipeline.py
        sleep 1
    fi
    
    if pgrep -f "run_pipeline.py" > /dev/null; then
        echo -e "${RED}✗ Failed to stop pipeline${NC}"
        exit 1
    else
        echo -e "${GREEN}✓ Pipeline stopped${NC}"
    fi
fi

echo ""
echo -e "${BLUE}🗑️  Cleaning Redis Data...${NC}"
echo ""

# Check Redis
if ! redis-cli ping &> /dev/null; then
    echo -e "${YELLOW}⚠️  Redis not running - no data to clean${NC}"
else
    # Show what will be deleted
    TOTAL_KEYS=$(redis-cli DBSIZE | grep -o '[0-9]*')
    echo -e "   Total Redis keys: ${CYAN}$TOTAL_KEYS${NC}"
    
    if [ "$TOTAL_KEYS" -gt 0 ]; then
        echo ""
        echo -e "   Streams to delete:"
        for stream in raw:price-updates raw:news-articles raw:social processed:price processed:master-state finrl-decisions; do
            COUNT=$(redis-cli XLEN $stream 2>/dev/null || echo '0')
            if [ "$COUNT" -gt 0 ]; then
                echo -e "   • ${YELLOW}$stream${NC}: $COUNT entries"
            fi
        done
        echo ""
        
        read -p "Delete all Redis data? (y/n) " -n 1 -r
        echo ""
        echo ""
        
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo -e "${YELLOW}Flushing Redis...${NC}"
            redis-cli FLUSHALL > /dev/null 2>&1
            echo -e "${GREEN}✓ Redis cleaned${NC}"
        else
            echo -e "${YELLOW}Skipped Redis cleanup${NC}"
        fi
    else
        echo -e "${GREEN}✓ Redis already clean${NC}"
    fi
fi

echo ""
echo -e "${BLUE}📁 Log Files:${NC}"
echo ""

# Archive current log
if [ -f "pipeline_output.log" ]; then
    LOG_SIZE=$(du -h pipeline_output.log | cut -f1)
    echo -e "   Current log: ${CYAN}pipeline_output.log${NC} (${CYAN}$LOG_SIZE${NC})"
    echo ""
    
    read -p "Archive log to backend/logs/? (y/n) " -n 1 -r
    echo ""
    echo ""
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
        mkdir -p backend/logs
        mv pipeline_output.log "backend/logs/pipeline_${TIMESTAMP}.log"
        echo -e "${GREEN}✓ Log archived: backend/logs/pipeline_${TIMESTAMP}.log${NC}"
    else
        rm pipeline_output.log
        echo -e "${GREEN}✓ Log deleted${NC}"
    fi
else
    echo -e "${YELLOW}   No log file found${NC}"
fi

# Clean nohup.out if exists
if [ -f "nohup.out" ]; then
    rm nohup.out
    echo -e "${GREEN}✓ Cleaned nohup.out${NC}"
fi

echo ""
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}                    ✅ Cleanup Complete                        ${NC}"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo -e "${BLUE}System Status:${NC}"
echo ""

# Final verification
if pgrep -f "run_pipeline.py" > /dev/null; then
    echo -e "   Pipeline: ${RED}Still Running${NC} ⚠️"
else
    echo -e "   Pipeline: ${GREEN}Stopped${NC} ✓"
fi

if redis-cli ping &> /dev/null; then
    KEYS=$(redis-cli DBSIZE | grep -o '[0-9]*')
    echo -e "   Redis: ${GREEN}Running${NC} ($KEYS keys)"
else
    echo -e "   Redis: ${YELLOW}Not Running${NC}"
fi

if docker ps | grep redis-commander &> /dev/null; then
    echo -e "   Redis Commander: ${GREEN}Running${NC} (http://localhost:8085)"
else
    echo -e "   Redis Commander: ${YELLOW}Not Running${NC}"
fi

echo ""
echo -e "${CYAN}To restart the pipeline:${NC}"
echo -e "${GREEN}./backend/scripts/start_pipeline.sh${NC}"
echo ""