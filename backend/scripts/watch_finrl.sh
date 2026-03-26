#!/bin/bash
# filepath: backend/scripts/watch_finrl.sh
# Monitor pipeline logs for FinRL execution

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

LOG_FILE="pipeline_output_fixed.log"

# Check if log file exists
if [ ! -f "$LOG_FILE" ]; then
    echo -e "${RED}Error: $LOG_FILE not found${NC}"
    echo "Make sure the pipeline is running."
    exit 1
fi

clear
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${CYAN}              FinRL Execution Monitor                        ${NC}"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# Extract FinRL start time
FINRL_START=$(grep "Starting FinRL Cycle at" "$LOG_FILE" | head -1 | sed -n 's/.*at \(.*\)/\1/p')
if [ -n "$FINRL_START" ]; then
    echo -e "${GREEN}Pipeline Started:${NC} $FINRL_START"
    
    # Extract expected completion time from logs
    EXPECTED=$(grep "Data collection will complete at:" "$LOG_FILE" | head -1 | sed -n 's/.*at: \(.*\)/\1/p')
    
    if [ -n "$EXPECTED" ]; then
        echo -e "${YELLOW}Expected FinRL:${NC} $EXPECTED"
    fi
    echo ""
fi

# Check if FinRL has started data collection
if grep -q "Waiting 60 minutes for data collection" "$LOG_FILE"; then
    echo -e "${GREEN}✓${NC} FinRL service started - waiting for data collection"
else
    echo -e "${RED}✗${NC} FinRL service not started yet"
fi

# Check for progress updates
PROGRESS=$(grep "Data collection in progress" "$LOG_FILE" | tail -1 | awk '{print $NF}')
if [ -n "$PROGRESS" ]; then
    echo -e "${YELLOW}⏱${NC}  Data collection in progress - $PROGRESS remaining"
fi

# Check if data collection completed
if grep -q "Data collection period complete" "$LOG_FILE"; then
    echo -e "${GREEN}✓${NC} Data collection complete!"
    
    # Check if data was fetched
    TICKER_COUNT=$(grep "Fetched data for" "$LOG_FILE" | tail -1 | awk '{print $7}')
    if [ -n "$TICKER_COUNT" ]; then
        echo -e "${GREEN}✓${NC} Fetched data for $TICKER_COUNT tickers"
    fi
    
    # Check if model started
    if grep -q "Running FinRL Paper Trading Model" "$LOG_FILE"; then
        echo -e "${GREEN}✓${NC} FinRL model started"
        
        # Check for model success
        if grep -q "FinRL Model Complete" "$LOG_FILE"; then
            echo -e "${GREEN}✓${NC} FinRL model executed successfully!"
            
            # Extract ticker selection
            SELECTED=$(grep "Selected top" "$LOG_FILE" | tail -1 | awk '{for(i=7;i<=NF;i++) printf "%s ", $i; print ""}')
            if [ -n "$SELECTED" ]; then
                echo -e "${GREEN}✓${NC} Selected: $SELECTED"
            fi
            
            # Check if producers activated
            if grep -q "Activated news/social producers" "$LOG_FILE"; then
                echo -e "${GREEN}✓${NC} News/Social producers activated"
            fi
        else
            # Check for errors
            if grep -q "FinRL model error" "$LOG_FILE"; then
                echo -e "${RED}✗${NC} FinRL model error detected!"
                echo ""
                echo -e "${RED}Error Details:${NC}"
                grep -A 5 "FinRL model error" "$LOG_FILE" | tail -10
            fi
        fi
    fi
fi

echo ""
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${YELLOW}Tip:${NC} Run this script periodically or use:"
echo -e "      ${CYAN}watch -n 30 ./backend/scripts/watch_finrl.sh${NC}"
echo ""
echo -e "${YELLOW}Live logs:${NC} ${CYAN}tail -f $LOG_FILE | grep -i finrl${NC}"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}
"
