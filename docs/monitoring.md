# Monitoring

### Real-time Monitoring

**Script**: `monitor_pipeline_detailed.py`

```bash
python monitor_pipeline_detailed.py
```

Monitors:

- Redis stream lengths (raw, processed)
- FinRL decision count
- Historical data range
- MongoDB status
- Real-time data capture counts

Sample output:

```
PIPELINE STATISTICS
Runtime: 65.2 minutes

REDIS STREAMS:
  Raw Price Stream: 1,950 entries
  Processed Stream: 1,950 entries
  FinRL Decisions: 1 runs
  Historical Range: 2024-01-13 -> 2024-01-15 (2.1 days)

REAL-TIME MONITORING (this session):
  Price Updates Captured: 195
  Processed Data Captured: 195
  FinRL Decisions Captured: 1
  Unique Tickers Seen: 30

MONGODB STATUS:
  Connected (21,600 docs)
```

### Log Files

#### Pipeline Logs

- `agent_logs/enhanced_TIMESTAMP/full_pipeline_enhanced.log`: Main pipeline log
- `backend/logs/pipeline_TIMESTAMP.log`: Component-level logs
- `pipeline_output.log`: Latest run output

#### Explainability Logs

- `logs/explainability/decisions_YYYYMMDD.jsonl`: JSONL format
  - One line per decision
  - Contains SHAP/LIME explanations
  - Top features logged

#### Trade Logs

- `agent_logs/enhanced_TIMESTAMP/cycle_N/08_trade_execution.json`
- `backend/logs/trades_YYYYMMDD.json`

### Dashboard (Optional)

**File**: `dashboard_bridge.py`

WebSocket server for real-time dashboard updates:

```bash
uvicorn dashboard_bridge:app --host 0.0.0.0 --port 8001
```

Features:

- Auto-sends JSON files from `agent_logs/`
- WebSocket endpoint: `ws://localhost:8001/ws`
- Manual file trigger: POST `/send_file`
- Pipeline control: POST `/start_pipeline`, `/stop_pipeline`
