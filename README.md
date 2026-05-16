# AEGIS Trading System

## Overview

AEGIS is an automated trading system that combines reinforcement learning (FinRL), multi-agent analysis, and real-time market data processing to make intelligent trading decisions. The system uses a modular architecture with Redis for streaming, MongoDB for persistence, and multiple specialized agents for comprehensive market analysis.

## Table of Contents

1. System Architecture
2. Prerequisites
3. Installation
4. Configuration
5. Quick Start
6. Core Components
7. Pipeline Workflows
8. Testing
9. Monitoring
10. Docker Deployment
11. API Documentation
12. Troubleshooting

## Developer inventory (refactor Phase 0)

Structured runbook, pipeline versus engine mapping, and deferred API/auth decisions live under [docs/phase0/README.md](docs/phase0/README.md). Root [requirements.txt](requirements.txt) exists for Docker and delegates to [backend/requirements.txt](backend/requirements.txt).

### Local Python layout

Run commands from the **repository root** so imports resolve as `backend.*`:

```bash
pip install -r requirements.txt
pip install -r backend/requirements-dev.txt   # optional: pytest, ruff
export PYTHONPATH=.   # usually set automatically by pytest.ini
python main.py
python -m backend.src.cli --single --quick
```

For dependency management with uv, use `backend/pyproject.toml` (`cd backend && uv sync`).

## System Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Data Collection Layer                        │
│  Price Producer | News Producer | Social Media Producer         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Streaming Engine (Pathway)                   │
│  Real-time Processing | Technical Indicators | Data Validation  │
└─────────────────────────────────────────────────────────────────┘
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
         ┌──────────────────┐  ┌──────────────────┐
         │  Redis Streams   │  │  MongoDB Atlas   │
         │  (Real-time)     │  │  (Historical)    │
         └──────────────────┘  └──────────────────┘
                    │                   │
                    └─────────┬─────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              FinRL Service (Reinforcement Learning)             │
│  PPO Model | Fine-tuning | Explainability (SHAP/LIME)          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Multi-Agent Analysis                         │
│  News Analyst | Social Analyst | Market Analyst | SEC Analyst  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              Debate & Validation Layer                          │
│  Agent Debate | Reconciliation | Risk Assessment               │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                  Trade Execution (Alpaca MCP)                   │
│  Portfolio Management | Order Execution | Position Tracking     │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **Data Producers**: Collect real-time price, news, and social media data
2. **Streaming Engine**: Processes raw data, calculates technical indicators
3. **Storage**: Redis for real-time, MongoDB for historical persistence
4. **FinRL Service**: Makes buy/sell decisions using PPO model
5. **Multi-Agent Analysis**: Four specialized agents analyze selected tickers
6. **Debate & Validation**: Agents debate and validate decisions
7. **Trade Execution**: Approved trades executed via Alpaca API

## Prerequisites

### System Requirements

- Python 3.10 or higher
- Redis 7.0 or higher
- MongoDB 7.0 or higher (Atlas recommended)
- 8GB RAM minimum (16GB recommended)
- 20GB free disk space

### Required API Keys

- **Alpaca**: Trading API (paper trading supported)
- **OpenAI**: GPT-4 for agent analysis
- **NewsAPI**: News article fetching
- **SEC API**: Financial data (optional)

## Installation

### 1. Clone Repository

```bash
git clone <repository-url>
cd InterIIT-HP3
```

### 2. Install Python Dependencies

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -r backend/requirements.txt
```

### 3. Install System Dependencies

#### Ubuntu/Debian

```bash
sudo apt-get update
sudo apt-get install -y redis-server build-essential git
```

#### macOS

```bash
brew install redis
```

### 4. Start Services

```bash
# Start Redis
redis-server

# Verify Redis
redis-cli ping
# Expected output: PONG
```

## Configuration

### 1. Environment Variables

Create `.env` file in project root:

```bash
# Alpaca Configuration
ALPACA_API_KEY=your_alpaca_key
ALPACA_SECRET_KEY=your_alpaca_secret
ALPACA_BASE_URL=https://paper-api.alpaca.markets

# OpenAI Configuration
OPENAI_API_KEY=your_openai_key

# NewsAPI Configuration
NEWSAPI_API_KEY=your_newsapi_key

# SEC API (Optional)
SEC_API_KEY=your_sec_api_key

# MongoDB Configuration
MONGODB_URI_STREAMING=mongodb+srv://username:password@cluster.mongodb.net/?retryWrites=true&w=majority

# Redis Configuration
REDIS_HOST=localhost
REDIS_PORT=6379

# Trading Configuration
SYMBOLS=AAPL,MSFT,GOOGL,AMZN,META,TSLA,NVDA,JPM,V,WMT,JNJ,PG,UNH,HD,BAC,XOM,MA,DIS,CSCO,NFLX,ADBE,CMCSA,PFE,KO,PEP,INTC,ABT,CRM,AVGO,NKE
FINRL_OUTPUT_TICKERS=10
```

### 2. Backend Configuration

Create `backend/.env`:

```bash
# Copy from root .env
cp .env backend/.env
```

### 3. Model Setup

Download pre-trained PPO model:

```bash
# Model should be placed at:
# backend/finrl_integration/agent_ppo.zip
```

## Quick Start

### Step 1: Initialize Historical Data

Before running the pipeline, ensure MongoDB has at least 3 days of historical data:

```bash
python historical_data.py
```

Expected output:

```
Historical Data Validator
Trading days in MongoDB: 3
New bars inserted: 21,600
Status: complete
```

### Step 2: Start Main Pipeline

#### Option A: Full Pipeline (Recommended)

```bash
# Continuous operation with fine-tuning
python full_pipeline_enhanced.py
```

#### Option B: Parallel Pipeline (Faster)

```bash
# Uses asyncio.gather for parallel agent processing
python parallel_full_pipeline_clean.py
```

#### Option C: Quick Test Mode

```bash
# Faster intervals for testing
python full_pipeline_enhanced.py --quick --single
```

### Step 3: Monitor Pipeline

In a separate terminal:

```bash
python monitor_pipeline_detailed.py
```

## Core Components

### 1. Data Producers

#### Price Producer

- **Location**: `backend/src/producers/price_producer_impl.py`
- **Function**: Fetches 1-minute OHLCV data from Alpaca
- **Symbols**: 30 tickers from SYMBOLS environment variable
- **Frequency**: Every 60 seconds

#### News Producer

- **Location**: `backend/src/producers/news_producer_impl.py`
- **Function**: Fetches news articles from NewsAPI
- **Activation**: Only for tickers selected by FinRL
- **Frequency**: Every 5 minutes

#### Social Producer

- **Location**: `backend/src/producers/social_producer_impl.py`
- **Function**: Generates synthetic social media sentiment
- **Activation**: Only for tickers selected by FinRL
- **Frequency**: Every 5 minutes

### 2. Streaming Engine

- **Location**: `backend/src/engine/streaming_engine.py`
- **Technology**: Pathway (real-time data processing)
- **Functions**:
  - Consumes raw price data from Redis
  - Calculates technical indicators:
    - MACD (12, 26, 9)
    - Bollinger Bands (20-period)
    - RSI (30-period)
    - CCI (30-period)
    - DX (30-period)
    - SMA (30, 60-period)
  - Publishes to `processed:price` stream
- **Frequency**: Real-time (5-second publish interval)

### 3. MongoDB Sync Service

- **Location**: `backend/src/services/mongodb_sync_service.py`
- **Function**: Syncs processed data to MongoDB
- **Storage Format**:
  ```json
  {
    "date": "2024-01-15T10:30:00",
    "tic": "AAPL",
    "open": 150.0,
    "high": 152.0,
    "low": 149.5,
    "close": 151.5,
    "volume": 1000000,
    "macd": 1.23,
    "boll_ub": 155.0,
    "boll_lb": 148.0,
    "rsi_30": 65.5,
    "cci_30": 100.2,
    "dx_30": 25.3,
    "close_30_sma": 150.5,
    "close_60_sma": 149.8
  }
  ```
- **Frequency**: Every 60 seconds (10 seconds in quick mode)
- **Retention**: Permanent (rolling 48-hour window for fine-tuning)

### 4. FinRL Service

- **Location**: `backend/src/services/finrl_service_finetuned.py`
- **Model**: Proximal Policy Optimization (PPO)
- **State Vector**: 301 dimensions
  - 1 cash balance
  - 30 prices
  - 30 holdings
  - 240 technical indicators (8 indicators × 30 tickers)
- **Action Space**: Continuous [-1, 1] per ticker, scaled by MAX_STOCK (100)
- **Decision Threshold**: ±10 shares minimum
- **Output**: Top 10 tickers (configurable)
- **Frequency**: Every 2 hours
- **Explainability**:
  - SHAP: Feature importance (50 samples)
  - LIME: Local explanations (100 samples)
  - JSONL logging: `logs/explainability/decisions_YYYYMMDD.jsonl`

### 5. Fine-tuning Service

- **Location**: `backend/src/services/finetuning_service.py`
- **Function**: Periodically fine-tunes PPO model
- **Data Source**: MongoDB (48-hour rolling window)
- **Frequency**: Every 2 hours
- **Validation**:
  - 70/30 train/validation split
  - Minimum 3 days of data required
  - Improvement threshold: 5%
  - Rollback if performance degrades
- **Output**: `backend/finrl_integration/agent_ppo_finetuned.zip`

### 6. Multi-Agent System

#### News Analyst

- **Location**: `backend/src/agents_redis/news_analyst_redis.py`
- **Function**: Analyzes news articles for sentiment and relevance
- **Model**: GPT-4
- **Output**: Sentiment score, key events, bullish/bearish signals

#### Social Analyst

- **Location**: `backend/src/agents_redis/social_analyst_redis.py`
- **Function**: Analyzes social media sentiment
- **Model**: GPT-4
- **Output**: Community sentiment, trending topics, risk signals

#### Market Analyst

- **Location**: `backend/src/agents_redis/market_analyst_redis.py`
- **Function**: Technical analysis using price and indicators
- **Model**: GPT-4
- **Output**: Support/resistance levels, trend analysis, technical signals

#### SEC Analyst

- **Location**: `backend/src/agents_redis/sec_report_analyst_redis.py`
- **Function**: Analyzes SEC filings and financial statements
- **Data Source**: MongoDB (financial data)
- **Model**: GPT-4
- **Output**: Financial health, growth metrics, risk factors

### 7. Debate & Validation

- **Location**: `backend/src/agents_redis/debate.py`
- **Function**: Multi-agent debate for consensus
- **Process**:
  1. Each agent presents analysis
  2. Agents debate pros/cons (3 rounds)
  3. Final vote on BUY/SELL/HOLD
  4. Confidence score (0-100)
- **Reconciliation**: Compares FinRL vs Validator decisions
  - Approved: Both agree
  - Rejected: Disagreement
  - Overruled: Validator blocks FinRL

### 8. Trade Execution

- **Location**: `backend/src/agents_redis/decision_agent_redis_mcp.py`
- **Integration**: Alpaca MCP server
- **Functions**:
  - Fetch current portfolio
  - Calculate position sizes (GPT-4 assisted)
  - Execute market orders
  - Log trades to Redis and JSON
- **Risk Management**:
  - Max 10% position size
  - Buying power checks
  - Order validation

## Pipeline Workflows

### Enhanced Pipeline (Production)

**File**: `full_pipeline_enhanced.py`

**Features**:

- MongoDB sync for historical storage
- Automatic model fine-tuning every 2 hours
- Fine-tuned model predictions
- Trigger-based immediate runs on breakouts
- Continuous operation with periodic agent analysis

**Workflow**:

```
1. Setup: Ensure 3 days of historical data (historical_data.py)
2. Start Enhanced Pipeline:
   - Price data collection (30 tickers)
   - MongoDB sync every 60 seconds
   - Fine-tuning runs immediately, then every 2 hours
   - FinRL runs every 2 hours with updated model
   - Trigger checker monitors for breakouts (5-minute intervals)
3. After First FinRL Run:
   - Fetch FinRL decisions from Redis
   - Retrieve processed market data
   - Run 4 agents in parallel
   - Run debate and validation
   - Reconcile FinRL vs Validator decisions
   - Execute approved trades
4. Continuous Operation:
   - Repeat agent analysis after each FinRL run
   - Background fine-tuning every 2 hours
   - Immediate FinRL runs on major breakouts
```

**Usage**:

```bash
# Full continuous operation
python full_pipeline_enhanced.py

# Single cycle only
python full_pipeline_enhanced.py --single

# Quick test mode (faster intervals)
python full_pipeline_enhanced.py --quick --single
```

### Parallel Pipeline (Optimized)

**File**: `parallel_full_pipeline_clean.py`

**Features**:

- Uses `asyncio.gather` for true parallel execution
- 3-4x speedup over sequential processing
- Same comprehensive logging

**Workflow**:

```
1. Start enhanced pipeline (background)
2. Wait for first FinRL run
3. For each selected ticker:
   - Run all 4 agents in PARALLEL using asyncio.gather
4. Process multiple tickers CONCURRENTLY
5. Run debate and validation
6. Execute trades
```

**Usage**:

```bash
# Continuous parallel operation
python parallel_full_pipeline_clean.py

# Single cycle
python parallel_full_pipeline_clean.py --single
```

### Log Structure

All pipelines create structured logs in `agent_logs/`:

```
agent_logs/
└── enhanced_20240115_143000/
    ├── full_pipeline_enhanced.log
    ├── cycle_001/
    │   ├── 00_SUMMARY.json
    │   ├── 01_finrl_output.json
    │   ├── 02_news_raw/
    │   ├── 03_social_raw/
    │   ├── 04_market_data/
    │   ├── 05_agent_analyses/
    │   ├── 06_debate_results/
    │   ├── 07_reconciliation.json
    │   └── 08_trade_execution.json
    └── cycle_002/
        └── ...
```

## Testing

### Unit Tests

Located in `unit_tests`:

#### Test Fine-tuning Workflow

```bash
python unit_tests/test_finetuning.py
```

Tests:

1. Base model prediction (first prediction chunk)
2. Training data preparation (historical period)
3. Fine-tuning validation (data quality)
4. Fine-tuned model prediction (comparison)

#### Test Explainability

```bash
python unit_tests/test_explainability.py
```

Tests:

1. SHAP library installation
2. LIME library installation
3. Background sample collection (50 samples)
4. SHAP explanation generation
5. LIME explanation generation
6. Global importance ranking

#### Test FinRL Base Model

```bash
python unit_tests/test_finrl_base_model.py
```

Tests:

1. Model loading
2. State vector construction (301 dims)
3. Prediction pipeline
4. Action scaling
5. Edge cases (subset tickers, single ticker)

### Integration Tests

#### Test Full Pipeline (Quick Mode)

```bash
python full_pipeline_enhanced.py --quick --single
```

Expected duration: 10-15 minutes

#### Test Parallel Processing

```bash
python parallel_full_pipeline_clean.py --quick --single
```

Expected speedup: 3-4x faster than sequential

### Data Validation

#### Verify Historical Data

```bash
python historical_data.py
```

Checks:

- MongoDB connection
- 3 trading days present
- 4+ hours per day
- All 30 tickers present

#### Clear Data (Reset)

```bash
# Clear Redis only
python backend/src/utils/clear_redis.py

# Clear MongoDB only
python backend/src/utils/clear_mongodb.py
```

## Monitoring

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

## Docker Deployment

### Build Images

```bash
# Build all services
docker-compose build

# Build specific service
docker-compose build aegis-pipeline
```

### Run Services

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f aegis-pipeline

# Stop services
docker-compose down
```

### Service Ports

- Redis: 6379
- MongoDB: 27017
- Backend API: 8000
- Dashboard Bridge: 8001
- Frontend: 3000

### Docker Configuration

**File**: `docker-compose.yml`

Services:

1. **redis**: Message broker (Redis 7)
2. **mongodb**: Historical storage (MongoDB 7)
3. **aegis-pipeline**: Main trading pipeline
4. **dashboard-bridge**: WebSocket server (optional)
5. **frontend**: Web dashboard (optional)

### Health Checks

```bash
# Check Redis
docker exec aegis-redis redis-cli ping

# Check MongoDB
docker exec aegis-mongodb mongosh --eval "db.adminCommand('ping')"

# Check pipeline
docker logs aegis-pipeline --tail 50
```

## API Documentation

### Redis Streams

#### Raw Data Streams

**raw:price-updates**

```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "ticker": "AAPL",
  "open": 150.0,
  "high": 152.0,
  "low": 149.5,
  "close": 151.5,
  "volume": 1000000
}
```

**raw:news-articles**

```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "ticker": "AAPL",
  "title": "Apple Announces New Product",
  "content": "...",
  "source": "NewsAPI",
  "url": "https://..."
}
```

**raw:social**

```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "ticker": "AAPL",
  "text": "AAPL looking strong today...",
  "sentiment": 0.8,
  "source": "synthetic"
}
```

#### Processed Streams

**processed:price**

```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "metadata": {
    "ticker": "AAPL",
    "source": "alpaca"
  },
  "price_data": {
    "open": 150.0,
    "high": 152.0,
    "low": 149.5,
    "close": 151.5,
    "volume": 1000000
  },
  "momentum_indicators": {
    "macd": { "macd_line": 1.23, "signal_line": 1.1, "histogram": 0.13 },
    "rsi_30": 65.5,
    "cci_30": 100.2
  },
  "volatility_indicators": {
    "boll_ub": 155.0,
    "boll_lb": 148.0,
    "boll_mid": 151.5
  },
  "trend_indicators": {
    "dx_30": 25.3
  },
  "moving_averages": {
    "close_30_sma": 150.5,
    "close_60_sma": 149.8
  }
}
```

#### Decision Streams

**finrl-decisions**

```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "data": {
    "selected_tickers": ["AAPL", "MSFT", "GOOGL"],
    "buy_decisions": {
      "AAPL": 50,
      "MSFT": 30
    },
    "sell_decisions": {
      "GOOGL": 20
    },
    "total_analyzed": 30,
    "scaled_actions": [50, 30, -20, ...]
  }
}
```

### MongoDB Collections

**market_data_1min**

Indexes:

- `date` (descending)
- `tic` (ascending)
- `(date, tic)` (unique compound)

Schema:

```json
{
  "date": "2024-01-15T10:30:00Z",
  "tic": "AAPL",
  "open": 150.0,
  "high": 152.0,
  "low": 149.5,
  "close": 151.5,
  "volume": 1000000,
  "macd": 1.23,
  "boll_ub": 155.0,
  "boll_lb": 148.0,
  "rsi_30": 65.5,
  "cci_30": 100.2,
  "dx_30": 25.3,
  "close_30_sma": 150.5,
  "close_60_sma": 149.8
}
```

### Alpaca MCP Server

**Endpoints** (via `backend/src/agents_redis/decision_agent_redis_mcp.py`):

#### Get Account

```python
GET /v2/account
Response: {
  "cash": 100000.0,
  "portfolio_value": 150000.0,
  "buying_power": 100000.0,
  "positions": [...]
}
```

#### Get Positions

```python
GET /v2/positions
Response: [
  {
    "symbol": "AAPL",
    "qty": 100,
    "avg_entry_price": 150.0,
    "current_price": 155.0,
    "market_value": 15500.0,
    "unrealized_pl": 500.0
  }
]
```

#### Submit Order

```python
POST /v2/orders
Body: {
  "symbol": "AAPL",
  "qty": 10,
  "side": "buy",
  "type": "market",
  "time_in_force": "day"
}
Response: {
  "id": "order_id",
  "status": "filled",
  "filled_qty": 10,
  "filled_avg_price": 155.5
}
```

## Troubleshooting

### Common Issues

#### 1. Redis Connection Failed

**Symptom**: `ConnectionRefusedError: [Errno 61] Connection refused`

**Solution**:

```bash
# Start Redis
redis-server

# Verify
redis-cli ping
```

#### 2. MongoDB Connection Timeout

**Symptom**: `ServerSelectionTimeoutError`

**Solution**:

- Check MongoDB Atlas connection string in `.env`
- Verify IP whitelist in Atlas dashboard
- Test connection:

```bash
python -c "from pymongo import MongoClient; client = MongoClient('YOUR_URI'); print(client.server_info())"
```

#### 3. No Historical Data

**Symptom**: `WARNING: Only found 0/3 trading days`

**Solution**:

```bash
# Clear existing data
python backend/src/utils/clear_mongodb.py

# Re-fetch historical data
python historical_data.py
```

#### 4. Model Not Found

**Symptom**: `FileNotFoundError: agent_ppo.zip`

**Solution**:

- Ensure model exists at `backend/finrl_integration/agent_ppo.zip`
- Train new model if needed (see FinRL documentation)

#### 5. OpenAI API Errors

**Symptom**: `RateLimitError` or `AuthenticationError`

**Solution**:

- Verify API key in `.env`
- Check rate limits (agents make multiple calls)
- Add retry logic if needed

#### 6. High Memory Usage

**Symptom**: Pipeline crashes with `MemoryError`

**Solution**:

- Reduce buffer sizes in agents:

```python
agent.max_buffer_size = 50  # Default is 100
```

- Clear Redis periodically:

```bash
redis-cli FLUSHALL
```

#### 7. FinRL Not Running

**Symptom**: No decisions in `finrl-decisions` stream

**Solution**:

- Check data availability:

```bash
redis-cli XLEN processed:price
```

- Verify model loaded:

```bash
ls -lh backend/finrl_integration/agent_ppo.zip
```

- Check logs:

```bash
tail -f agent_logs/enhanced_*/full_pipeline_enhanced.log
```

### Debug Commands

#### Check Redis Status

```bash
# Stream lengths
redis-cli XLEN raw:price-updates
redis-cli XLEN processed:price
redis-cli XLEN finrl-decisions

# Latest entry
redis-cli XREVRANGE processed:price + - COUNT 1

# Memory usage
redis-cli INFO memory
```

#### Check MongoDB Status

```bash
# Connect to MongoDB
mongosh "YOUR_MONGODB_URI"

# Count documents
db.market_data_1min.countDocuments()

# Find latest
db.market_data_1min.find().sort({date: -1}).limit(1)

# Check date range
db.market_data_1min.aggregate([
  {$group: {_id: null, min: {$min: "$date"}, max: {$max: "$date"}}}
])
```

#### Check Pipeline Status

```bash
# Process running
ps aux | grep full_pipeline_enhanced

# Log recent activity
tail -50 pipeline_output.log

# Check cycle count
ls -d agent_logs/enhanced_*/cycle_* | wc -l
```

### Performance Optimization

#### 1. Redis Persistence

Disable Redis persistence for faster writes:

```bash
redis-server --appendonly no --save ""
```

#### 2. MongoDB Indexes

Ensure indexes are created:

```python
collection.create_index([('date', -1)])
collection.create_index([('tic', 1)])
collection.create_index([('date', -1), ('tic', 1)], unique=True)
```

#### 3. Parallel Processing

Use parallel pipeline for faster agent processing:

```bash
python parallel_full_pipeline_clean.py
```

Expected speedup: 3-4x

#### 4. Quick Mode

Use quick mode for testing (faster intervals):

```bash
python full_pipeline_enhanced.py --quick --single
```

Intervals:

- MongoDB sync: 10 seconds (vs 60)
- Fine-tuning: 10 minutes (vs 2 hours)
- FinRL: 5 minutes (vs 2 hours)

## Project Structure

```
InterIIT-HP3/
├── backend/
│   ├── src/
│   │   ├── agents_redis/          # Multi-agent system
│   │   ├── consumers/              # Data consumers
│   │   ├── engine/                 # Streaming engine
│   │   ├── orchestration/          # Pipeline orchestrators
│   │   ├── producers/              # Data producers
│   │   ├── services/               # Core services
│   │   └── utils/                  # Utilities
│   ├── config/                     # Configuration
│   ├── finrl_integration/          # FinRL model
│   └── scripts/                    # Utility scripts
├── frontend/                       # Web dashboard (optional)
├── unit_tests/                     # Test suite
├── agent_logs/                     # Pipeline logs
├── logs/                           # Service logs
├── .env                            # Environment variables
├── historical_data.py              # Historical data fetcher
├── full_pipeline_enhanced.py       # Main pipeline
├── parallel_full_pipeline_clean.py # Parallel pipeline
├── monitor_pipeline_detailed.py    # Monitoring script
├── dashboard_bridge.py             # WebSocket server
├── docker-compose.yml              # Docker configuration
└── README.md                       # This file
```
