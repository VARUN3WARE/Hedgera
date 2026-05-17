# Components

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

- **Locations**: `backend/src/engine/streaming_engine.py` (Redis manual), `backend/src/pathway_engine/` (Pathway)
- **Selection**: `STREAMING_ENGINE` env or `backend.src.streaming.create_streaming_engine()`
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
