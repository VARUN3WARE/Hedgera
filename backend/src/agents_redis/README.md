# Redis-Connected Agents

This folder contains all AI agents that consume data from Redis streams and perform real-time analysis.

## Overview

All agents in this folder:

- Consume data from Redis streams
- Perform continuous analysis using LLMs
- Store results in memory for API access
- Support the FastAPI backend

## Agents

### Base Agent (`base_agent.py`)

Abstract base class for all Redis-connected agents.

**Features:**

- Redis stream consumption with consumer groups
- Automatic reconnection handling
- Message acknowledgment
- Graceful shutdown

**Usage:**

```python
from src.agents_redis import BaseRedisAgent

class MyAgent(BaseRedisAgent):
    async def process_message(self, message_id: str, data: Dict[str, Any]):
        # Process the message
        pass
```

### Market Analyst (`market_analyst_redis.py`)

Analyzes price data from the `price_stream`.

**Features:**

- Technical indicator calculation (SMA, price changes)
- LLM-powered market analysis
- Sentiment detection (bullish/bearish/neutral)
- Price trend analysis

**Data Source:** `price_stream`  
**Consumer Group:** `market_analyst_group`

### News Analyst (`news_analyst_redis.py`)

Analyzes news articles from the `news_stream`.

**Features:**

- News sentiment analysis
- Headline tracking
- Impact assessment
- Theme extraction

**Data Source:** `news_stream`  
**Consumer Group:** `news_analyst_group`

### Social Analyst (`social_analyst_redis.py`)

Analyzes social media posts from the `social_stream`.

**Features:**

- Social sentiment analysis
- Engagement metrics (likes, comments, shares)
- Buzz score calculation
- Viral trend detection

**Data Source:** `social_stream`  
**Consumer Group:** `social_analyst_group`

### Master Runner (`master_runner.py`)

Orchestrates all agents and generates comprehensive reports.

**Features:**

- Starts/stops all agents
- Aggregates analysis from all sources
- Generates comprehensive reports
- Overall sentiment calculation

## Architecture

```
Redis Streams
    ↓
Base Agent (abstract)
    ↓
┌───────────────┬───────────────┬───────────────┐
│               │               │               │
Market Analyst  News Analyst  Social Analyst
│               │               │               │
└───────────────┴───────────────┴───────────────┘
                    ↓
            Master Runner
                    ↓
              FastAPI
                    ↓
              Frontend
```

## Usage

### Running Individual Agents

```python
import asyncio
from src.agents_redis import MarketAnalystRedis

async def main():
    agent = MarketAnalystRedis(
        redis_url="redis://localhost:6379",
        openai_api_key="sk-...",
    )

    await agent.run()

asyncio.run(main())
```

### Running All Agents

```python
import asyncio
from src.agents_redis import MasterAgentRunner

async def main():
    runner = MasterAgentRunner(
        redis_url="redis://localhost:6379",
        openai_api_key="sk-...",
    )

    # Run for 60 seconds
    await runner.run(duration=60)

asyncio.run(main())
```

### Getting Analysis Results

```python
# Get market analysis for a ticker
analysis = agent.get_latest_analysis("AAPL")

# Get all analysis
all_analysis = agent.get_latest_analysis()
```

### Generating Reports

```python
# Generate comprehensive report
report = runner.generate_report("AAPL")

# Save report to file
filepath = await runner.save_report("AAPL")
```

## Configuration

All agents support these configuration options:

- `redis_url`: Redis connection URL (default: `redis://localhost:6379`)
- `stream_key`: Redis stream to consume from
- `consumer_group`: Consumer group name
- `consumer_name`: Consumer name within group
- `openai_api_key`: OpenAI API key for LLM
- `model`: OpenAI model to use (default: `gpt-4o-mini`)
- `logger`: Optional logger instance

## Environment Variables

```bash
# Redis
REDIS_URL=redis://localhost:6379

# OpenAI
OPENAI_API_KEY=sk-...

# Agent configuration
OPENAI_MODEL=gpt-4o-mini
```

## Data Flow

1. **Producer** pushes data to Redis stream
2. **Agent** consumes from stream using consumer group
3. **Agent** processes message with LLM
4. **Agent** stores analysis in memory
5. **API** exposes analysis via REST endpoints
6. **Frontend** fetches and displays analysis

## Integration with API

The FastAPI backend (see `src/api/main.py`) integrates with these agents:

```python
from src.agents_redis import MasterAgentRunner

# In API startup
agent_runner = MasterAgentRunner(...)
await agent_runner.start_all()

# In API endpoint
@app.get("/api/v1/analysis/market/{ticker}")
async def get_market_analysis(ticker: str):
    return agent_runner.market_agent.get_latest_analysis(ticker)
```

## Error Handling

Agents handle errors gracefully:

- **Connection errors**: Automatic reconnection
- **Processing errors**: Logged but don't stop agent
- **Missing data**: Returns error in analysis result
- **LLM errors**: Fallback to basic analysis

## Performance

- **Buffer Size**: 50-100 items per ticker
- **Processing**: Real-time (on every message)
- **Memory**: ~100MB per agent
- **CPU**: Low (LLM calls are async)

## Troubleshooting

### Agent won't start

```bash
# Check Redis is running
redis-cli ping

# Check stream exists
redis-cli XLEN price_stream

# Check OpenAI key
echo $OPENAI_API_KEY
```

### No analysis data

```bash
# Check producers are running
python main.py price &

# Wait for data to accumulate
# Agents need 20+ data points for analysis
```

### High memory usage

```python
# Reduce buffer sizes
agent = MarketAnalystRedis(...)
agent.max_buffer_size = 50  # Default is 100
```

## Testing

```bash
# Run unit tests
pytest tests/unit/test_agents_redis.py

# Run integration tests
pytest tests/integration/test_agents_redis.py
```

## Dependencies

- `redis`: Redis Python client
- `langchain`: LLM framework
- `langchain-openai`: OpenAI integration
- `pandas`: Data analysis
- `pydantic`: Data validation

## Related Documentation

- [API Guide](../docs/API_GUIDE.md)
- [Agent Implementation](../docs/AGENT_IMPLEMENTATION.md)
- [Quick Start](../docs/QUICK_START.md)
