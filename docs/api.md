# API reference

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
