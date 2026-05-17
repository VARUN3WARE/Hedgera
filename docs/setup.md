# Setup

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
cd Hedgera
```

### 2. Install Python Dependencies

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scriptsctivate

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
NEWS_API_KEY=your_newsapi_key

# Auth (FastAPI / Beanie) — required for signup/login
MONGODB_URI=mongodb://localhost:27017/aegis_trading
SECRET_KEY=change-me-in-production

# Streaming engine: redis (default) or pathway
STREAMING_ENGINE=redis

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
