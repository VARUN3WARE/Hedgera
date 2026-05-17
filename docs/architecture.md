# Architecture

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
