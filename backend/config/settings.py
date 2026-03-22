"""Application settings using Pydantic."""
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Main application settings."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )
    
    # App Configuration
    app_name: str = "Aegis Trading System"
    debug: bool = Field(default=True, description="Debug mode")
    
    # Redis Configuration
    redis_host: str = Field(default="localhost", description="Redis host")
    redis_port: int = Field(default=6379, description="Redis port")
    redis_password: Optional[str] = Field(default=None, description="Redis password")
    redis_db: int = Field(default=0, description="Redis database number")
    redis_max_connections: int = Field(default=50, description="Max Redis connections")
    
    # API Configuration
    api_host: str = Field(default="0.0.0.0", description="API host")
    api_port: int = Field(default=8000, description="API port")
    
    # Producer Configuration
    producer_fetch_interval: float = Field(default=300.0, description="Price fetch interval in seconds (5 minutes)")
    news_fetch_interval: float = Field(default=300.0, description="News fetch interval (5 minutes)")
    social_fetch_interval: float = Field(default=180.0, description="Social fetch interval (3 minutes)")
    
    # API Keys
    alpaca_api_key: Optional[str] = Field(default=None, description="Alpaca API key")
    alpaca_secret_key: Optional[str] = Field(default=None, description="Alpaca secret key")
    alpaca_base_url: str = Field(default="https://paper-api.alpaca.markets", description="Alpaca base URL")
    news_api_key: Optional[str] = Field(default=None, description="News API key")
    twitter_bearer_token: Optional[str] = Field(default=None, description="Twitter API token")
    rapidapi_key: Optional[str] = Field(default=None, description="RapidAPI key")
    rapidapi_twitter_host: str = Field(
        default="twitter-api45.p.rapidapi.com",
        description="RapidAPI Twitter host"
    )
    
    # Symbols to Track (30 trading tickers only - VIXY removed)
    symbols: str = Field(
        default="AAPL,AMGN,AMZN,AXP,BA,CAT,CRM,CSCO,CVX,DIS,DOW,GS,HD,HON,IBM,INTC,JNJ,JPM,KO,MCD,MMM,MRK,MSFT,NKE,NVDA,PG,UNH,V,VZ,WMT",
        description="Trading symbols to monitor (30 tickers, no VIXY)"
    )
    
    # FinRL Configuration
    finrl_model_path: str = Field(
        default="backend/finrl_integration/agent_ppo.zip",
        description="Path to FinRL trained model"
    )
    finrl_run_interval: int = Field(
        default=7200,
        description="FinRL execution interval in seconds (2 hours)"
    )
    finrl_output_tickers: int = Field(
        default=10,
        description="Number of tickers FinRL should output"
    )
    
    # Pathway Configuration
    pathway_consumer_group: str = Field(
        default="pathway_engine_group",
        description="Pathway consumer group name"
    )
    pathway_publish_interval: int = Field(
        default=5,
        description="Publish aggregated state interval in seconds"
    )
    
    # Logging
    log_level: str = Field(default="INFO", description="Logging level")
    
    # Database (keeping for backward compatibility)
    database_url: str = "sqlite:///./aegis.db"
    
    # MongoDB Configuration (for streaming/fine-tuning)
    mongodb_uri_streaming: str = Field(
        default="mongodb://localhost:27017/",
        description="MongoDB URI for streaming data (uses MONGODB_URI_STREAMING from .env)"
    )
    mongodb_db_name: str = Field(
        default="finrl_trading",
        description="MongoDB database name for streaming data"
    )
    mongodb_collection_name: str = Field(
        default="market_data_1min",
        description="MongoDB collection name for market data"
    )
    
    @property
    def symbols_list(self) -> list[str]:
        """Get symbols as a list."""
        if isinstance(self.symbols, str):
            return [s.strip().upper() for s in self.symbols.split(",") if s.strip()]
        return self.symbols
    
    @property
    def redis_url(self) -> str:
        """Construct Redis URL."""
        auth = f":{self.redis_password}@" if self.redis_password else ""
        return f"redis://{auth}{self.redis_host}:{self.redis_port}/{self.redis_db}"


# Global settings instance
settings = Settings()
