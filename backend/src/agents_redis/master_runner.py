"""Master runner to orchestrate all Redis-connected agents."""

import asyncio
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from .market_analyst_redis import MarketAnalystRedis
from .news_analyst_redis import NewsAnalystRedis
from .social_analyst_redis import SocialAnalystRedis
from .sec_report_analyst_redis import SecReportAnalystRedis
from .debate import DebateAgentRedis


async def run_master_agent(tickers: List[str], duration: int = 60) -> str:
    """
    Run master agent analysis for a list of tickers.
    
    Args:
        tickers: List of ticker symbols to analyze
        duration: Duration in seconds to collect data (default 60)
    
    Returns:
        Formatted markdown report
    """
    logger = logging.getLogger("MasterAgent")
    
    # Get configuration
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    openai_api_key = os.getenv("OPENAI_API_KEY")
    mongodb_uri = os.getenv("MONGODB_URI")
    
    if not openai_api_key:
        logger.error("OPENAI_API_KEY not set in environment")
        return "# Error: OPENAI_API_KEY not configured\n"
    
    # Create runner
    runner = MasterAgentRunner(
        redis_url=redis_url,
        openai_api_key=openai_api_key,
        mongodb_uri=mongodb_uri,
        model="gpt-4o-mini",
        logger=logger,
    )
    
    # Start agents
    await runner.start_all()
    
    # Fetch SEC data for tickers first (if MongoDB is configured)
    if runner.sec_agent and runner.sec_agent.use_mongodb:
        logger.info(f"Fetching SEC data for {len(tickers)} tickers...")
        await runner.sec_agent.fetch_for_tickers(tickers)
    
    # Let agents collect market/news/social data
    logger.info(f"Collecting analysis for {len(tickers)} tickers for {duration}s...")
    await asyncio.sleep(duration)
    
    # Generate reports for each ticker
    report_lines = [
        "# Multi-Agent Analysis Report",
        f"\n**Generated:** {datetime.now().isoformat()}",
        f"\n**Tickers Analyzed:** {', '.join(tickers)}",
        "\n---\n",
    ]
    
    for ticker in tickers:
        try:
            ticker_report = runner.generate_report(ticker)
            
            report_lines.append(f"\n## {ticker}")
            report_lines.append(f"\n**Overall Sentiment:** {ticker_report['overall_sentiment'].upper()}")
            
            # Market analysis
            market = ticker_report.get('market_analysis', {})
            if market:
                report_lines.append("\n### 📊 Market Analysis")
                report_lines.append(f"- **Sentiment:** {market.get('sentiment', 'N/A')}")
                report_lines.append(f"- **Analysis:** {market.get('analysis', 'No data')}")
            
            # News analysis
            news = ticker_report.get('news_analysis', {})
            if news:
                report_lines.append("\n### 📰 News Analysis")
                report_lines.append(f"- **Sentiment:** {news.get('sentiment', 'N/A')}")
                report_lines.append(f"- **Analysis:** {news.get('analysis', 'No data')}")
            
            # Social analysis
            social = ticker_report.get('social_analysis', {})
            if social:
                report_lines.append("\n### 🐦 Social Sentiment")
                report_lines.append(f"- **Sentiment:** {social.get('sentiment', 'N/A')}")
                report_lines.append(f"- **Analysis:** {social.get('analysis', 'No data')}")
            
            # SEC/Fundamental analysis
            sec = ticker_report.get('sec_analysis', {})
            if sec:
                report_lines.append("\n### 📈 Fundamental Analysis (SEC Filings)")
                report_lines.append(f"- **Form Type:** {sec.get('form_type', 'N/A')}")
                report_lines.append(f"- **Report Date:** {sec.get('report_date', 'N/A')}")
                report_lines.append(f"- **Summary:**\n{sec.get('summary', 'No data')}")
            
            report_lines.append("\n---")
            
        except Exception as e:
            logger.error(f"Error generating report for {ticker}: {e}")
            report_lines.append(f"\n## {ticker}")
            report_lines.append(f"\n**Error:** {str(e)}\n")
    
    # Run debate analysis if enabled
    if runner.debate_agent:
        logger.info("Running Bull vs Bear debate analysis...")
        try:
            # Prepare final_reports data for debate
            final_reports = {
                "agents": [
                    {"agent": "market_analyst", "output": runner.market_agent.latest_analysis},
                    {"agent": "news_analyst", "output": runner.news_agent.latest_analysis},
                    {"agent": "social_media_analyst", "output": runner.social_agent.latest_analysis},
                    {"agent": "sec_report_analyst", "output": runner.sec_agent.latest_analysis if runner.sec_agent else {}},
                ]
            }
            
            # Trigger debate for first ticker (or run separately for each if needed)
            await runner.run_debate(tickers[0] if tickers else "AAPL", final_reports)
            
            # Add debate results to report
            if tickers and tickers[0] in runner.debate_agent.latest_results:
                debate_result = runner.debate_agent.latest_results[tickers[0]]
                report_lines.append("\n## 🎯 Bull vs Bear Debate Analysis")
                report_lines.append(f"\n### Debate for {tickers[0]}")
                
                validation = debate_result.get("validation", {})
                report_lines.append(f"\n**Final Decision:** {validation.get('final_recommendation', {}).get('decision', 'N/A')}")
                report_lines.append(f"**Conviction:** {validation.get('final_recommendation', {}).get('conviction', 'N/A')}")
                report_lines.append(f"\n**Summary:** {validation.get('summary', 'N/A')}")
                
                report_lines.append("\n#### Positive Points:")
                for point in validation.get('positivePoints', []):
                    report_lines.append(f"- {point}")
                
                report_lines.append("\n#### Negative Points:")
                for point in validation.get('negativePoints', []):
                    report_lines.append(f"- {point}")
                
        except Exception as e:
            logger.error(f"Error running debate analysis: {e}", exc_info=True)
    
    # Stop agents
    await runner.stop_all()
    
    return "\n".join(report_lines)


class MasterAgentRunner:
    """Orchestrates all analyst agents consuming from Redis streams."""

    def __init__(
        self,
        redis_url: str,
        openai_api_key: str,
        mongodb_uri: Optional[str] = None,
        model: str = "gpt-4o-mini",
        logger: Optional[logging.Logger] = None,
    ):
        """
        Initialize the Master Agent Runner.

        Args:
            redis_url: Redis connection URL
            openai_api_key: OpenAI API key
            mongodb_uri: MongoDB connection URI for SEC data
            model: OpenAI model to use
            logger: Optional logger instance
        """
        self.redis_url = redis_url
        self.openai_api_key = openai_api_key
        self.mongodb_uri = mongodb_uri
        self.model = model
        self.logger = logger or logging.getLogger(__name__)

        # Initialize agents
        self.market_agent = MarketAnalystRedis(
            redis_url=redis_url,
            openai_api_key=openai_api_key,
            model=model,
            logger=logging.getLogger("MarketAnalyst"),
        )

        self.news_agent = NewsAnalystRedis(
            redis_url=redis_url,
            openai_api_key=openai_api_key,
            model=model,
            logger=logging.getLogger("NewsAnalyst"),
        )

        self.social_agent = SocialAnalystRedis(
            redis_url=redis_url,
            openai_api_key=openai_api_key,
            model=model,
            logger=logging.getLogger("SocialAnalyst"),
        )

        # Initialize SEC analyst (with MongoDB support)
        try:
            self.sec_agent = SecReportAnalystRedis(
                redis_url=redis_url,
                openai_api_key=openai_api_key,
                mongodb_uri=mongodb_uri,
                model=model,
                logger=logging.getLogger("SecAnalyst"),
                fetch_mode=True,  # Fetch mode enabled
                use_mongodb=bool(mongodb_uri),  # Use MongoDB if URI provided
            )
            self.logger.info("SEC analyst initialized with MongoDB support")
        except Exception as e:
            self.logger.warning(f"SEC analyst initialization failed: {e}")
            self.sec_agent = None

        # Initialize debate agent
        try:
            self.debate_agent = DebateAgentRedis(
                redis_url=redis_url,
                logger=logging.getLogger("DebateAgent"),
            )
            self.logger.info("Debate agent initialized")
        except Exception as e:
            self.logger.warning(f"Debate agent initialization failed: {e}")
            self.debate_agent = None

        # Collect all agents
        self.agents = [self.market_agent, self.news_agent, self.social_agent]
        if self.sec_agent:
            self.agents.append(self.sec_agent)
        
        self.tasks: List[asyncio.Task] = []

    async def start_all(self):
        """Start all agents."""
        self.logger.info("Starting all agents...")

        for agent in self.agents:
            task = asyncio.create_task(agent.run())
            self.tasks.append(task)
            self.logger.info(f"Started {agent.__class__.__name__}")

    async def stop_all(self):
        """Stop all agents."""
        self.logger.info("Stopping all agents...")

        for agent in self.agents:
            await agent.stop()

        # Cancel all tasks
        for task in self.tasks:
            task.cancel()

        # Wait for tasks to complete
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.logger.info("All agents stopped")

    def get_all_analysis(self, ticker: Optional[str] = None) -> Dict[str, Dict]:
        """
        Get analysis from all agents.

        Args:
            ticker: Optional ticker to filter by

        Returns:
            Dictionary with analysis from all agents
        """
        result = {
            "market": self.market_agent.get_latest_analysis(ticker),
            "news": self.news_agent.get_latest_analysis(ticker),
            "social": self.social_agent.get_latest_analysis(ticker),
        }
        
        # Add SEC analysis if available
        if self.sec_agent:
            result["sec"] = self.sec_agent.latest_analysis.get(ticker, {}) if ticker else self.sec_agent.latest_analysis
        
        return result

    def generate_report(self, ticker: str) -> Dict:
        """
        Generate a comprehensive report for a ticker.

        Args:
            ticker: Stock ticker symbol

        Returns:
            Comprehensive report dictionary
        """
        analysis = self.get_all_analysis(ticker)
        
        market_data = analysis["market"].get(ticker, {})
        news_data = analysis["news"].get(ticker, {})
        social_data = analysis["social"].get(ticker, {})
        sec_data = analysis.get("sec", {})

        # Calculate overall sentiment
        sentiments = []
        if market_data.get("sentiment"):
            sentiments.append(market_data["sentiment"])
        if news_data.get("sentiment"):
            sentiments.append(news_data["sentiment"])
        if social_data.get("sentiment"):
            sentiments.append(social_data["sentiment"])

        # Simple majority vote for overall sentiment
        if sentiments:
            sentiment_counts = {
                "positive": sentiments.count("positive") + sentiments.count("bullish"),
                "negative": sentiments.count("negative") + sentiments.count("bearish"),
                "neutral": sentiments.count("neutral"),
            }
            overall_sentiment = max(sentiment_counts, key=sentiment_counts.get)
        else:
            overall_sentiment = "neutral"

        return {
            "ticker": ticker,
            "timestamp": datetime.now().isoformat(),
            "overall_sentiment": overall_sentiment,
            "market_analysis": market_data,
            "news_analysis": news_data,
            "social_analysis": social_data,
            "sec_analysis": sec_data,
        }

    async def run_debate(self, ticker: str, final_reports: Dict):
        """
        Run the Bull vs Bear debate for a ticker.
        
        Args:
            ticker: Stock ticker symbol
            final_reports: Dictionary containing all agent analyses
        """
        if not self.debate_agent:
            self.logger.warning("Debate agent not initialized")
            return
        
        try:
            from .debate import run_debate
            
            # Run debate in executor to avoid blocking
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, run_debate, final_reports, None)
            
            # Store result
            self.debate_agent.latest_results[ticker] = result
            
            self.logger.info(f"Debate completed for {ticker}")
            
        except Exception as e:
            self.logger.error(f"Error running debate for {ticker}: {e}", exc_info=True)

    async def save_report(self, ticker: str, output_dir: str = "reports"):
        """
        Generate and save a report for a ticker.

        Args:
            ticker: Stock ticker symbol
            output_dir: Directory to save reports
        """
        report = self.generate_report(ticker)
        
        # Create output directory
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Save report
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{ticker}_{timestamp}.json"
        filepath = output_path / filename

        with open(filepath, "w") as f:
            json.dump(report, f, indent=2)

        self.logger.info(f"Report saved to {filepath}")
        return str(filepath)

    async def run(self, duration: Optional[int] = None):
        """
        Run all agents for a specified duration.

        Args:
            duration: Duration in seconds. If None, runs indefinitely.
        """
        try:
            await self.start_all()
            
            if duration:
                await asyncio.sleep(duration)
            else:
                # Run indefinitely
                await asyncio.gather(*self.tasks)
                
        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal")
        finally:
            await self.stop_all()
