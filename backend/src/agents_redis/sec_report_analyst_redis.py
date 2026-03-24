"""SEC Filing Analyst Agent that fetches data from SEC API and publishes to Redis.

This agent follows the same pattern as other Redis-connected agents in
`src/agents_redis/`. It fetches SEC filings via the SEC API, processes them
with XBRL data extraction, summarizes them using an LLM, and publishes results
to Redis streams for downstream consumption.
"""

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, Optional

from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate

try:
    from sec_api import QueryApi, XbrlApi
    SEC_API_AVAILABLE = True
except ImportError:
    SEC_API_AVAILABLE = False
    logging.warning("sec_api not available. Install with: pip install sec-api")

try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False
    logging.warning("yfinance not available. Install with: pip install yfinance")

try:
    from pymongo import MongoClient
    PYMONGO_AVAILABLE = True
except ImportError:
    PYMONGO_AVAILABLE = False
    logging.warning("pymongo not available. Install with: pip install pymongo")

from .base_agent import BaseRedisAgent

logger = logging.getLogger(__name__)


class SecReportAnalystRedis(BaseRedisAgent):
    """Analyst that fetches SEC filings via API, processes XBRL data, and publishes analysis."""

    def __init__(
        self,
        redis_url: str,
        stream_key: str = "sec_stream",
        consumer_group: str = "sec_analyst_group",
        consumer_name: str = "sec_analyst_1",
        openai_api_key: Optional[str] = None,
        sec_api_key: Optional[str] = None,
        mongodb_uri: Optional[str] = None,
        model: str = "gpt-4o-mini",
        logger: Optional[logging.Logger] = None,
        fetch_mode: bool = True,  # If True, fetch from data source; if False, consume from stream
        use_mongodb: bool = True,  # If True, use MongoDB; if False, use SEC API
    ):
        super().__init__(redis_url, stream_key, consumer_group, consumer_name, logger)

        # Initialize LLM client
        self.llm = ChatOpenAI(model=model, api_key=openai_api_key, temperature=0.0)

        # Data source configuration
        self.fetch_mode = fetch_mode
        self.use_mongodb = use_mongodb
        
        # Initialize MongoDB client
        if use_mongodb and PYMONGO_AVAILABLE:
            self.mongodb_uri = mongodb_uri or os.environ.get("MONGODB_URI")
            if self.mongodb_uri:
                try:
                    self.mongo_client = MongoClient(self.mongodb_uri)
                    self.mongo_db = self.mongo_client.get_database("sec_filing")
                    # Note: Collection name has typo in database
                    self.mongo_collection = self.mongo_db["fundamanetal"]
                    self.logger.info("MongoDB client initialized successfully")
                except Exception as e:
                    self.logger.error(f"Failed to initialize MongoDB client: {e}")
                    self.use_mongodb = False
                    self.fetch_mode = False
            else:
                self.logger.warning("MONGODB_URI not provided - MongoDB fetching disabled")
                self.use_mongodb = False
        elif use_mongodb and not PYMONGO_AVAILABLE:
            self.logger.warning("pymongo library not available - MongoDB fetching disabled")
            self.use_mongodb = False
        
        # Initialize SEC API clients (fallback if MongoDB not used)
        if not use_mongodb and fetch_mode and SEC_API_AVAILABLE:
            self.sec_api_key = sec_api_key or os.environ.get("SEC_API_KEY")
            if self.sec_api_key:
                self.query_api = QueryApi(api_key=self.sec_api_key)
                self.xbrl_api = XbrlApi(api_key=self.sec_api_key)
                self.logger.info("SEC API clients initialized")
            else:
                self.logger.warning("SEC_API_KEY not provided - SEC API fetching disabled")
                self.fetch_mode = False
        elif not use_mongodb and fetch_mode and not SEC_API_AVAILABLE:
            self.logger.warning("sec_api library not available - SEC API fetching disabled")
            self.fetch_mode = False

        # Buffer and results
        self.filing_buffer: Dict[str, Dict[str, Any]] = {}
        self.latest_analysis: Dict[str, Dict[str, Any]] = {}
        self.processed_tickers: set = set()  # Track which tickers we've already fetched

        # File used by debate module as a simple integration point
        self._final_reports_file = ("""%s/final_reports.json""" % __import__("os").path.dirname(__file__))

    def fetch_from_mongodb(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Fetch SEC filing data from MongoDB database."""
        if not self.use_mongodb:
            self.logger.warning("MongoDB not configured")
            return None
        
        try:
            self.logger.info(f"Fetching SEC data from MongoDB for {ticker}...")
            
            # Query MongoDB for the ticker
            document = self.mongo_collection.find_one({"ticker": ticker})
            
            if not document:
                self.logger.warning(f"No SEC data found in MongoDB for {ticker}")
                return None
            
            # Convert MongoDB document to expected format
            self.logger.info(f"Found SEC data in MongoDB for {ticker}")
            
            # Extract financials
            financials = document.get("financials", {})
            
            # Handle MongoDB NumberLong format
            def extract_value(field):
                if isinstance(field, dict):
                    if "$numberLong" in field:
                        return float(field["$numberLong"])
                    return field.get("current", field)
                return field
            
            # Build result structure compatible with existing code
            result = {
                "company": document.get("company", ticker),
                "ticker": ticker,
                "report_date": document.get("report_date", datetime.now().isoformat()),
                "form_type": "10-Q",  # Default form type
                "data_source": document.get("data_source", "MongoDB"),
                "financials": {
                    "revenue": {"current": extract_value(financials.get("revenue", {}).get("current"))},
                    "eps": {"current": financials.get("eps", {}).get("current")},
                    "gross_margin_pct": financials.get("gross_margin_pct"),
                    "operating_margin_pct": financials.get("operating_margin_pct"),
                    "free_cash_flow": extract_value(financials.get("free_cash_flow")),
                    "total_debt": extract_value(financials.get("total_debt")),
                    "cash_and_equivalents": extract_value(financials.get("cash_and_equivalents")),
                },
                "shareholder_activity": document.get("shareholder_activity", {
                    "dividend_declared": None,
                    "share_buyback_value": None,
                }),
                "insider_trading_activity": document.get("insider_trading_activity", {
                    "insider_buys": 0,
                    "insider_sells": 0,
                    "net_transactions": 0,
                }),
                "institutional_holding_activity": {
                    "total_institutions": 0,
                },
                "derived_features": document.get("derived_features", {}),
            }
            
            self.logger.info(f"Successfully fetched SEC data from MongoDB for {ticker}")
            return result
            
        except Exception as e:
            self.logger.error(f"Error fetching from MongoDB for {ticker}: {e}", exc_info=True)
            return None

    def fetch_sec_filing_data(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Fetch SEC filing data from SEC API with XBRL extraction (from demo.py logic)."""
        if not self.fetch_mode:
            self.logger.warning("Fetch mode disabled - cannot fetch SEC data")
            return None

        try:
            self.logger.info(f"Fetching SEC 10-Q filing for {ticker}...")
            
            # Query for latest 10-Q filing
            query_10q = {
                "query": f"ticker:{ticker} AND formType:\"10-Q\"",
                "from": "0",
                "size": "1",
                "sort": [{"filedAt": {"order": "desc"}}]
            }
            filings_10q = self.query_api.get_filings(query_10q).get("filings", [])
            
            if not filings_10q:
                self.logger.warning(f"No 10-Q filing found for {ticker}")
                return None
            
            filing = filings_10q[0]
            
            # Find XBRL document URL
            filing_url = None
            data_files = filing.get("dataFiles", [])
            for f in data_files:
                if isinstance(f, dict):
                    url = f.get("url", "")
                    if url.endswith("_htm.xml") or url.endswith(".xml"):
                        filing_url = url
                        break
            
            if not filing_url:
                for f in data_files:
                    if isinstance(f, str) and (f.endswith("_htm.xml") or f.endswith(".xml")):
                        filing_url = f
                        break
            
            if not filing_url:
                filing_url = filing.get("linkToXbrl")
            
            if not filing_url:
                self.logger.warning(f"No XBRL instance document found for {ticker}")
                return None
            
            self.logger.info(f"Parsing XBRL data for {ticker}...")
            xbrl_json = self.xbrl_api.xbrl_to_json(filing_url)
            
            # Categorize sections
            income_sections, balance_sections, cashflow_sections = [], [], []
            for section in xbrl_json.keys():
                section_lower = section.lower()
                if "income" in section_lower or "operations" in section_lower or "earnings" in section_lower:
                    income_sections.append(section)
                if "balance" in section_lower or ("financial" in section_lower and "position" in section_lower):
                    balance_sections.append(section)
                if "cash" in section_lower and "flow" in section_lower:
                    cashflow_sections.append(section)
            
            # Helper to extract values
            def get_val(possible_sections, possible_keys):
                if isinstance(possible_keys, str):
                    possible_keys = [possible_keys]
                for section in possible_sections:
                    if section not in xbrl_json:
                        continue
                    for key in possible_keys:
                        if key in xbrl_json[section]:
                            val_data = xbrl_json[section][key]
                            if isinstance(val_data, list):
                                if len(val_data) > 0:
                                    val_data = val_data[0]
                                else:
                                    continue
                            if isinstance(val_data, dict):
                                val = val_data.get("value")
                            else:
                                val = val_data
                            try:
                                return float(val)
                            except (TypeError, ValueError):
                                return val
                return None
            
            # Extract key financial metrics
            revenue = get_val(income_sections, ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet"])
            eps = get_val(income_sections, ["EarningsPerShareDiluted", "EarningsPerShareBasicAndDiluted"])
            gross_profit = get_val(income_sections, ["GrossProfit", "GrossProfitLoss"])
            op_income = get_val(income_sections, ["OperatingIncomeLoss", "IncomeLossFromOperations"])
            fcf = get_val(cashflow_sections, ["NetCashProvidedByUsedInOperatingActivities"])
            total_debt = get_val(balance_sections, ["LongTermDebtNoncurrent", "LongTermDebt"])
            cash = get_val(balance_sections, ["CashAndCashEquivalentsAtCarryingValue", "Cash"])
            dividend = get_val(income_sections, ["CommonStockDividendsPerShareDeclared"])
            buyback = get_val(cashflow_sections, ["PaymentsForRepurchaseOfCommonStock"])
            
            # Calculate margins
            op_margin_pct = (op_income / revenue * 100) if op_income and revenue else None
            gross_margin_pct = (gross_profit / revenue * 100) if gross_profit and revenue else None
            
            # Fetch earnings surprises using yfinance if available
            eps_surprise, rev_surprise = None, None
            if YFINANCE_AVAILABLE:
                try:
                    ticker_obj = yf.Ticker(ticker)
                    hist = ticker_obj.quarterly_earnings
                    if hist is not None and not hist.empty and len(hist) >= 2:
                        current_eps = hist["Earnings"].iloc[0]
                        previous_eps = hist["Earnings"].iloc[1]
                        current_rev = hist["Revenue"].iloc[0]
                        previous_rev = hist["Revenue"].iloc[1]
                        if previous_eps and previous_eps != 0:
                            eps_surprise = ((current_eps - previous_eps) / abs(previous_eps)) * 100
                        if previous_rev and previous_rev != 0:
                            rev_surprise = ((current_rev - previous_rev) / abs(previous_rev)) * 100
                except Exception as e:
                    self.logger.warning(f"Could not fetch earnings surprise data: {e}")
            
            # Fetch insider trading data (Form 4)
            query_form4 = {
                "query": f"ticker:{ticker} AND formType:\"4\"",
                "from": "0",
                "size": "20",
                "sort": [{"filedAt": {"order": "desc"}}]
            }
            filings_form4 = self.query_api.get_filings(query_form4).get("filings", [])
            insider_buys = sum(1 for f in filings_form4 if "Buy" in str(f.get("documents", "")).title())
            insider_sells = sum(1 for f in filings_form4 if "Sell" in str(f.get("documents", "")).title())
            net_transactions = insider_buys - insider_sells
            
            # Fetch institutional holdings (Form 13F)
            query_13f = {
                "query": f"ticker:{ticker} AND formType:\"13F-HR\"",
                "from": "0",
                "size": "1",
                "sort": [{"filedAt": {"order": "desc"}}]
            }
            filings_13f = self.query_api.get_filings(query_13f).get("filings", [])
            total_institutions = len(filings_13f)
            
            # Build result structure
            result = {
                "company": ticker,
                "ticker": ticker,
                "report_date": filing.get("filedAt"),
                "form_type": "10-Q",
                "financials": {
                    "revenue": {"current": revenue},
                    "eps": {"current": eps},
                    "gross_margin_pct": gross_margin_pct,
                    "operating_margin_pct": op_margin_pct,
                    "free_cash_flow": fcf,
                    "total_debt": total_debt,
                    "cash_and_equivalents": cash,
                },
                "shareholder_activity": {
                    "dividend_declared": dividend,
                    "share_buyback_value": buyback,
                },
                "insider_trading_activity": {
                    "insider_buys": insider_buys,
                    "insider_sells": insider_sells,
                    "net_transactions": net_transactions,
                },
                "institutional_holding_activity": {
                    "total_institutions": total_institutions,
                },
                "derived_features": {
                    "eps_surprise_pct": eps_surprise,
                    "revenue_surprise_pct": rev_surprise,
                    "insider_buy_ratio": (insider_buys / max(insider_sells, 1)) if (insider_buys or insider_sells) else None,
                },
            }
            
            self.logger.info(f"Successfully fetched SEC data for {ticker}")
            return result
            
        except Exception as e:
            self.logger.error(f"Error fetching SEC data for {ticker}: {e}", exc_info=True)
            return None

    def format_sec_data_for_analysis(self, sec_data: Dict[str, Any]) -> str:
        """Format SEC data into readable text for LLM analysis."""
        prompt_parts = []
        prompt_parts.append("=== SEC FILING DATA FOR ANALYSIS ===\n")
        prompt_parts.append(f"Company: {sec_data['company']} ({sec_data['ticker']})")
        prompt_parts.append(f"Form Type: {sec_data.get('form_type', 'N/A')}")
        prompt_parts.append(f"Report Date: {sec_data['report_date']}\n")
        
        financials = sec_data['financials']
        prompt_parts.append("FINANCIAL METRICS:")
        if financials.get('revenue', {}).get('current'):
            prompt_parts.append(f"- Revenue: ${financials['revenue']['current']:,.0f}")
        else:
            prompt_parts.append("- Revenue: N/A")
        if financials.get('eps', {}).get('current'):
            prompt_parts.append(f"- EPS (Diluted): ${financials['eps']['current']:.2f}")
        else:
            prompt_parts.append("- EPS: N/A")
        if financials.get('gross_margin_pct'):
            prompt_parts.append(f"- Gross Margin: {financials['gross_margin_pct']:.2f}%")
        else:
            prompt_parts.append("- Gross Margin: N/A")
        if financials.get('operating_margin_pct'):
            prompt_parts.append(f"- Operating Margin: {financials['operating_margin_pct']:.2f}%")
        else:
            prompt_parts.append("- Operating Margin: N/A")
        if financials.get('free_cash_flow'):
            prompt_parts.append(f"- Free Cash Flow: ${financials['free_cash_flow']:,.0f}")
        else:
            prompt_parts.append("- Free Cash Flow: N/A")
        if financials.get('total_debt'):
            prompt_parts.append(f"- Total Debt: ${financials['total_debt']:,.0f}")
        else:
            prompt_parts.append("- Total Debt: N/A")
        if financials.get('cash_and_equivalents'):
            prompt_parts.append(f"- Cash & Equivalents: ${financials['cash_and_equivalents']:,.0f}")
        else:
            prompt_parts.append("- Cash: N/A")
        prompt_parts.append("")
        
        prompt_parts.append("SHAREHOLDER ACTIVITY:")
        shareholder = sec_data['shareholder_activity']
        if shareholder.get('dividend_declared'):
            prompt_parts.append(f"- Dividend Declared: ${shareholder['dividend_declared']}")
        else:
            prompt_parts.append("- Dividend: N/A")
        if shareholder.get('share_buyback_value'):
            prompt_parts.append(f"- Share Buyback: ${shareholder['share_buyback_value']:,.0f}")
        else:
            prompt_parts.append("- Share Buyback: N/A")
        prompt_parts.append("")
        
        prompt_parts.append("INSIDER TRADING ACTIVITY:")
        insider = sec_data['insider_trading_activity']
        prompt_parts.append(f"- Insider Buys: {insider.get('insider_buys', 0)}")
        prompt_parts.append(f"- Insider Sells: {insider.get('insider_sells', 0)}")
        prompt_parts.append(f"- Net Transactions: {insider.get('net_transactions', 0)}")
        prompt_parts.append("")
        
        prompt_parts.append("DERIVED SIGNALS:")
        derived = sec_data['derived_features']
        if derived.get('eps_surprise_pct'):
            prompt_parts.append(f"- EPS Surprise: {derived['eps_surprise_pct']:.2f}%")
        else:
            prompt_parts.append("- EPS Surprise: N/A")
        if derived.get('revenue_surprise_pct'):
            prompt_parts.append(f"- Revenue Surprise: {derived['revenue_surprise_pct']:.2f}%")
        else:
            prompt_parts.append("- Revenue Surprise: N/A")
        if derived.get('insider_buy_ratio'):
            prompt_parts.append(f"- Insider Buy Ratio: {derived['insider_buy_ratio']:.2f}")
        else:
            prompt_parts.append("- Insider Buy Ratio: Neutral")
        
        return "\n".join(prompt_parts)

    async def fetch_and_analyze_ticker(self, ticker: str):
        """Fetch SEC data for a ticker and analyze it."""
        if ticker in self.processed_tickers:
            self.logger.info(f"Already processed SEC data for {ticker}, skipping")
            return self.latest_analysis.get(ticker)
        
        # Fetch SEC filing data from appropriate source
        if self.use_mongodb:
            sec_data = self.fetch_from_mongodb(ticker)
        else:
            sec_data = self.fetch_sec_filing_data(ticker)
        
        if not sec_data:
            self.logger.warning(f"No SEC data available for {ticker}")
            return {"error": "No analysis available"}
        
        # Format and analyze
        formatted_data = self.format_sec_data_for_analysis(sec_data)
        analysis = await self.analyze_sec_filing(ticker, formatted_data, sec_data)
        
        # Store results
        self.latest_analysis[ticker] = analysis
        self.processed_tickers.add(ticker)
        
        # Update final_reports.json
        try:
            try:
                with open(self._final_reports_file, "r") as f:
                    existing = json.load(f)
            except Exception:
                existing = {"agents": []}

            agents = existing.get("agents", [])
            filtered = [a for a in agents if a.get("agent") != "sec_report_analyst"]
            filtered.append({"agent": "sec_report_analyst", "output": analysis})
            existing["agents"] = filtered

            with open(self._final_reports_file, "w") as f:
                json.dump(existing, f, indent=2)
        except Exception as e:
            self.logger.warning(f"Failed to persist final_reports.json: {e}")

        self.logger.info(f"Completed SEC analysis for {ticker}")
        return analysis

    async def process_message(self, message_id: str, data: Dict[str, Any]):
        """Process a single SEC filing message and summarize it."""
        try:
            raw = json.loads(data.get("data", "{}"))
            
            # Check if this is a ticker request (for fetch mode)
            if self.fetch_mode and "ticker" in raw and "fetch_request" in raw:
                ticker = raw["ticker"]
                await self.fetch_and_analyze_ticker(ticker)
                return
            
            # Otherwise process as a regular filing message
            ticker = raw.get("ticker") or raw.get("symbol") or raw.get("company") or "UNKNOWN"

            # Keep last file per ticker
            self.filing_buffer[ticker] = raw

            # Summarize filing with LLM
            analysis = await self.analyze_filing(ticker, raw)
            self.latest_analysis[ticker] = analysis

            # Append/update final_reports.json for downstream consumers (debate module)
            try:
                # Load existing
                try:
                    with open(self._final_reports_file, "r") as f:
                        existing = json.load(f)
                except Exception:
                    existing = {"agents": []}

                # Replace or append sec_report_analyst entry
                agents = existing.get("agents", [])
                filtered = [a for a in agents if a.get("agent") != "sec_report_analyst"]
                filtered.append({"agent": "sec_report_analyst", "output": analysis})
                existing["agents"] = filtered

                with open(self._final_reports_file, "w") as f:
                    json.dump(existing, f, indent=2)
            except Exception as e:
                self.logger.warning(f"Failed to persist final_reports.json: {e}")

            self.logger.info(f"Updated SEC filing analysis for {ticker}")

        except Exception as e:
            self.logger.error(f"Error processing SEC filing message: {e}", exc_info=True)

    async def fetch_for_tickers(self, tickers: list):
        """Fetch and analyze SEC data for a list of tickers (used when in fetch mode)."""
        if not self.fetch_mode:
            self.logger.warning("Fetch mode is disabled, cannot fetch for tickers")
            return
        
        for ticker in tickers:
            await self.fetch_and_analyze_ticker(ticker)

    async def analyze_sec_filing(self, ticker: str, formatted_data: str, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Use the LLM to produce a comprehensive analysis of the SEC filing data."""
        try:
            prompt = ChatPromptTemplate.from_messages([
                ("system", "You are an expert SEC filings analyst with deep knowledge of financial statements, XBRL data, and fundamental analysis."),
                ("user", """Analyze the following SEC filing data and provide a comprehensive fundamental analysis report:

{sec_filing_data}

Please provide:
1. Financial Health Assessment (revenue, margins, cash flow, debt)
2. Capital Allocation Analysis (buybacks, dividends, debt management)
3. Insider Activity Interpretation
4. Key Strengths and Weaknesses
5. Investment Outlook

Structure your response with clear sections and include a summary table:
| Metric Category | Assessment | Key Insight |
|-----------------|------------|-------------|
| [Category] | [Strong/Moderate/Weak] | [Brief insight] |

Keep the analysis focused on quantitative fundamentals and actionable insights."""),
            ])

            chain = prompt | self.llm
            response = await chain.ainvoke({"sec_filing_data": formatted_data})

            return {
                "ticker": ticker,
                "timestamp": datetime.now().isoformat(),
                "analysis_type": "sec_filing",
                "form_type": raw_data.get("form_type", "N/A"),
                "report_date": raw_data.get("report_date", "N/A"),
                "summary": response.content,
                "financials": raw_data.get("financials", {}),
                "derived_features": raw_data.get("derived_features", {}),
            }

        except Exception as e:
            self.logger.error(f"Error analyzing SEC filing for {ticker}: {e}", exc_info=True)
            return {"ticker": ticker, "error": str(e)}

    async def analyze_filing(self, ticker: str, filing: Dict[str, Any]) -> Dict[str, Any]:
        """Use the LLM to produce a concise structured summary of the filing (legacy method for stream consumption)."""
        try:
            content = filing.get("content") or filing.get("text") or filing.get("body") or str(filing)

            prompt = ChatPromptTemplate.from_messages([
                ("system", "You are an expert SEC filings analyst. Extract key facts, material events, and risk items."),
                ("user", "Summarize the following SEC filing for {ticker} and list 3 key takeaways and 2 risks:\n\n{content}"),
            ])

            chain = prompt | self.llm
            response = await chain.ainvoke({"ticker": ticker, "content": content[:4000]})

            return {
                "ticker": ticker,
                "timestamp": datetime.now().isoformat(),
                "analysis_type": "sec_filing",
                "summary": response.content,
                "source_raw": filing,
            }

        except Exception as e:
            self.logger.error(f"Error analyzing filing for {ticker}: {e}", exc_info=True)
            return {"ticker": ticker, "error": str(e)}


if __name__ == "__main__":
    import asyncio
    from src.core.redis_client import RedisStreamClient

    logging.basicConfig(level=logging.INFO)

    async def main():
        agent = SecReportAnalystRedis(redis_url="redis://localhost:6379", openai_api_key=None)
        try:
            await agent.run()
        except KeyboardInterrupt:
            await agent.stop()
        finally:
            await RedisStreamClient.close()

    asyncio.run(main())
