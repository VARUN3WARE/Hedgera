"""
Pathway Aggregator for AEGIS Agent Results

Lightweight batch processing using Pathway's groupby() and reduce() operators.
NO streaming - processes agent results after they complete.
"""

import pathway as pw
from typing import Dict, Any, List
from pathlib import Path
import json
import logging

logger = logging.getLogger(__name__)


class PathwayAggregator:
    """
    Aggregates multi-agent trading decisions using Pathway.
    
    Uses only:
    - pw.schema_from_types() - Define data structure
    - groupby() - Group by ticker
    - reduce() - Aggregate metrics
    
    NO streaming connectors - pure batch processing.
    """
    
    def __init__(self, cycle_log_dir: Path):
        """
        Initialize aggregator.
        
        Args:
            cycle_log_dir: Directory to save intermediate files
        """
        self.cycle_log_dir = Path(cycle_log_dir)
        self.cycle_log_dir.mkdir(exist_ok=True, parents=True)
    
    def aggregate_agent_results(self, agent_results: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
        """
        Aggregate agent results using Pathway operations.
        
        Args:
            agent_results: Dict mapping ticker -> agent analysis results
            
        Returns:
            Dict mapping ticker -> aggregated metrics
            {
                "AAPL": {
                    "avg_sentiment": 0.75,
                    "sentiment_std": 0.12,
                    "avg_confidence": 0.82,
                    "buy_votes": 3,
                    "sell_votes": 0,
                    "hold_votes": 1,
                    "total_agents": 4,
                    "consensus_score": 75.0,
                    "sentiment_agreement": 88.0
                }
            }
        """
        try:
            logger.info("⚡ Pathway: Starting batch aggregation...")
            
            # Extract agent data into flat structure
            agent_records = self._extract_agent_records(agent_results)
            
            if not agent_records:
                logger.warning("⚠️  Pathway: No agent records to process")
                return {}
            
            # Define Pathway schema
            schema = pw.schema_from_types(
                ticker=str,
                agent=str,
                sentiment=float,
                confidence=float,
                decision=str
            )
            
            # Create Pathway table from records
            agent_table = pw.debug.table_from_rows(
                schema=schema,
                rows=agent_records
            )
            
            # ═══════════════════════════════════════════════════════
            # GROUPBY + REDUCE: Aggregate by ticker
            # ═══════════════════════════════════════════════════════
            aggregated = agent_table.groupby(agent_table.ticker).reduce(
                ticker=pw.this.ticker,
                
                # Sentiment metrics
                avg_sentiment=pw.reducers.avg(pw.this.sentiment),
                sentiment_std=pw.reducers.stddev(pw.this.sentiment),
                
                # Confidence metrics
                avg_confidence=pw.reducers.avg(pw.this.confidence),
                min_confidence=pw.reducers.min(pw.this.confidence),
                max_confidence=pw.reducers.max(pw.this.confidence),
                
                # Decision voting
                buy_votes=pw.reducers.count(pw.if_else(pw.this.decision == "BUY", 1, None)),
                sell_votes=pw.reducers.count(pw.if_else(pw.this.decision == "SELL", 1, None)),
                hold_votes=pw.reducers.count(pw.if_else(pw.this.decision == "HOLD", 1, None)),
                
                # Agent count
                total_agents=pw.reducers.count()
            )
            
            # ═══════════════════════════════════════════════════════
            # ADD COMPUTED COLUMNS: Consensus scores
            # ═══════════════════════════════════════════════════════
            enriched = aggregated.select(
                pw.this.ticker,
                pw.this.avg_sentiment,
                pw.this.sentiment_std,
                pw.this.avg_confidence,
                pw.this.min_confidence,
                pw.this.max_confidence,
                pw.this.buy_votes,
                pw.this.sell_votes,
                pw.this.hold_votes,
                pw.this.total_agents,
                
                # Consensus score: % of agents agreeing on BUY
                consensus_score=pw.apply(
                    lambda buy, total: (buy / total * 100) if total > 0 else 0.0,
                    pw.this.buy_votes,
                    pw.this.total_agents
                ),
                
                # Sentiment agreement: inverse of std deviation (0-100 scale)
                sentiment_agreement=pw.apply(
                    lambda std: max(0.0, min(100.0, 100.0 - (std * 100.0))) if std is not None else 0.0,
                    pw.this.sentiment_std
                )
            )
            
            # Compute and extract results
            result_table = pw.debug.compute_and_print(enriched, include_id=False)
            
            # Convert to dict format
            aggregated_metrics = self._table_to_dict(result_table)
            
            # Save results
            output_path = self.cycle_log_dir / "pathway_aggregated.json"
            with open(output_path, 'w') as f:
                json.dump(aggregated_metrics, f, indent=2)
            
            logger.info(f"✅ Pathway: Aggregated {len(aggregated_metrics)} tickers")
            
            return aggregated_metrics
            
        except Exception as e:
            logger.error(f"❌ Pathway aggregation failed: {e}", exc_info=True)
            return {}
    
    def _extract_agent_records(self, agent_results: Dict[str, Any]) -> List[tuple]:
        """
        Extract flat records from nested agent results.
        
        Returns list of tuples: (ticker, agent, sentiment, confidence, decision)
        """
        records = []
        
        for ticker, result in agent_results.items():
            if "error" in result:
                continue
            
            # Extract debate result
            debate = result.get('debate_result', {})
            validator = debate.get('validation', {}).get('final_recommendation', {})
            
            # News agent
            news = result.get('news_analysis', {})
            if news:
                records.append((
                    ticker,
                    'news',
                    float(news.get('sentiment', 0.5)),
                    float(news.get('confidence', 0.5)),
                    str(news.get('decision', 'HOLD'))
                ))
            
            # Social agent
            social = result.get('social_analysis', {})
            if social:
                records.append((
                    ticker,
                    'social',
                    float(social.get('sentiment', 0.5)),
                    float(social.get('confidence', 0.5)),
                    str(social.get('decision', 'HOLD'))
                ))
            
            # Market agent
            market = result.get('market_analysis', {})
            if market:
                records.append((
                    ticker,
                    'market',
                    float(market.get('sentiment', 0.5)),
                    float(market.get('confidence', 0.5)),
                    str(market.get('decision', 'HOLD'))
                ))
            
            # SEC agent
            sec = result.get('sec_analysis', {})
            if sec:
                records.append((
                    ticker,
                    'sec',
                    float(sec.get('sentiment', 0.5)),
                    float(sec.get('confidence', 0.5)),
                    str(sec.get('decision', 'HOLD'))
                ))
            
            # Validator (final recommendation)
            if validator:
                records.append((
                    ticker,
                    'validator',
                    float(validator.get('sentiment', 0.5)),
                    float(validator.get('conviction', 0.5)),
                    str(validator.get('decision', 'HOLD'))
                ))
        
        return records
    
    def _table_to_dict(self, result_table) -> Dict[str, Dict[str, float]]:
        """Convert Pathway result table to dict format."""
        aggregated = {}
        
        # Result table is a list of rows
        for row in result_table:
            ticker = row[0]  # First column is ticker
            aggregated[ticker] = {
                'avg_sentiment': float(row[1]),
                'sentiment_std': float(row[2]) if row[2] is not None else 0.0,
                'avg_confidence': float(row[3]),
                'min_confidence': float(row[4]),
                'max_confidence': float(row[5]),
                'buy_votes': int(row[6]),
                'sell_votes': int(row[7]),
                'hold_votes': int(row[8]),
                'total_agents': int(row[9]),
                'consensus_score': float(row[10]),
                'sentiment_agreement': float(row[11])
            }
        
        return aggregated


def compute_pathway_metrics(agent_results: Dict[str, Any], cycle_log_dir: Path) -> Dict[str, Dict[str, float]]:
    """
    Convenience function for computing Pathway metrics.
    
    Args:
        agent_results: Agent analysis results
        cycle_log_dir: Directory for logs
        
    Returns:
        Aggregated metrics per ticker
    """
    aggregator = PathwayAggregator(cycle_log_dir)
    return aggregator.aggregate_agent_results(agent_results)
