#!/usr/bin/env python3
"""
Test Pathway Integration

Quick test to verify Pathway aggregator works correctly.
"""

import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent))

# Mock agent results for testing
MOCK_AGENT_RESULTS = {
    "AAPL": {
        "ticker": "AAPL",
        "timestamp": datetime.now().isoformat(),
        "news_analysis": {
            "sentiment": 0.75,
            "confidence": 0.82,
            "decision": "BUY"
        },
        "social_analysis": {
            "sentiment": 0.68,
            "confidence": 0.78,
            "decision": "BUY"
        },
        "market_analysis": {
            "sentiment": 0.72,
            "confidence": 0.85,
            "decision": "BUY"
        },
        "sec_analysis": {
            "sentiment": 0.55,
            "confidence": 0.65,
            "decision": "HOLD"
        },
        "debate_result": {
            "validation": {
                "final_recommendation": {
                    "sentiment": 0.70,
                    "conviction": 0.80,
                    "decision": "BUY"
                }
            }
        }
    },
    "MSFT": {
        "ticker": "MSFT",
        "timestamp": datetime.now().isoformat(),
        "news_analysis": {
            "sentiment": 0.45,
            "confidence": 0.72,
            "decision": "HOLD"
        },
        "social_analysis": {
            "sentiment": 0.52,
            "confidence": 0.68,
            "decision": "HOLD"
        },
        "market_analysis": {
            "sentiment": 0.48,
            "confidence": 0.75,
            "decision": "HOLD"
        },
        "sec_analysis": {
            "sentiment": 0.50,
            "confidence": 0.70,
            "decision": "HOLD"
        },
        "debate_result": {
            "validation": {
                "final_recommendation": {
                    "sentiment": 0.49,
                    "conviction": 0.71,
                    "decision": "HOLD"
                }
            }
        }
    }
}


def test_pathway_aggregation():
    """Test Pathway aggregator with mock data"""
    print("\n" + "=" * 80)
    print("🧪 TESTING PATHWAY INTEGRATION")
    print("=" * 80)
    
    try:
        from backend.src.pathway_integration.aggregator import PathwayAggregator
        
        # Create test directory
        test_dir = Path("test_pathway_output")
        test_dir.mkdir(exist_ok=True)
        
        # Initialize aggregator
        aggregator = PathwayAggregator(test_dir)
        print("\n✅ PathwayAggregator initialized")
        
        # Run aggregation
        print("\n⚡ Running Pathway aggregation on mock data...")
        results = aggregator.aggregate_agent_results(MOCK_AGENT_RESULTS)
        
        # Display results
        print("\n📊 AGGREGATION RESULTS:")
        print("=" * 80)
        for ticker, metrics in results.items():
            print(f"\n{ticker}:")
            print(f"  Consensus Score:      {metrics['consensus_score']:.1f}%")
            print(f"  Sentiment Agreement:  {metrics['sentiment_agreement']:.1f}%")
            print(f"  Avg Confidence:       {metrics['avg_confidence']:.2f}")
            print(f"  Voting: BUY={metrics['buy_votes']} SELL={metrics['sell_votes']} HOLD={metrics['hold_votes']}")
            print(f"  Total Agents:         {metrics['total_agents']}")
        
        print("\n" + "=" * 80)
        print("✅ TEST PASSED - Pathway integration working!")
        print("=" * 80)
        print(f"\nResults saved to: {test_dir / 'pathway_aggregated.json'}")
        
        return True
        
    except ImportError as e:
        print(f"\n❌ Import Error: {e}")
        print("\n⚠️  Pathway not installed. Run:")
        print("   pip install pathway>=0.7.0")
        return False
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_pathway_aggregation()
    sys.exit(0 if success else 1)
