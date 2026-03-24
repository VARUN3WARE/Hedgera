"""
Detailed logging utilities for deep-dive debugging.
Logs complete data snapshots at each pipeline stage.
"""
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List
import pandas as pd

logger = logging.getLogger(__name__)


class DetailedLogger:
    """Logs detailed data snapshots for debugging and analysis."""
    
    def __init__(self, session_dir: str = None):
        """Initialize detailed logger with session directory."""
        if session_dir is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            session_dir = f"backend/logs/detailed_{timestamp}"
        
        self.session_dir = Path(session_dir)
        self.session_dir.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories for different stages
        (self.session_dir / "01_raw_fetches").mkdir(exist_ok=True)
        (self.session_dir / "02_processed_data").mkdir(exist_ok=True)
        (self.session_dir / "03_finrl_input").mkdir(exist_ok=True)
        (self.session_dir / "04_finrl_output").mkdir(exist_ok=True)
        (self.session_dir / "05_snapshots").mkdir(exist_ok=True)
        
        logger.info(f"📊 Detailed logging enabled: {self.session_dir}")
    
    def log_raw_fetch(self, producer_name: str, data: Dict[str, Any], fetch_count: int):
        """Log raw data fetched by producers."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        filename = self.session_dir / "01_raw_fetches" / f"{producer_name}_{fetch_count:04d}_{timestamp}.json"
        
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "producer": producer_name,
            "fetch_number": fetch_count,
            "data_summary": {
                "total_items": len(data) if isinstance(data, (list, dict)) else 1,
                "data_type": type(data).__name__
            },
            "full_data": data
        }
        
        with open(filename, 'w') as f:
            json.dump(log_entry, f, indent=2, default=str)
        
        logger.info(f"📥 Raw fetch logged: {producer_name} #{fetch_count} → {filename.name}")
    
    def log_processed_data(self, ticker: str, indicators: Dict[str, Any], sequence_num: int):
        """Log processed data with calculated indicators."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        filename = self.session_dir / "02_processed_data" / f"{ticker}_{sequence_num:06d}_{timestamp}.json"
        
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "ticker": ticker,
            "sequence": sequence_num,
            "indicators": indicators,
            "indicator_summary": {
                "macd": indicators.get("macd", 0),
                "rsi_30": indicators.get("rsi_30", 0),
                "close": indicators.get("close", 0),
                "volume": indicators.get("volume", 0)
            }
        }
        
        with open(filename, 'w') as f:
            json.dump(log_entry, f, indent=2, default=str)
    
    def log_finrl_input(self, df: pd.DataFrame, cycle_number: int):
        """Log complete DataFrame passed to FinRL model."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save as CSV for easy inspection
        csv_file = self.session_dir / "03_finrl_input" / f"cycle_{cycle_number:03d}_{timestamp}.csv"
        df.to_csv(csv_file, index=False)
        
        # Save summary as JSON
        json_file = self.session_dir / "03_finrl_input" / f"cycle_{cycle_number:03d}_{timestamp}_summary.json"
        
        summary = {
            "timestamp": datetime.now().isoformat(),
            "cycle_number": cycle_number,
            "dataframe_info": {
                "total_rows": len(df),
                "total_columns": len(df.columns),
                "columns": list(df.columns),
                "tickers": sorted(df['tic'].unique().tolist()) if 'tic' in df.columns else [],
                "ticker_count": df['tic'].nunique() if 'tic' in df.columns else 0,
                "date_range": {
                    "start": df['timestamp'].min() if 'timestamp' in df.columns else None,
                    "end": df['timestamp'].max() if 'timestamp' in df.columns else None,
                    "total_timestamps": df['timestamp'].nunique() if 'timestamp' in df.columns else 0
                }
            },
            "data_quality": {
                "null_counts": df.isnull().sum().to_dict(),
                "data_types": df.dtypes.astype(str).to_dict()
            },
            "sample_data": {
                "first_5_rows": df.head(5).to_dict('records'),
                "last_5_rows": df.tail(5).to_dict('records')
            }
        }
        
        # Add per-ticker statistics
        if 'tic' in df.columns:
            ticker_stats = {}
            for ticker in df['tic'].unique():
                ticker_df = df[df['tic'] == ticker]
                ticker_stats[ticker] = {
                    "row_count": len(ticker_df),
                    "price_range": {
                        "min": float(ticker_df['close'].min()) if 'close' in ticker_df.columns else None,
                        "max": float(ticker_df['close'].max()) if 'close' in ticker_df.columns else None,
                        "mean": float(ticker_df['close'].mean()) if 'close' in ticker_df.columns else None
                    },
                    "indicators": {
                        "macd": float(ticker_df['macd'].iloc[-1]) if 'macd' in ticker_df.columns and len(ticker_df) > 0 else None,
                        "rsi_30": float(ticker_df['rsi_30'].iloc[-1]) if 'rsi_30' in ticker_df.columns and len(ticker_df) > 0 else None,
                        "cci_30": float(ticker_df['cci_30'].iloc[-1]) if 'cci_30' in ticker_df.columns and len(ticker_df) > 0 else None
                    }
                }
            summary["ticker_statistics"] = ticker_stats
        
        with open(json_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        logger.info(f"📊 FinRL input logged: {len(df)} rows, {df['tic'].nunique() if 'tic' in df.columns else 0} tickers")
        logger.info(f"   CSV: {csv_file.name}")
        logger.info(f"   Summary: {json_file.name}")
    
    def log_finrl_output(self, decisions: Dict[str, Any], cycle_number: int):
        """Log FinRL output decisions."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = self.session_dir / "04_finrl_output" / f"cycle_{cycle_number:03d}_{timestamp}.json"
        
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "cycle_number": cycle_number,
            "model_used": decisions.get("model_used", "unknown"),
            "selected_tickers": decisions.get("selected_tickers", []),
            "ticker_count": len(decisions.get("selected_tickers", [])),
            "decisions": decisions.get("decisions", {}),
            "full_output": decisions
        }
        
        with open(filename, 'w') as f:
            json.dump(log_entry, f, indent=2, default=str)
        
        logger.info(f"🤖 FinRL output logged: {len(decisions.get('selected_tickers', []))} tickers selected")
        logger.info(f"   File: {filename.name}")
        
        # Log human-readable summary
        summary_file = self.session_dir / "04_finrl_output" / f"cycle_{cycle_number:03d}_{timestamp}_SUMMARY.txt"
        with open(summary_file, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write(f"FinRL OUTPUT SUMMARY - Cycle {cycle_number}\n")
            f.write(f"Timestamp: {datetime.now().isoformat()}\n")
            f.write("=" * 80 + "\n\n")
            
            f.write(f"Model Used: {decisions.get('model_used', 'unknown')}\n")
            f.write(f"Selected Tickers: {len(decisions.get('selected_tickers', []))}\n\n")
            
            f.write("DECISIONS:\n")
            f.write("-" * 80 + "\n")
            for ticker, decision in decisions.get("decisions", {}).items():
                f.write(f"\n{ticker}:\n")
                f.write(f"  Action: {decision.get('action', 'N/A')}\n")
                f.write(f"  Shares: {decision.get('shares', 0)}\n")
                f.write(f"  Price: ${decision.get('price', 0):.2f}\n")
                f.write(f"  Signal Strength: {decision.get('signal_strength', 0):.2f}\n")
    
    def create_snapshot(self, stage: str, data: Dict[str, Any]):
        """Create a snapshot of current pipeline state."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = self.session_dir / "05_snapshots" / f"{stage}_{timestamp}.json"
        
        snapshot = {
            "timestamp": datetime.now().isoformat(),
            "stage": stage,
            "data": data
        }
        
        with open(filename, 'w') as f:
            json.dump(snapshot, f, indent=2, default=str)
        
        logger.info(f"📸 Snapshot created: {stage} → {filename.name}")
    
    def create_summary_report(self):
        """Create a comprehensive summary report of the entire session."""
        report_file = self.session_dir / "SESSION_REPORT.txt"
        
        with open(report_file, 'w') as f:
            f.write("=" * 100 + "\n")
            f.write("AEGIS TRADING SYSTEM - DETAILED SESSION REPORT\n")
            f.write("=" * 100 + "\n")
            f.write(f"Session Directory: {self.session_dir}\n")
            f.write(f"Report Generated: {datetime.now().isoformat()}\n")
            f.write("=" * 100 + "\n\n")
            
            # Count files in each directory
            for subdir in ["01_raw_fetches", "02_processed_data", "03_finrl_input", "04_finrl_output", "05_snapshots"]:
                full_path = self.session_dir / subdir
                file_count = len(list(full_path.glob("*")))
                f.write(f"{subdir}: {file_count} files\n")
            
            f.write("\n" + "=" * 100 + "\n")
            f.write("All detailed logs are available in the respective subdirectories.\n")
            f.write("=" * 100 + "\n")
        
        logger.info(f"📋 Session report created: {report_file}")
        return report_file


# Global instance
_detailed_logger = None

def get_detailed_logger(session_dir: str = None) -> DetailedLogger:
    """Get or create the global detailed logger instance."""
    global _detailed_logger
    if _detailed_logger is None:
        _detailed_logger = DetailedLogger(session_dir)
    return _detailed_logger
