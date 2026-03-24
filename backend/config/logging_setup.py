"""
Logging configuration for AEGIS Trading System.
"""
import logging
import sys
import json
from pathlib import Path
from datetime import datetime
from logging.handlers import RotatingFileHandler


def setup_logging(log_dir: str = "backend/logs", level: str = "INFO"):
    """Setup comprehensive logging for all pipeline components."""
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    
    session_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))
    root_logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    
    # Pipeline log
    pipeline_handler = RotatingFileHandler(
        log_path / f"pipeline_{session_time}.log",
        maxBytes=10*1024*1024,
        backupCount=5
    )
    pipeline_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    pipeline_handler.setFormatter(file_formatter)
    root_logger.addHandler(pipeline_handler)
    
    # Error log
    error_handler = RotatingFileHandler(
        log_path / f"errors_{session_time}.log",
        maxBytes=10*1024*1024,
        backupCount=5
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(file_formatter)
    root_logger.addHandler(error_handler)
    
    return {
        'session_time': session_time,
        'log_dir': str(log_path),
        'raw_data_log': str(log_path / f"raw_data_{session_time}.jsonl"),
        'processed_data_log': str(log_path / f"processed_data_{session_time}.jsonl"),
        'finrl_decisions_log': str(log_path / f"finrl_decisions_{session_time}.jsonl"),
    }


def log_data(log_file: str, data: dict):
    """Log data in JSONL format."""
    with open(log_file, 'a') as f:
        f.write(json.dumps(data) + '\n')
