"""Data formatters"""
from datetime import datetime

def format_timestamp(dt: datetime) -> str:
    """Format timestamp"""
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def format_number(num: float, decimals: int = 2) -> str:
    """Format number"""
    return f"{num:.{decimals}f}"
