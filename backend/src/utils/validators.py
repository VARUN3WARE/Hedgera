"""Validators"""

def validate_symbol(symbol: str) -> bool:
    """Validate trading symbol"""
    return len(symbol) > 0 and symbol.isupper()

def validate_price(price: float) -> bool:
    """Validate price"""
    return price > 0
