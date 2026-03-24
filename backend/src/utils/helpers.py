"""Helper functions"""

def format_currency(amount: float) -> str:
    """Format currency"""
    return f"${amount:,.2f}"

def calculate_percentage(part: float, whole: float) -> float:
    """Calculate percentage"""
    return (part / whole) * 100 if whole != 0 else 0
