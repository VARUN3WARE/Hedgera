"""Test the fixed state vector shape."""
import numpy as np

# Configuration matching FinRL model
num_stocks = 30
num_indicators = 8

# Simulate data
stocks = np.zeros(num_stocks, dtype=float)
price = np.array([100.0] * num_stocks, dtype=np.float32)
tech = np.array([0.5] * (num_stocks * num_indicators), dtype=np.float32) * 2**-7  # scaled
turbulence_bool = 0

# Build NEW state vector (simplified)
scale = np.array(2**-6, dtype=np.float32)
state = np.hstack((
    turbulence_bool,  # 1 value
    price * scale,    # 30 values
    stocks * scale,   # 30 values  
    tech,             # 240 values (8 indicators * 30 stocks)
)).astype(np.float32)

print(f"State vector composition:")
print(f"  turbulence_bool: 1")
print(f"  price: {len(price)}")
print(f"  stocks: {len(stocks)}")
print(f"  tech: {len(tech)}")
print(f"\nTotal shape: {state.shape}")
print(f"Expected: (301,)")
print(f"Match: {state.shape[0] == 301}")

# Verify breakdown
print(f"\nVerification: 1 + 30 + 30 + 240 = {1 + 30 + 30 + 240}")
