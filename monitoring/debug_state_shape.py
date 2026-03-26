"""Debug script to check state vector shape construction."""
import numpy as np

# Configuration
num_stocks = 30
num_indicators = 8
ticker_list = ["STOCK" + str(i) for i in range(num_stocks)]

# Simulate state construction
cash = 1_000_000.0
stocks = np.zeros(num_stocks, dtype=float)
stocks_cd = np.zeros(num_stocks, dtype=float)
price = np.array([100.0] * num_stocks, dtype=np.float32)
tech = np.array([0.5] * (num_stocks * num_indicators), dtype=np.float32)
turbulence = np.array(0.5, dtype=np.float32)
turbulence_bool = 0

# Build state vector (same as paper_trading.py)
amount = np.array(cash * (2**-12), dtype=np.float32)
scale = np.array(2**-6, dtype=np.float32)

state = np.hstack((
    amount,           # 1
    turbulence,       # 1
    turbulence_bool,  # 1
    price * scale,    # num_stocks
    stocks * scale,   # num_stocks
    stocks_cd,        # num_stocks
    tech,             # num_stocks * num_indicators
)).astype(np.float32)

print(f"Configuration:")
print(f"  Stocks: {num_stocks}")
print(f"  Indicators: {num_indicators}")
print(f"\nState vector composition:")
print(f"  amount: 1")
print(f"  turbulence: 1")
print(f"  turbulence_bool: 1")
print(f"  price: {len(price)}")
print(f"  stocks: {len(stocks)}")
print(f"  stocks_cd: {len(stocks_cd)}")
print(f"  tech: {len(tech)}")
print(f"\nTotal shape: {state.shape}")
print(f"Expected: (301,)")
print(f"Difference: {state.shape[0] - 301}")

# Calculate what configuration gives 301
print(f"\n--- What gives 301? ---")
for n_stocks in range(25, 35):
    for n_ind in range(4, 12):
        total = 3 + 3*n_stocks + n_stocks*n_ind
        if total == 301:
            print(f"Found: {n_stocks} stocks, {n_ind} indicators = {total}")

# Try without stocks_cd
print(f"\n--- Without stocks_cd ---")
for n_stocks in range(25, 35):
    for n_ind in range(4, 12):
        total = 3 + 2*n_stocks + n_stocks*n_ind
        if total == 301:
            print(f"Found: {n_stocks} stocks, {n_ind} indicators = {total}")
