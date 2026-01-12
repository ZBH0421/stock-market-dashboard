import yfinance as yf
import pandas as pd

try:
    ticker = yf.Ticker("AAPL")
    # Get expiration dates
    exps = ticker.options
    if not exps:
        print("No options data found.")
    else:
        print(f"Expirations: {exps[:3]}")
        # Get chain for first expiration
        opt = ticker.option_chain(exps[0])
        print("\nCalls Columns:")
        print(opt.calls.columns)
        print("\nFirst Call Row:")
        print(opt.calls.head(1).T)
        
        print("\nPuts Columns:")
        print(opt.puts.columns)
except Exception as e:
    print(f"Error: {e}")
