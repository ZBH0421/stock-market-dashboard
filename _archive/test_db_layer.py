from market_data_db import MarketDataDB
import pandas as pd
from datetime import datetime

# Warning: This test requires a valid .env file connecting to a running Postgres instance

def test_integration():
    print("Initializing DB...")
    try:
        db = MarketDataDB()
    except ValueError as e:
        print(f"Aborting test: {e}")
        print("Please create a .env file based on .env.example")
        return

    # Simulate Data from Fetcher
    print("Simulating Fetcher Data...")
    data = {
        'close': [100.50, 101.00, 102.50],
        'adj_close': [100.00, 100.50, 102.00],
        'volume': [500000, 600000, 550000]
    }
    dates = pd.to_datetime(['2025-01-01', '2025-01-02', '2025-01-03'])
    df = pd.DataFrame(data, index=dates)
    df.index.name = 'market_date'
    
    # We need to inject the symbol as the Fetcher usually returns data for one symbol
    # The Controller would typically add this column or we pass it
    # In this specific DB design, save_daily_data expects 'symbol' column in DF
    symbol = "TEST_INTEGRATION"
    df['symbol'] = symbol
    
    # Save
    print(f"Upserting data for {symbol}...")
    db.save_daily_data(df)
    
    # Retrieve
    print("Reading back data...")
    df_read = db.get_data(symbol)
    print(df_read)
    
    assert len(df_read) == 3
    print("[SUCCESS] Integration Test Passed")

if __name__ == "__main__":
    test_integration()
