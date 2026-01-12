from market_data_fetcher import MarketDataFetcher
from market_data_db import MarketDataDB
from datetime import datetime, timedelta
import pandas as pd

def test_e2e_aapl():
    print("--- Starting End-to-End Test (Fetcher -> DB) ---")
    
    # 1. Initialize Components
    try:
        db = MarketDataDB()
        fetcher = MarketDataFetcher()
    except Exception as e:
        print(f"[FATAL] Initialization failed: {e}")
        return

    # 2. Fetch Data (AAPL, Last 30 Days)
    symbol = "AAPL"
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    print(f"\n[1] Fetching {symbol} data ({start_str} to {end_str})...")
    df = fetcher.fetch_us_daily_close(symbol, start_str, end_str)
    
    if df is None or df.empty:
        print("[ERROR] Fetch failed, aborting.")
        return
        
    # Inject 'symbol' column (Required for DB Schema)
    df['symbol'] = symbol
    print(f"    Fetched {len(df)} rows.")

    # 3. Save to Database
    print(f"\n[2] Saving to Database...")
    try:
        db.save_daily_data(df)
    except Exception as e:
        print(f"[ERROR] Save failed: {e}")
        return

    # 4. Verify (Read Back)
    print(f"\n[3] Verifying Data in DB...")
    saved_df = db.get_data(symbol, start_str, end_str)
    
    if saved_df.empty:
        print("[FAIL] Database returned empty result!")
    else:
        print(f"    Successfully retrieved {len(saved_df)} rows from DB.")
        print("\n[Preview from DB]")
        print(saved_df.head())
        print(f"\n[SUCCESS] E2E Test Passed for {symbol}")

if __name__ == "__main__":
    test_e2e_aapl()
