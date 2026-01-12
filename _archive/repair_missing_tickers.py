from market_data_db import MarketDataDB
from market_data_fetcher import MarketDataFetcher
from sqlalchemy import text
import pandas as pd
import time
from datetime import datetime

def repair_missing_data():
    print("Starting Repair Job for Missing Tickers...")
    db = MarketDataDB()
    fetcher = MarketDataFetcher()
    
    start_date = "2024-01-01"
    end_date = datetime.now().strftime("%Y-%m-%d")

    with db.engine.connect() as conn:
        # 1. Identify missing tickers (same query as check script)
        missing_query = text("""
            SELECT t.ticker, t.industry_id
            FROM tickers t
            LEFT JOIN us_daily_prices p ON t.ticker = p.symbol
            WHERE p.symbol IS NULL
        """)
        df_missing = pd.read_sql(missing_query, conn)
    
    if df_missing.empty:
        print("No missing tickers found! Use check_data_completeness.py to verify.")
        return

    print(f"Found {len(df_missing)} tickers to repair.")
    print(df_missing['ticker'].tolist())
    
    success_count = 0
    
    for i, row in df_missing.iterrows():
        ticker = row['ticker']
        ind_id = row['industry_id']
        print(f"\n[{i+1}/{len(df_missing)}] Repairing: {ticker}")
        
        try:
            # A. Fetch Prices
            df_prices = fetcher.fetch_us_daily_close(ticker, start_date, end_date)
            
            if df_prices is not None and not df_prices.empty:
                # Store Prices
                df_prices['symbol'] = ticker # DB expects 'symbol' column
                db.save_daily_data(df_prices)
                
                # B. Fetch Info (Market Cap etc) since we're here
                info = fetcher.get_ticker_info(ticker)
                if info:
                   db.register_ticker(ticker, ind_id, info) # Fixed Order: ticker str first
                
                print(f"   -> Success! ({len(df_prices)} records)")
                success_count += 1
            else:
                print(f"   -> Failed: Still no data from source.")
                
            time.sleep(1) # Be gentle with API
            
        except Exception as e:
            print(f"   -> Error: {e}")

    print(f"\nRepair Job Complete. Validated {success_count}/{len(df_missing)} tickers.")

if __name__ == "__main__":
    repair_missing_data()
