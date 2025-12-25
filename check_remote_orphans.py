from market_data_db import MarketDataDB
from sqlalchemy import create_engine, text
import pandas as pd

# Local Config
OLD_USER = "postgres"                
OLD_PASS = "ad25287706" 
OLD_HOST = "localhost"
OLD_PORT = "5432"
OLD_DB   = "postgres"

def check_remote_orphans():
    print("Checking for Local Price Symbols missing in Cloud Tickers...")
    
    # 1. Get all symbols from Local Prices
    old_url = f"postgresql+psycopg2://{OLD_USER}:{OLD_PASS}@{OLD_HOST}:{OLD_PORT}/{OLD_DB}"
    old_engine = create_engine(old_url)
    
    print("  Fetching Local Price Symbols...")
    with old_engine.connect() as conn:
        # DISTINCT reduces size significantly
        local_symbols = pd.read_sql("SELECT DISTINCT symbol FROM us_daily_prices", conn)
        
    print(f"  Found {len(local_symbols)} unique symbols in Local Prices.")

    # 2. Get all symbols from Cloud Tickers
    new_db = MarketDataDB()
    print("  Fetching Cloud Tickers...")
    with new_db.engine.connect() as conn:
        cloud_tickers = pd.read_sql("SELECT ticker FROM tickers", conn)
        
    print(f"  Found {len(cloud_tickers)} unique tickers in Cloud DB.")
    
    # 3. Compare
    local_set = set(local_symbols['symbol'].str.strip())
    cloud_set = set(cloud_tickers['ticker'].str.strip())
    
    missing = local_set - cloud_set
    
    if missing:
        print(f"\n[CRITICAL] Found {len(missing)} symbols in Local Prices that are MISSING in Cloud Tickers!")
        print(list(missing)[:50]) # Show first 50
    else:
        print("\n[OK] All local price symbols exist in Cloud Tickers.")

if __name__ == "__main__":
    check_remote_orphans()
