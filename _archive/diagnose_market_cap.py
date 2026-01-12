from market_data_db import MarketDataDB
from sqlalchemy import text
import pandas as pd

def diagnose():
    db = MarketDataDB()
    with db.engine.connect() as conn:
        print("--- Diagnosing Market Cap Data ---")
        
        # 1. Check Tickers Table (Used by Galaxy API)
        t_count = conn.execute(text("SELECT COUNT(*) FROM tickers WHERE market_cap IS NOT NULL AND market_cap > 0")).scalar()
        t_total = conn.execute(text("SELECT COUNT(*) FROM tickers")).scalar()
        print(f"Tickers Table: {t_count}/{t_total} tickers have Market Cap.")
        
        # 2. Check Daily Prices Table (Where backfill was writing)
        p_count = conn.execute(text("SELECT COUNT(*) FROM us_daily_prices WHERE market_cap IS NOT NULL AND market_cap > 0")).scalar()
        print(f"Daily Prices Table: {p_count} rows have Market Cap.")
        
        # 3. Check if we can sync
        if t_count == 0 and p_count > 0:
            print("\n[ROOT CAUSE FOUND] Data is in 'us_daily_prices' but not synced to 'tickers' table.")
            
if __name__ == "__main__":
    diagnose()
