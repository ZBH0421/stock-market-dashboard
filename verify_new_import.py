from market_data_db import MarketDataDB
from sqlalchemy import text
import pandas as pd

def verify_new_data():
    db = MarketDataDB()
    print("--- Verifying Newly Added Industries (S-Z) ---")
    
    target_industries = [
        'Semiconductors', 
        'Software - Infrastructure', 
        'Software - Application',
        'Solar',
        'Utilities - Regulated Electric'
    ]
    
    with db.engine.connect() as conn:
        for ind_name in target_industries:
            print(f"\nChecking: {ind_name}")
            # 1. Check Industry existence
            res = conn.execute(text("SELECT id, name FROM industries WHERE name = :name"), {"name": ind_name}).fetchone()
            if not res:
                print(f"  [MISSING] in DB")
                continue
            
            ind_id = res[0]
            print(f"  [OK] Found ID: {ind_id}")
            
            # 2. Check Ticker Count
            tick_count = conn.execute(text("SELECT count(*) FROM tickers WHERE industry_id = :id"), {"id": ind_id}).scalar()
            print(f"  Tickers Linked: {tick_count}")
            
            if tick_count == 0:
                print("  [WARN] No tickers found!")
                continue
                
            # 3. Check Price Data Density (Sample)
            # Check price count for a few tickers
            sample_tickers = conn.execute(text("SELECT ticker FROM tickers WHERE industry_id = :id LIMIT 3"), {"id": ind_id}).fetchall()
            for t in sample_tickers:
                sym = t[0]
                price_count = conn.execute(text("SELECT count(*) FROM us_daily_prices WHERE symbol = :sym"), {"sym": sym}).scalar()
                status = "[OK]" if price_count > 10 else "[EMPTY]"
                print(f"    - {sym}: {price_count} prices {status}")

if __name__ == "__main__":
    verify_new_data()
