from market_data_db import MarketDataDB
from sqlalchemy import text
import pandas as pd

def check_autoparts():
    db = MarketDataDB()
    with db.engine.connect() as conn:
        print("--- Checking 'Auto Parts' ---")
        
        # 1. Broad Search
        res = conn.execute(text("SELECT id, name FROM industries WHERE name LIKE '%Auto%'")).fetchall()
        print(f"Found Auto Industries: {res}")
        
        target_id = None
        target_name = None
        
        for r in res:
            if "Prt" in r[1] or "Part" in r[1]:
                target_id = r[0]
                target_name = r[1]
                break
        
        if not target_id:
            print("❌ No 'Auto Parts' equivalent found!")
            return

        print(f"\nTarget Identified: '{target_name}' (ID: {target_id})")
        
        # 2. Check Tickers
        tickers = [t[0] for t in conn.execute(text("SELECT ticker FROM tickers WHERE industry_id = :i"), {"i": target_id}).fetchall()]
        print(f"Tickers Found: {len(tickers)}")
        print(f"Sample: {tickers[:5]}")
        
        # 3. Check Data Quality for first ticker
        if tickers:
            t = tickers[0]
            count = conn.execute(text("SELECT COUNT(*) FROM us_daily_prices WHERE symbol = :s"), {"s": t}).scalar()
            print(f"Price Data for {t}: {count} rows")
            
            # API Simulation
            print(f"\n--- API Simulation for '{target_name}' ---")
            from urllib.parse import quote
            safe_url = quote(target_name)
            print(f"URL Encoding Test: {safe_url}")

if __name__ == "__main__":
    check_autoparts()
