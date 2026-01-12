from market_data_db import MarketDataDB
from sqlalchemy import text
import pandas as pd

def check_ticker_quality():
    db = MarketDataDB()
    with db.engine.connect() as conn:
        print("--- Ticker Quality Check ---")
        
        # Targets: Known corrupted industry names from previous listing
        # 'Semiondutors' (missing c, c, r?) -> Check widely known tickers like NVDA, AMD
        # 'Softwre - Infrstruture' (missing a, a) -> Check MSFT?
        
        corrupted_targets = [
            ('Semiondutors', ['NVDA', 'AMD', 'INTC', 'TSM', 'AVGO']), 
            ('Softwre - Infrstruture', ['MSFT', 'ORCL', 'ADBE']),
            ('Biotehnology', ['AMGN', 'GILD', 'VRTX'])
        ]
        
        for ind_name, expected_tickers in corrupted_targets:
            print(f"\n[Checking: {ind_name}]")
            
            # 1. basic stats
            ind_query = text("SELECT id FROM industries WHERE name = :n")
            ind_id = conn.execute(ind_query, {"n": ind_name}).scalar()
            
            if not ind_id:
                print(f"WARN: Industry name '{ind_name}' not found exactly in DB. Trying LIKE match...")
                # Try simple match
                short = ind_name[:5]
                like_query = text(f"SELECT id, name FROM industries WHERE name LIKE '{short}%'")
                res = conn.execute(like_query).fetchone()
                if res:
                    ind_id, real_name = res
                    print(f"   -> Match found: '{real_name}'")
                else:
                    print(f"   -> No match found. Skip.")
                    continue
            
            # 2. Get actual tickers in DB
            t_query = text("SELECT ticker FROM tickers WHERE industry_id = :iid")
            db_tickers = [t[0] for t in conn.execute(t_query, {"iid": ind_id}).fetchall()]
            
            print(f"   -> Total Tickers found: {len(db_tickers)}")
            print(f"   -> Sample of first 10: {db_tickers[:10]}")
            
            # 3. Verify Expected Tickers
            found_count = 0
            for exp in expected_tickers:
                if exp in db_tickers:
                    found_count += 1
                else:
                    print(f"   WARN: Expected ticker '{exp}' NOT found in this industry group.")
            
            
            print(f"   -> Expected Tickers Verification: {found_count}/{len(expected_tickers)} found.")
            
            # 4. Price Check Coverage
            if db_tickers:
                price_check_q = text("""
                    SELECT COUNT(DISTINCT symbol) 
                    FROM us_daily_prices 
                    WHERE symbol IN :tickers
                """)
                # Split into chunks if too many, but here we just want a quick count
                # Postgres handles IN clause reasonable well for a few hundred
                # Let's just check sample
                sample_t = tuple(db_tickers[:50])
                covered_count = conn.execute(text(f"SELECT COUNT(DISTINCT symbol) FROM us_daily_prices WHERE symbol IN {sample_t}")).scalar()
                print(f"   -> Price Availability (Sample 50): {covered_count}/50 have data.")

if __name__ == "__main__":
    check_ticker_quality()
