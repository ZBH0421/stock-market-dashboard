from market_data_db import MarketDataDB
from sqlalchemy import text
import pandas as pd

def diagnose_api_logic():
    db = MarketDataDB()
    industries_to_test = ["Airlines", "Semiconductors", "Software", "Biotehnology"]
    
    with db.engine.connect() as conn:
        print(f"{'Industry':<20} | {'Found?':<6} | {'Tickers':<8} | {'Prices':<8} | {'Status'}")
        print("-" * 60)
        
        for name in industries_to_test:
            # 1. Check Industry Table
            ind_res = conn.execute(text("SELECT id, name FROM industries WHERE LOWER(name) = LOWER(:n)"), {"n": name}).fetchone()
            if not ind_res:
                print(f"{name:<20} | {'No':<6} | {'-':<8} | {'-':<8} | Industry not in DB")
                continue
            
            ind_id, db_name = ind_res
            
            # 2. Check Tickers Table
            t_count = conn.execute(text("SELECT COUNT(*) FROM tickers WHERE industry_id = :iid"), {"iid": ind_id}).scalar()
            
            # 3. Check Prices
            p_count = conn.execute(text("""
                SELECT COUNT(*) FROM us_daily_prices 
                WHERE symbol IN (SELECT ticker FROM tickers WHERE industry_id = :iid)
            """), {"iid": ind_id}).scalar()
            
            status = "OK" if p_count > 0 else "MISSING PRICES"
            if t_count == 0: status = "NO TICKERS"
            
            print(f"{db_name:<20} | {'Yes':<6} | {t_count:<8} | {p_count:<8} | {status}")

        print("\n--- Top 10 Industries in DB by Linked Price Data ---")
        top_query = text("""
            SELECT i.name, COUNT(t.ticker) as t_count, (SELECT COUNT(p.symbol) FROM us_daily_prices p WHERE p.symbol IN (SELECT ticker FROM tickers WHERE industry_id = i.id)) as p_count
            FROM industries i
            JOIN tickers t ON i.id = t.industry_id
            GROUP BY i.name, i.id
            ORDER BY p_count DESC
            LIMIT 10
        """)
        top_df = pd.read_sql(top_query, conn)
        print(top_df)

if __name__ == "__main__":
    diagnose_api_logic()
