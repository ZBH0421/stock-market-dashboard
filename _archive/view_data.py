from market_data_db import MarketDataDB
import pandas as pd
from sqlalchemy import text

def view_data():
    try:
        db = MarketDataDB()
        
        print("\n=== 1. Industries Overview ===")
        with db.engine.connect() as conn:
            query = text("""
                SELECT i.name, COUNT(t.ticker) as tickers 
                FROM industries i 
                LEFT JOIN tickers t ON i.id = t.industry_id 
                GROUP BY i.name
            """)
            df = pd.read_sql(query, conn)
            print(df)
            
        print("\n=== 2. Airlines Tickers Data Check ===")
        with db.engine.connect() as conn:
            query = text("""
                SELECT t.ticker, COUNT(p.date) as records, MAX(p.date) as latest_date
                FROM tickers t
                JOIN industries i ON t.industry_id = i.id
                JOIN us_daily_prices p ON t.ticker = p.symbol
                WHERE i.name = 'Airlines'
                GROUP BY t.ticker
                ORDER BY records DESC
                LIMIT 5
            """)
            df = pd.read_sql(query, conn)
            print(df)
            
    except Exception as e:
        print(f"Error viewing data: {e}")

if __name__ == "__main__":
    view_data()
