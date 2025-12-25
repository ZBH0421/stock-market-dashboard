from market_data_db import MarketDataDB
from sqlalchemy import text
import pandas as pd

def check_completeness():
    print("Checking Data Completeness...")
    db = MarketDataDB()
    
    with db.engine.connect() as conn:
        # 1. Total Tickers
        total_tickers = conn.execute(text("SELECT COUNT(*) FROM tickers")).scalar()
        print(f"Total Tickers Registered: {total_tickers}")

        # 2. Tickers with at least one price record
        tickers_with_data = conn.execute(text("SELECT COUNT(DISTINCT symbol) FROM us_daily_prices")).scalar()
        print(f"Tickers with Price Data:  {tickers_with_data}")

        # 3. Find missing tickers
        missing_query = text("""
            SELECT t.ticker, i.name as industry
            FROM tickers t
            LEFT JOIN us_daily_prices p ON t.ticker = p.symbol
            LEFT JOIN industries i ON t.industry_id = i.id
            WHERE p.symbol IS NULL
        """)
        missing_df = pd.read_sql(missing_query, conn)
        
        if missing_df.empty:
            print("\n[SUCCESS] ALL tickers have data! (100% Coverage)")
        else:
            print(f"\n[WARNING] Found {len(missing_df)} tickers with NO price data.")
            print("Top missing industries:")
            print(missing_df['industry'].value_counts().head(10))
            print("\nSample missing tickers:")
            print(missing_df.head(10))

if __name__ == "__main__":
    check_completeness()
