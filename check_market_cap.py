from market_data_db import MarketDataDB
from sqlalchemy import text
import pandas as pd

def check_market_cap():
    print("Checking Market Cap Data...")
    try:
        db = MarketDataDB()
        with db.engine.connect() as conn:
            query = text("""
                SELECT ticker, market_cap 
                FROM tickers 
                WHERE market_cap IS NOT NULL
                ORDER BY market_cap DESC
            """)
            df = pd.read_sql(query, conn)
            
            # Format market cap for readability
            if not df.empty:
                df['market_cap_formatted'] = df['market_cap'].apply(lambda x: f"${x:,.0f}")
                print("\n[Top Tickers by Market Cap]")
                print(df[['ticker', 'market_cap_formatted']].head(10))
                print(f"\nTotal tickers with Market Cap: {len(df)}")
            else:
                print("No market cap data found.")

    except Exception as e:
        print(f"Error checking market cap: {e}")

if __name__ == "__main__":
    check_market_cap()
