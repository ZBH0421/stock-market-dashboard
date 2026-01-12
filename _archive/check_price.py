from market_data_db import MarketDataDB
from sqlalchemy import text
import pandas as pd

def check_price(ticker, target_date):
    print(f"Checking price for {ticker} on {target_date}...")
    try:
        db = MarketDataDB()
        with db.engine.connect() as conn:
            query = text("""
                SELECT symbol, date, close, adj_close, volume
                FROM us_daily_prices
                WHERE symbol = :ticker AND date = :date
            """)
            result = conn.execute(query, {"ticker": ticker, "date": target_date}).fetchone()
            
            if result:
                print("\n[Found Data]")
                print(f"Date: {result.date}")
                print(f"Ticker: {result.symbol}")
                print(f"Close: {result.close}")
                print(f"Adj Close: {result.adj_close}")
                print(f"Volume: {result.volume}")
            else:
                print(f"\n[No Data] Could not find data for {ticker} on {target_date}.")
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_price("DAL", "2025-11-26")
