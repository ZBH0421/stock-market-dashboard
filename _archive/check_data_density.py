
from market_data_db import MarketDataDB
from sqlalchemy import text
import pandas as pd

def check_density():
    db = MarketDataDB()
    with db.engine.connect() as conn:
        # Check a major stock like DAL
        ticker = 'DAL'
        df = pd.read_sql(text("""
            SELECT date, close 
            FROM us_daily_prices 
            WHERE symbol = :s 
              AND date >= CURRENT_DATE - INTERVAL '1 year'
            ORDER BY date
        """), conn, params={"s": ticker})
        
        if df.empty:
            print("No data found for DAL in the last year.")
            return

        print(f"Data for {ticker} in the last 365 calendar days:")
        print(f"Total rows: {len(df)}")
        print(f"Earliest date: {df['date'].min()}")
        print(f"Latest date: {df['date'].max()}")
        
        # Calculate average days per month in these records
        if len(df) > 0:
            avg = len(df) / 12
            print(f"Average rows per month: {avg:.1f}")

if __name__ == "__main__":
    check_density()
