from market_data_db import MarketDataDB
from sqlalchemy import text
import pandas as pd

def check_adrs():
    db = MarketDataDB()
    with db.engine.connect() as conn:
        print("--- Checking ADR Data (Currency Issue) ---")
        
        # Suspected ADRs + Reference US Stock
        tickers = ['TM', 'SONY', 'HMC', 'TSM', 'BABA', 'F'] # F (Ford) as USD baseline
        
        query = text(f"""
            SELECT ticker, company_name, revenue, market_cap 
            FROM tickers 
            WHERE ticker IN :tickers
        """)
        
        df = pd.read_sql(query, conn, params={"tickers": tuple(tickers)})
        
        # Display nicely
        pd.options.display.float_format = '{:,.0f}'.format
        print(df)
        
        print("\n--- Analysis ---")
        # Toyota 2023 Revenue approx 40 Trillion JPY ($270B USD)
        # Sony 2023 Revenue approx 11 Trillion JPY ($75B USD)
        # If we see Trillions here, it's JPY.
        
        tm_row = df[df['ticker'] == 'TM']
        if not tm_row.empty:
            rev = tm_row.iloc[0]['revenue']
            print(f"Toyota Revenue in DB: {rev:,.0f}")
            if rev > 1_000_000_000_000: # > 1 Trillion
                print(" -> DEFINITELY LOCAL CURRENCY (JPY)")
            else:
                print(" -> Seems like USD (or data is missing)")

if __name__ == "__main__":
    check_adrs()
