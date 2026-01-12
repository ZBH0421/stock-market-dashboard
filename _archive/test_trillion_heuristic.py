from market_data_db import MarketDataDB
from sqlalchemy import text
import pandas as pd

def check_trillionaires():
    db = MarketDataDB()
    with db.engine.connect() as conn:
        print("--- Checking for 'Trillionaire' Revenue Anomalies ---")
        
        # Threshold: 800 Billion (Walmart is ~648B, Amazon ~574B)
        # Setting low enough to catch BABA (1T CNY) but high enough to clear WMT
        threshold = 900_000_000_000
        
        query = text(f"""
            SELECT ticker, company_name, revenue, market_cap 
            FROM tickers 
            WHERE revenue > {threshold}
            ORDER BY revenue DESC
        """)
        
        df = pd.read_sql(query, conn)
        
        print(f"Found {len(df)} tickers with > 900B Revenue:")
        pd.options.display.float_format = '{:,.0f}'.format
        print(df)
        
        # Check against known US giants to ensure we don't kill WMT/AMZN
        us_giants = ['WMT', 'AMZN', 'AAPL']
        print("\n--- Safety Check (US Giants) ---")
        for t in us_giants:
            row = df[df['ticker'] == t]
            if not row.empty:
                print(f"WARNING: {t} is in the kill list! Rev: {row.iloc[0]['revenue']:,.0f}")
            else:
                print(f"SAFE: {t} is below threshold.")

if __name__ == "__main__":
    check_trillionaires()
