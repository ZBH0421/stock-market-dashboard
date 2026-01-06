from market_data_db import MarketDataDB
from sqlalchemy import text
import pandas as pd

def audit_revenue():
    db = MarketDataDB()
    with db.engine.connect() as conn:
        print("--- REVENUE DATA AUDIT ---")
        
        # 1. Fetch all revenue data
        query = text("""
            SELECT ticker, company_name, revenue, market_cap 
            FROM tickers 
            WHERE revenue IS NOT NULL
            ORDER BY revenue DESC
        """)
        df = pd.read_sql(query, conn)
        
        # 2. Top 15 (Check for "Trillionaire" errors or unit mismatches)
        print(f"\n[TOP 15 HIGHEST REVENUE]")
        pd.options.display.float_format = '{:,.0f}'.format
        print(df.head(15)[['ticker', 'revenue', 'company_name']])
        
        # 3. Negative Revenue (Should be rare/impossible for most)
        negatives = df[df['revenue'] < 0]
        if not negatives.empty:
            print(f"\n[WARNING: NEGATIVE REVENUE] Found {len(negatives)} tickers:")
            print(negatives[['ticker', 'revenue', 'company_name']])
            
        # 4. Zero Revenue (Shell companies or data missing)
        zeros = df[df['revenue'] == 0]
        print(f"\n[INFO] Tickers with Zero Revenue: {len(zeros)}")
        if not zeros.empty:
            print(zeros.head(5)[['ticker', 'company_name']])

        # 5. Low Revenue (micro caps)
        print(f"\n[BOTTOM 10 POSITIVE REVENUE]")
        positive_low = df[df['revenue'] > 0].tail(10)
        print(positive_low[['ticker', 'revenue', 'company_name']])

if __name__ == "__main__":
    audit_revenue()
