from market_data_db import MarketDataDB
from sqlalchemy import text
import pandas as pd

def check_fundamentals():
    print("Checking Financial Fundamentals...")
    try:
        db = MarketDataDB()
        with db.engine.connect() as conn:
            query = text("""
                SELECT 
                    ticker,
                    revenue / 1000000000.0 as revenue_B,
                    pe_ratio,
                    profit_margin * 100 as margin_pct,
                    dividend_yield as div_yield_pct
                FROM tickers 
                WHERE revenue IS NOT NULL
                ORDER BY revenue DESC
            """)
            df = pd.read_sql(query, conn)
            
            if not df.empty:
                # Format
                pd.options.display.float_format = '{:,.2f}'.format
                print("\n[Fundamentals (Top by Revenue)]")
                print(df.head(10).to_string(index=False))
            else:
                print("No fundamental data found.")

    except Exception as e:
        print(f"Error checking fundamentals: {e}")

if __name__ == "__main__":
    check_fundamentals()
