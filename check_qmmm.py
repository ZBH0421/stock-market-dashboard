from market_data_db import MarketDataDB
from sqlalchemy import text
import pandas as pd

def check_qmmm():
    db = MarketDataDB()
    with db.engine.connect() as conn:
        print("--- Investigating QMMM ---")
        
        # 1. Get raw prices for context
        print("Recent Prices:")
        query_prices = text("SELECT date, close FROM us_daily_prices WHERE symbol = 'QMMM' ORDER BY date DESC LIMIT 5")
        print(pd.read_sql(query_prices, conn))
        
        print("\nPrices ~1 Year Ago:")
        query_old = text("SELECT date, close FROM us_daily_prices WHERE symbol = 'QMMM' AND date <= (CURRENT_DATE - INTERVAL '360 days') ORDER BY date DESC LIMIT 5")
        print(pd.read_sql(query_old, conn))

        # 2. Run the exact calculation from api.py
        perf_query = text("""
            WITH latest_prices AS (
                SELECT symbol, close as price_now 
                FROM us_daily_prices 
                WHERE symbol = 'QMMM' AND date = (SELECT MAX(date) FROM us_daily_prices WHERE symbol='QMMM')
            ),
            old_prices AS (
                SELECT symbol, close as price_old
                FROM us_daily_prices 
                WHERE symbol = 'QMMM' 
                  AND date >= (CURRENT_DATE - INTERVAL '370 days') 
                  AND date <= (CURRENT_DATE - INTERVAL '360 days')
            )
            SELECT l.symbol, l.price_now, o.price_old, 
                   ((l.price_now - o.price_old) / o.price_old) * 100 as change_pct
            FROM latest_prices l
            LEFT JOIN old_prices o ON l.symbol = o.symbol
        """)
        
        df = pd.read_sql(perf_query, conn)
        print("\n--- Calculated Performance ---")
        pd.options.display.float_format = '{:,.2f}'.format
        print(df)

if __name__ == "__main__":
    check_qmmm()
