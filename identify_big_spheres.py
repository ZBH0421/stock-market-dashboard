from market_data_db import MarketDataDB
from sqlalchemy import text
import pandas as pd

def find_super_gainers():
    db = MarketDataDB()
    with db.engine.connect() as conn:
        print("--- Hunting for Big Spheres (Top 1Y Performers) ---")
        
        # Logic matches api.py
        query = text("""
            WITH latest_prices AS (
                SELECT symbol, close as price_now 
                FROM us_daily_prices 
                WHERE date = (SELECT MAX(date) FROM us_daily_prices)
            ),
            old_prices AS (
                SELECT symbol, close as price_old
                FROM us_daily_prices 
                WHERE date >= (CURRENT_DATE - INTERVAL '370 days') 
                  AND date <= (CURRENT_DATE - INTERVAL '360 days')
            )
            SELECT l.symbol, l.price_now, o.price_old, 
                   ((l.price_now - o.price_old) / o.price_old) * 100 as change_pct
            FROM latest_prices l
            JOIN old_prices o ON l.symbol = o.symbol
            ORDER BY change_pct DESC
            LIMIT 10
        """)
        
        df = pd.read_sql(query, conn)
        
        pd.options.display.float_format = '{:,.2f}'.format
        print(df)
        
        if not df.empty:
            top_stock = df.iloc[0]
            print(f"\nThe 'Big Sphere' is likely: {top_stock['symbol']} (+{top_stock['change_pct']:,.0f}%)")

if __name__ == "__main__":
    find_super_gainers()
