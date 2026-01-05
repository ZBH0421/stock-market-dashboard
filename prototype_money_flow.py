from market_data_db import MarketDataDB
from sqlalchemy import text
import pandas as pd

def check_industry_money_flow():
    """
    Approximates 'Money Flow' into industries.
    Logic: Net Dollar Flow = Sum(Volume * (Close - PrevClose))
    """
    db = MarketDataDB()
    with db.engine.connect() as conn:
        print("Calculating Net Money Flow (Last 1 Day)...")
        
        # 1. Get Tickers and Industries
        tickers_df = pd.read_sql(text("SELECT t.ticker, i.name as industry FROM tickers t JOIN industries i ON t.industry_id = i.id"), conn)
        
        # 2. Get Recent Prices (Last 2 days to calc change)
        # Note: In a real app we'd do this in SQL window functions, but for prototyping pandas is easier debug
        query = text("""
            SELECT symbol, date, close, volume 
            FROM us_daily_prices 
            WHERE date >= CURRENT_DATE - INTERVAL '5 days'
            ORDER BY symbol, date
        """)
        prices_df = pd.read_sql(query, conn)
        
        if prices_df.empty:
            print("Not enough price data to calculate flow.")
            return

        # 3. Calculate Daily Change & Money Flow
        prices_df['prev_close'] = prices_df.groupby('symbol')['close'].shift(1)
        prices_df['price_change'] = prices_df['close'] - prices_df['prev_close']
        
        # Net Flow = Volume * Price Change (Approximation: Money moving in direction of price)
        # Or simpler: (Close * Volume) if Green, -(Close * Volume) if Red. 
        # Let's use: Signed Dollar Volume = Volume * Price_Change
        prices_df['net_flow'] = prices_df['volume'] * prices_df['price_change']
        
        # Filter for latest date only
        latest_date = prices_df['date'].max()
        latest_df = prices_df[prices_df['date'] == latest_date].copy()
        
        # 4. Merge Industry
        merged = latest_df.merge(tickers_df, left_on='symbol', right_on='ticker')
        
        # 5. Group by Industry
        industry_flow = merged.groupby('industry')[['net_flow']].sum().sort_values('net_flow', ascending=False)
        
        print(f"\n--- Estimated Capital Flow ({latest_date}) ---")
        print("Format: Industry | Net Flow (USD)")
        
        pd.options.display.float_format = '{:,.0f}'.format
        print(industry_flow.head(10))
        print("\n--- Bottom 5 (Outflow) ---")
        print(industry_flow.tail(5))

if __name__ == "__main__":
    check_industry_money_flow()
