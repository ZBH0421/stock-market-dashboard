import pandas as pd
import yfinance as yf
from market_data_db import MarketDataDB
from sqlalchemy import text, bindparam
from tqdm import tqdm
import time

class MarketCapBackfiller:
    def __init__(self):
        self.db = MarketDataDB()
        
    def get_tickers(self):
        with self.db.engine.connect() as conn:
            # Only get tickers present in the prices table to save time
            query = text("SELECT DISTINCT symbol FROM us_daily_prices")
            res = conn.execute(query).fetchall()
            return [r[0] for r in res]

    def backfill_ticker(self, ticker):
        # 1. Fetch Shares History
        try:
            yf_ticker = yf.Ticker(ticker.replace('.', '-'))
            shares = yf_ticker.get_shares_full(start="2020-01-01")
            
            if shares is None or shares.empty:
                print(f"[Warn] No shares data for {ticker}")
                return False
                
            # Convert shares series to DataFrame
            shares_df = pd.DataFrame({'shares': shares})
            shares_df.index = pd.to_datetime(shares_df.index).tz_localize(None)
            shares_df = shares_df.sort_index()
            # Deduplicate by 'last'
            shares_df = shares_df.groupby(shares_df.index).last()
            
        except Exception as e:
            print(f"[Error] Failed to fetch shares for {ticker}: {e}")
            return False

        # 2. Fetch Daily Prices from DB
        with self.db.engine.connect() as conn:
            prices_df = pd.read_sql(
                text("SELECT date, close FROM us_daily_prices WHERE symbol = :sym ORDER BY date ASC"),
                conn,
                params={"sym": ticker}
            )
            
        if prices_df.empty:
            return False
            
        prices_df['date'] = pd.to_datetime(prices_df['date'])
        prices_df = prices_df.set_index('date')
        
        # 3. Merge Strategies
        # We need to forward fill shares to match price dates
        
        # Reindex shares to cover the full price range + forward fill
        # Start from the first share date or first price date, whichever is earlier
        full_idx = prices_df.index.union(shares_df.index).sort_values()
        
        combined = pd.DataFrame(index=full_idx)
        combined['shares'] = shares_df['shares']
        combined = combined.ffill() # Forward fill share counts
        
        # Align with price dates (only keep dates we have prices for)
        merged = prices_df.join(combined['shares'], how='inner')
        
        # 4. Calculate Market Cap
        merged['market_cap'] = merged['close'] * merged['shares']
        merged = merged.dropna(subset=['market_cap'])
        
        if merged.empty:
            return False

        # 5. Bulk Update DB
        # We use a temporary table or direct loop update. 
        # Since standard SQL update is slow for many rows, we can use SQLAlchemy params.
        # But `us_daily_prices` might be large. For a single ticker (couple hundred rows), 
        # batch update is fine.
        
        # Prepare data for bulk update
        update_data = []
        for date, row in merged.iterrows():
            update_data.append({
                "p_symbol": ticker,
                "p_date": date.strftime('%Y-%m-%d'),
                "p_cap": int(row['market_cap'])
            })

        # Update Query
        stmt = text("""
            UPDATE us_daily_prices 
            SET market_cap = :p_cap 
            WHERE symbol = :p_symbol AND date = :p_date
        """)

        with self.db.engine.begin() as conn:
             conn.execute(stmt, update_data)
             
        return True

    def run(self):
        print("--- Starting Market Cap Backfill ---")
        tickers = self.get_tickers()
        print(f"Found {len(tickers)} tickers with price data.")
        
        success = 0
        
        pbar = tqdm(tickers)
        for ticker in pbar:
            pbar.set_description(f"Processing {ticker}")
            if self.backfill_ticker(ticker):
                success += 1
            time.sleep(0.1) # Rate limit
            
        print(f"\nBackfill Complete. Success: {success}/{len(tickers)}")

if __name__ == "__main__":
    bf = MarketCapBackfiller()
    bf.run()
