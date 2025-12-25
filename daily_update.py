import pandas as pd
from market_data_db import MarketDataDB
from market_data_fetcher import MarketDataFetcher
from sqlalchemy import text
from datetime import datetime, timedelta
import time
from tqdm import tqdm

class DailyUpdater:
    def __init__(self):
        print("Initializing Daily Updater...")
        self.db = MarketDataDB()
        self.fetcher = MarketDataFetcher()
        
    def get_active_tickers(self):
        """Fetches all registered tickers from the database."""
        with self.db.engine.connect() as conn:
            # Get list of tickers
            tickers = conn.execute(text("SELECT ticker FROM tickers ORDER BY ticker")).fetchall()
            return [t[0] for t in tickers]

    def get_last_update_date(self):
        """
        Gets the maximum date currently in the database to determine start date.
        Returns a string 'YYYY-MM-DD'.
        """
        with self.db.engine.connect() as conn:
            # Find the latest date across all prices
            # (In a perfect world we check per ticker, but for daily batch, global max or T-3 is usually safe)
            result = conn.execute(text("SELECT MAX(date) FROM us_daily_prices")).scalar()
            
            if result:
                # Start from the NEXT day
                next_day = result + timedelta(days=1)
                return next_day.strftime('%Y-%m-%d')
            else:
                # If DB is empty, default safely
                return "2024-01-01"

    def run(self):
        print("--- Starting Daily Stock Update ---")
        
        # 1. Get Tickers
        tickers = self.get_active_tickers()
        print(f"Found {len(tickers)} active tickers in database.")
        
        if not tickers:
            print("No tickers found. Nothing to update.")
            return

        # 2. Determine Date Range
        # Ideally we want to fill gaps. 
        # Strategy: Fetch from (Today - 3 days) to Today. 
        # The DB 'upsert' logic handles duplicates, so slight overlap is cleaner than missing data.
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
        
        print(f"Update Range: {start_date} -> {end_date}")
        print("(Fetching last 5 days to ensure no gaps/corrections are missed)")
        
        # 3. Batch Process
        success_count = 0
        skip_count = 0
        error_count = 0
        
        pbar = tqdm(tickers, desc="Updating Prices")
        
        for ticker in pbar:
            try:
                # Fetch
                df = self.fetcher.fetch_us_daily_close(ticker, start_date, end_date)
                
                if df is None or df.empty:
                    skip_count += 1
                    continue
                
                # Transform
                df['symbol'] = ticker
                
                # Save (Upsert)
                self.db.save_daily_data(df)
                success_count += 1
                
                # Rate limit (be nice to yfinance)
                time.sleep(0.2)
                
            except Exception as e:
                error_count += 1
                # print(f"Failed {ticker}: {e}") # Optional: verify verbose
                
        print("\n--- Update Completed ---")
        print(f"Processed: {len(tickers)}")
        print(f"Success:   {success_count}")
        print(f"Skipped:   {skip_count} (No new data)")
        print(f"Errors:    {error_count}")

if __name__ == "__main__":
    updater = DailyUpdater()
    updater.run()
