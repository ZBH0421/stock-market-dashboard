import pandas as pd
from market_data_db import MarketDataDB
from market_data_fetcher import MarketDataFetcher
from sqlalchemy import text
from datetime import datetime, timedelta
import time
from tqdm import tqdm
import subprocess
import sys
from pathlib import Path
from generate_rotation_history import ALL_SECTORS as _ALL_SECTORS

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
        Gets the start date for the next update run.
        Uses the date where at least 90% of tickers have coverage,
        avoiding a partial run pushing MAX() ahead and skipping the rest.
        """
        with self.db.engine.connect() as conn:
            total_tickers = conn.execute(text("SELECT COUNT(*) FROM tickers")).scalar() or 1
            threshold = int(total_tickers * 0.9)
            # Find the most recent date that has data for at least 90% of tickers
            result = conn.execute(text("""
                SELECT MAX(date) FROM (
                    SELECT date FROM us_daily_prices
                    GROUP BY date
                    HAVING COUNT(DISTINCT symbol) >= :threshold
                ) covered_dates
            """), {'threshold': threshold}).scalar()

            if result:
                next_day = result + timedelta(days=1)
                return next_day.strftime('%Y-%m-%d')
            else:
                return "2024-01-01"

    def _warmup_industry_cache(self):
        """Pre-generate industry rotation history for all 11 sectors."""
        script = Path(__file__).parent / "generate_industry_rotation_history.py"
        print("\n--- Warming up industry rotation cache ---")
        for sector in _ALL_SECTORS:
            print(f"  Generating {sector}…")
            result = subprocess.run(
                [sys.executable, str(script), "--sector", sector, "--force"],
                check=False,
            )
            if result.returncode != 0:
                print(f"  WARNING: warm-up failed for {sector} (exit {result.returncode})")
        print("--- Warm-up complete ---")

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
        # yfinance end is exclusive, so use tomorrow to include today's closing prices
        end_date = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = self.get_last_update_date()
        
        print(f"Update Range: {start_date} -> {end_date}")
        print("(Fetching last 5 days to ensure no gaps/corrections are missed)")
        
        # 3. Batch Process
        success_count = 0
        skip_count = 0
        error_count = 0
        delisted_count = 0
        
        pbar = tqdm(tickers, desc="Updating Prices")
        
        for ticker in pbar:
            try:
                # Fetch
                df = self.fetcher.fetch_us_daily_close(ticker, start_date, end_date)
                
                if df is None or df.empty:
                    if self.fetcher.is_delisted(ticker):
                        with self.db.engine.begin() as conn:
                            conn.execute(text("DELETE FROM us_daily_prices WHERE symbol = :t"), {'t': ticker})
                            conn.execute(text("DELETE FROM tickers WHERE ticker = :t"), {'t': ticker})
                        print(f"[Delisted] Removed {ticker} from DB.")
                        delisted_count += 1
                    else:
                        skip_count += 1
                    continue
                
                # Transform
                df['symbol'] = ticker
                
                # Save (Upsert)
                self.db.save_daily_data(df)
                success_count += 1
                
                # --- NEW: Update Fundamentals (Market Cap, PE, etc.) ---
                try:
                    info = self.fetcher.get_ticker_info(ticker)
                    if info:
                        # 1. Update Ticker Fundamentals (The snapshot)
                        self.db.update_ticker_fundamentals(ticker, info)
                        
                        # 2. Add Market Cap to Daily Data (The history)
                        # Use shares_outstanding to calculate market cap for the fetched daily data
                        shares = info.get('shares_outstanding')
                        if shares:
                            # Apply to the entire fetched dataframe
                            # This assumes shares count is constant for the fetch window (usually few days)
                            df['market_cap'] = df['close'] * shares
                            df['market_cap'] = df['market_cap'].fillna(0).astype('int64')

                except Exception as e_fund:
                    print(f"Warning: Could not update fundamentals for {ticker}: {e_fund}")


                # Rate limit (be nice to yfinance)
                time.sleep(0.2)
                
            except Exception as e:
                error_count += 1
                # print(f"Failed {ticker}: {e}") # Optional: verify verbose
                
        print("\n--- Update Completed ---")
        print(f"Processed: {len(tickers)}")
        print(f"Success:   {success_count}")
        print(f"Skipped:   {skip_count} (No new data)")
        print(f"Delisted:  {delisted_count} (Removed from DB)")
        print(f"Errors:    {error_count}")
        self._warmup_industry_cache()

if __name__ == "__main__":
    updater = DailyUpdater()
    updater.run()
