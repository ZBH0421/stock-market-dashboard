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

    def _regenerate_sector_rrg(self):
        """Force-regenerate today's sector rotation history cache from latest DB data."""
        script = Path(__file__).parent / "generate_rotation_history.py"
        print("\n--- Regenerating sector RRG cache ---")
        result = subprocess.run(
            [sys.executable, str(script), "--force"],
            check=False,
        )
        if result.returncode != 0:
            print(f"  WARNING: sector RRG regeneration failed (exit {result.returncode})")
        else:
            print("  Sector RRG cache updated.")

    def _regenerate_sector_rrg_daily(self):
        """Force-regenerate today's daily sector rotation history cache from latest DB data."""
        script = Path(__file__).parent / "generate_rotation_history.py"
        print("\n--- Regenerating daily sector RRG cache ---")
        result = subprocess.run(
            [sys.executable, str(script), "--force", "--interval", "daily"],
            check=False,
        )
        if result.returncode != 0:
            print(f"  WARNING: daily sector RRG regeneration failed (exit {result.returncode})")
        else:
            print("  Daily sector RRG cache updated.")

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

    def _refresh_shiller_cape(self):
        """Pre-fetch Shiller CAPE so it's ready for the day (deletes stale cache)."""
        import datetime as _dt, json as _json, urllib.request, tempfile, os
        cache = Path("/tmp/shiller_cape_cache.json")
        cache.unlink(missing_ok=True)
        print("\n--- Refreshing Shiller CAPE cache ---")
        try:
            url = "http://www.econ.yale.edu/~shiller/data/ie_data.xls"
            tmp = Path(tempfile.mktemp(suffix=".xls"))
            urllib.request.urlretrieve(url, tmp)
            import openpyxl, pandas as _pd
            wb = openpyxl.load_workbook(tmp, read_only=True, data_only=True)
            ws = wb.worksheets[1]
            rows = list(ws.iter_rows(values_only=True))
            header_row = next(i for i, r in enumerate(rows) if r[0] == "Date")
            data_rows = [r for r in rows[header_row + 2:] if r[0] and r[12] is not None]
            latest = data_rows[-1]
            cape = float(latest[12])
            raw_date = str(latest[0])
            year = int(raw_date[:4])
            month = int(round((float(raw_date) - int(raw_date[:4])) * 100))
            month = max(1, min(12, month))
            date_str = f"{year}-{month:02d}"
            cache.write_text(_json.dumps({"cape": round(cape, 2), "date": date_str,
                                          "source": "Robert Shiller / Yale (ie_data.xls)"}))
            print(f"  CAPE updated: {cape:.2f} ({date_str})")
            tmp.unlink(missing_ok=True)
        except Exception as e:
            print(f"  WARNING: Shiller CAPE refresh failed: {e}")

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
        self._regenerate_sector_rrg()
        self._regenerate_sector_rrg_daily()
        self._warmup_industry_cache()
        self._refresh_shiller_cape()

if __name__ == "__main__":
    updater = DailyUpdater()
    updater.run()
