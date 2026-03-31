"""
One-time backfill: fetch trailingEps from yfinance for all tickers in DB.
Skips tickers that already have trailing_eps set.
Run once: venv/bin/python backfill_eps.py
"""
from market_data_db import MarketDataDB
from market_data_fetcher import MarketDataFetcher
from sqlalchemy import text
from tqdm import tqdm
import time

db = MarketDataDB()
fetcher = MarketDataFetcher()

with db.engine.connect() as conn:
    rows = conn.execute(text(
        "SELECT ticker FROM tickers WHERE trailing_eps IS NULL ORDER BY ticker"
    )).fetchall()

tickers = [r[0] for r in rows]
print(f"Backfilling trailing_eps for {len(tickers)} tickers...")

updated = 0
skipped = 0
errors = 0

for ticker in tqdm(tickers, desc="Backfilling EPS"):
    try:
        info = fetcher.get_ticker_info(ticker)
        eps = info.get('trailing_eps')
        if eps and eps > 0:
            with db.engine.begin() as conn:
                conn.execute(
                    text("UPDATE tickers SET trailing_eps = :eps WHERE ticker = :t"),
                    {'eps': eps, 't': ticker}
                )
            updated += 1
        else:
            skipped += 1
        time.sleep(0.3)
    except Exception as e:
        errors += 1

print(f"\nDone. Updated: {updated}, Skipped (no EPS): {skipped}, Errors: {errors}")
