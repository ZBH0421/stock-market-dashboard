from market_data_db import MarketDataDB
from sqlalchemy import text

def diagnose_rev():
    db = MarketDataDB()
    with db.engine.connect() as conn:
        print("--- Diagnosing Revenue Data ---")
        rev_count = conn.execute(text("SELECT COUNT(*) FROM tickers WHERE revenue IS NOT NULL AND revenue > 0")).scalar()
        total = conn.execute(text("SELECT COUNT(*) FROM tickers")).scalar()
        print(f"Tickers with Revenue > 0: {rev_count} / {total}")

if __name__ == "__main__":
    diagnose_rev()
