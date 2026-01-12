from market_data_db import MarketDataDB
from sqlalchemy import text
import sys

def test_connection():
    print("--- Testing Connection to Supabase ---")
    try:
        # 1. Initialize DB (This will attempt to connect and create tables)
        db = MarketDataDB()
        print("[OK] Connection Successful! Schema Initialized.")
        
        # 2. Check if tables are truly empty (Fresh DB)
        with db.engine.connect() as conn:
            ind_count = conn.execute(text("SELECT count(*) FROM industries")).scalar()
            ticker_count = conn.execute(text("SELECT count(*) FROM tickers")).scalar()
            
            print(f"\n[Status Check]")
            print(f"  Industries: {ind_count}")
            print(f"  Tickers:    {ticker_count}")
            
            if ind_count == 0:
                print("\n[Result] Database is connected and EMPTY (Ready for migration).")
            else:
                print(f"\n[Result] Database already has {ind_count} industries.")

    except Exception as e:
        print(f"\n[FATAL ERROR] Connection Failed:\n{e}")
        sys.exit(1)

if __name__ == "__main__":
    test_connection()
