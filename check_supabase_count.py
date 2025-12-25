from market_data_db import MarketDataDB
from sqlalchemy import text

def check_count():
    try:
        db = MarketDataDB()
        with db.engine.connect() as conn:
            count = conn.execute(text("SELECT count(*) FROM us_daily_prices")).scalar()
            print(f"Current rows in Supabase 'us_daily_prices': {count}")
    except Exception as e:
        print(f"Error checking count: {e}")

if __name__ == "__main__":
    check_count()
