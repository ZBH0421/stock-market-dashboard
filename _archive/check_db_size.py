from market_data_db import MarketDataDB
from sqlalchemy import text

def check_size():
    db = MarketDataDB()
    with db.engine.connect() as conn:
        # 1. Total DB Size
        total_size = conn.execute(text("SELECT pg_size_pretty(pg_database_size(current_database()))")).scalar()
        
        # 2. Table Sizes
        print(f"Total Database Size: {total_size}")
        print("-" * 30)
        print(f"{'Table':<20} | {'Size':<10}")
        print("-" * 30)
        
        tables = ['industries', 'tickers', 'us_daily_prices']
        for t in tables:
            size = conn.execute(text(f"SELECT pg_size_pretty(pg_total_relation_size('{t}'))")).scalar()
            print(f"{t:<20} | {size:<10}")

        # 3. Row Counts
        print("-" * 30)
        print(f"{'Table':<20} | {'Rows':<10}")
        print("-" * 30)
        for t in tables:
            count = conn.execute(text(f"SELECT count(*) FROM {t}")).scalar()
            print(f"{t:<20} | {count:<10}")

if __name__ == "__main__":
    check_size()
