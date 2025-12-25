from market_data_db import MarketDataDB
from sqlalchemy import text
import pandas as pd

def check_db_size():
    print("Calculating Database Size...")
    try:
        db = MarketDataDB()
        with db.engine.connect() as conn:
            # 1. Total Database Size
            query_total = text("SELECT pg_size_pretty(pg_database_size(current_database())) as total_size;")
            total_size = conn.execute(query_total).scalar()
            
            # 2. Daily Prices Table Size (Data + Indexes)
            query_table = text("SELECT pg_size_pretty(pg_total_relation_size('us_daily_prices')) as table_size;")
            table_size = conn.execute(query_table).scalar()
            
            # 3. Row Count
            query_count = text("SELECT COUNT(*) FROM us_daily_prices;")
            row_count = conn.execute(query_count).scalar()
            
            print(f"\n[Database Statistics]")
            print(f"Total DB Name: {os.getenv('DB_NAME', 'postgres')}")
            print(f"Total DB Size: {total_size}")
            print(f"Prices Table Size: {table_size}")
            print(f"Total Rows (Prices): {row_count:,}")
            
            # Estimate per row
            if row_count > 0:
                # Get raw size in bytes for calculation
                size_bytes = conn.execute(text("SELECT pg_total_relation_size('us_daily_prices')")).scalar()
                avg_row_size = size_bytes / row_count
                print(f"Avg Size per Row: {avg_row_size:.2f} bytes")

    except Exception as e:
        print(f"Error checking size: {e}")

import os
if __name__ == "__main__":
    check_db_size()
