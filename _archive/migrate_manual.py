from market_data_db import MarketDataDB
from sqlalchemy import create_engine, text
import pandas as pd
import time
import sys

# ==========================================
# 🛑 請在下方填入您「舊電腦/本機」資料庫的密碼
# ==========================================
OLD_USER = "postgres"                
OLD_PASS = "ad25287706"  # <--- ⚠️ 請修改這裡 (填入舊密碼)
OLD_HOST = "localhost"
OLD_PORT = "5432"
OLD_DB   = "postgres"
# ==========================================

print("--- Starting Manual Migration ---")
print(f"Source (Local): {OLD_HOST}:{OLD_PORT} ({OLD_DB})")
print("Target (Cloud): Supabase (via .env)")

# 1. 建立舊家 (Local) 連線
try:
    old_url = f"postgresql+psycopg2://{OLD_USER}:{OLD_PASS}@{OLD_HOST}:{OLD_PORT}/{OLD_DB}"
    old_engine = create_engine(old_url)
    with old_engine.connect() as conn:
        print("[Check] Connected to Local DB successfully.")
except Exception as e:
    print(f"\n[Error] Failed to connect to LOCAL DB. Please check your password inside this script.\n{e}")
    sys.exit(1)

# 2. 建立新家 (Supabase) 連線
try:
    new_db = MarketDataDB() # Reads from .env
    print("[Check] Connected to Cloud DB successfully.")
except Exception as e:
    print(f"\n[Error] Failed to connect to CLOUD DB. Check your .env file.\n{e}")
    sys.exit(1)

from sqlalchemy.dialects.postgresql import insert as psql_insert

def upsert_on_conflict(table, conn, keys, data_iter):
    """
    Custom method for pandas to_sql to perform PostgreSQL UPSERT (DO NOTHING).
    """
    data = [dict(zip(keys, row)) for row in data_iter]
    if not data:
        return

    # Use the table from the metadata
    stmt = psql_insert(table.table).values(data)
    
    # Identify Primary Key columns for conflict resolution
    # Pandas table object might not have PKs reflected if not created via SQLALchemy reflection
    if table.name == 'us_daily_prices':
        pk_cols = ['symbol', 'date']
    elif table.name == 'tickers':
        pk_cols = ['ticker']
    elif table.name == 'industries':
        pk_cols = ['id'] # or name? Schema says id is PK, name is unique.
    else:
        # Fallback to metadata reflection if available
        pk_cols = [key.name for key in table.table.primary_key]
        
    if not pk_cols:
        # Fallback: simple insert if no PK found
        # (This was the bug: pandas didn't see PKs, so it did simple insert -> crash on overlap)
        print(f"    [Warning] No PK found for {table.name}, performing simple INSERT.")
        conn.execute(stmt)
        return

    # ON CONFLICT DO NOTHING
    stmt = stmt.on_conflict_do_nothing(index_elements=pk_cols)
    conn.execute(stmt)

def migrate_table(table_name, chunk_size=5000):
    print(f"\n>>> Migrating Table: [{table_name}]")
    
    # Check if target is empty to avoid duplicates and enable RESUME
    start_offset = 0
    safety_margin = 5000 # Overlap to ensure no gaps from partial batches
    
    with new_db.engine.connect() as conn:
        existing = conn.execute(text(f"SELECT count(*) FROM {table_name}")).scalar()
        if existing > 0:
            # Back off by safety_margin to handle partial updates safely using UPSERT
            start_offset = max(0, existing - safety_margin)
            print(f"    [Checkpoint] Found {existing} rows in Target.")
            print(f"    [Resume] Starting from offset {start_offset} (Overlap {safety_margin} rows for safety).")
        else:
            print("    [Start] Target is empty. Starting from scratch.")

    # 讀取舊資料總數
    with old_engine.connect() as conn:
        if table_name == 'us_daily_prices':
            count_query = f"SELECT count(*) FROM {table_name} WHERE symbol != 'TEST_INTEGRATION'"
        else:
            count_query = f"SELECT count(*) FROM {table_name}"
            
        total = conn.execute(text(count_query)).scalar()
        print(f"    Total rows in Source (Filtered): {total}")
        
    if start_offset >= total:
        print("    [Skip] Target appears to have all rows. Migration complete for this table.")
        return

    # 分批搬運
    offset = start_offset
    start_time = time.time()
    
    while offset < total:
        try:
            # 從舊家讀取
            if table_name == 'us_daily_prices':
                query = f"SELECT * FROM {table_name} WHERE symbol != 'TEST_INTEGRATION' ORDER BY 1 LIMIT {chunk_size} OFFSET {offset}"
            else:
                query = f"SELECT * FROM {table_name} ORDER BY 1 LIMIT {chunk_size} OFFSET {offset}"
                
            df = pd.read_sql(query, old_engine)
            
            if df.empty:
                break
                
            # 寫入新家 (Supabase) using UPSERT
            # method=upsert_on_conflict handles "ON CONFLICT DO NOTHING"
            df.to_sql(table_name, new_db.engine, if_exists='append', 
                     index=False, method=upsert_on_conflict, chunksize=1000)
            
            offset += len(df)
            elapsed = time.time() - start_time
            if elapsed > 0:
                speed = (offset - start_offset) / elapsed
            else:
                speed = 0
            
            remaining = total - offset
            percent = (offset / total) * 100
            
            # Enhanced Status
            print(f"    [Progress] {percent:.1f}% | Moved: {offset} | Remaining: {remaining} | Speed: {speed:.0f} rows/sec")
            
            # Anti-throttle sleep
            time.sleep(0.5) 
            
        except Exception as e:
            import traceback
            error_msg = f"Batch failed at offset {offset}: {e}\n{traceback.format_exc()}"
            print(f"    [Error] {error_msg}")
            
            with open("migration_error.log", "w", encoding="utf-8") as f:
                f.write(error_msg)
                
            print("    [Checkpoint] You can restart the script to resume from this point.")
            break

if __name__ == "__main__":
    # 順序很重要: Parents first
    migrate_table('industries')
    migrate_table('tickers')
    migrate_table('us_daily_prices')
    
    print("\n[SUCCESS] Migration Completed!")
