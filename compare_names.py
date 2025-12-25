from market_data_db import MarketDataDB
from sqlalchemy import text
import pandas as pd

def compare_names():
    # 1. Load User's Correct File
    try:
        # Note: Filename has a space before .xlsx
        df = pd.read_excel('industry name .xlsx')
        user_names = sorted(df[df.columns[0]].dropna().unique().tolist()) # Assume first column is name
        print(f"User provided {len(user_names)} correct names.")
    except Exception as e:
        print(f"Error reading excel: {e}")
        return

    # 2. Load Current DB Names
    db = MarketDataDB()
    with db.engine.connect() as conn:
        db_names = sorted([r[0] for r in conn.execute(text("SELECT name FROM industries")).fetchall()])
        
    print(f"Database currently has {len(db_names)} names.")
    
    # 3. Comparison
    print("\n--- DISCREPANCY CHECK ---")
    
    # Check if DB has names NOT in user list (Typos?)
    suspicious = [d for d in db_names if d not in user_names]
    print(f"\n[In DB but NOT in User List] (Potential lingering typos): {len(suspicious)}")
    for s in suspicious:
        print(f"  ? '{s}'")
        
    # Check if User List has names NOT in DB (Missing industries?)
    missing = [u for u in user_names if u not in db_names]
    print(f"\n[In User List but NOT in DB] (Missing/Mismatched): {len(missing)}")
    for m in missing:
        print(f"  ! '{m}'")
        
    # Match Rate
    common = set(db_names).intersection(set(user_names))
    print(f"\nExact Matches: {len(common)}")

if __name__ == "__main__":
    compare_names()
