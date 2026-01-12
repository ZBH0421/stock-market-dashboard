from market_data_db import MarketDataDB
from sqlalchemy import create_engine, text

# Local Config
OLD_USER = "postgres"                
OLD_PASS = "ad25287706" 
OLD_HOST = "localhost"
OLD_PORT = "5432"
OLD_DB   = "postgres"

def verify_counts():
    print("Verifying Migration Counts...")
    
    # 1. Source Counts
    old_url = f"postgresql+psycopg2://{OLD_USER}:{OLD_PASS}@{OLD_HOST}:{OLD_PORT}/{OLD_DB}"
    old_engine = create_engine(old_url)
    
    with old_engine.connect() as conn:
        print("  Checking Source (Local)...")
        industries_src = conn.execute(text("SELECT count(*) FROM industries")).scalar()
        tickers_src = conn.execute(text("SELECT count(*) FROM tickers")).scalar()
        # Filter TEST_INTEGRATION
        prices_src = conn.execute(text("SELECT count(*) FROM us_daily_prices WHERE symbol != 'TEST_INTEGRATION'")).scalar()
        
    # 2. Target Counts
    new_db = MarketDataDB()
    with new_db.engine.connect() as conn:
        print("  Checking Target (Cloud)...")
        industries_tgt = conn.execute(text("SELECT count(*) FROM industries")).scalar()
        tickers_tgt = conn.execute(text("SELECT count(*) FROM tickers")).scalar()
        prices_tgt = conn.execute(text("SELECT count(*) FROM us_daily_prices")).scalar()

    print("\n--- Summary ---")
    print(f"Industries: Local={industries_src} | Cloud={industries_tgt} | Match={industries_src==industries_tgt}")
    print(f"Tickers:    Local={tickers_src} | Cloud={tickers_tgt} | Match={tickers_src==tickers_tgt}")
    print(f"Prices:     Local={prices_src} | Cloud={prices_tgt} | Match={prices_src==prices_tgt}")
    
    if prices_src != prices_tgt:
        diff = prices_src - prices_tgt
        print(f"\n[WARNING] Price count mismatch! Diff: {diff}")
    else:
        print("\n[SUCCESS] All data migrated successfully!")

if __name__ == "__main__":
    verify_counts()
