from market_data_db import MarketDataDB
from sqlalchemy import create_engine, text
import sys

# Local Config (Same as migrate_manual.py)
OLD_USER = "postgres"                
OLD_PASS = "ad25287706" 
OLD_HOST = "localhost"
OLD_PORT = "5432"
OLD_DB   = "postgres"

def check_ticker(ticker):
    print(f"Checking ticker: {ticker}")
    
    # 1. Check Local
    try:
        old_url = f"postgresql+psycopg2://{OLD_USER}:{OLD_PASS}@{OLD_HOST}:{OLD_PORT}/{OLD_DB}"
        old_engine = create_engine(old_url)
        with old_engine.connect() as conn:
            exists = conn.execute(text(f"SELECT count(*) FROM tickers WHERE ticker='{ticker}'")).scalar()
            print(f"  [Local]  Found in tickers? {'YES' if exists else 'NO'}")
    except Exception as e:
        print(f"  [Local]  Error: {e}")

    # 2. Check Remote
    try:
        new_db = MarketDataDB()
        with new_db.engine.connect() as conn:
            exists = conn.execute(text(f"SELECT count(*) FROM tickers WHERE ticker='{ticker}'")).scalar()
            print(f"  [Cloud]  Found in tickers? {'YES' if exists else 'NO'}")
    except Exception as e:
        print(f"  [Cloud]  Error: {e}")

if __name__ == "__main__":
    check_ticker("FEAM")
