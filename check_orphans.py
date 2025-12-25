from sqlalchemy import create_engine, text
import pandas as pd

# Local Config
OLD_USER = "postgres"                
OLD_PASS = "ad25287706" 
OLD_HOST = "localhost"
OLD_PORT = "5432"
OLD_DB   = "postgres"

def check_orphans():
    print("Checking for Orphan Tickers (Present in Prices, Missing in Tickers)...")
    
    url = f"postgresql+psycopg2://{OLD_USER}:{OLD_PASS}@{OLD_HOST}:{OLD_PORT}/{OLD_DB}"
    engine = create_engine(url)
    
    query = """
    SELECT DISTINCT p.symbol 
    FROM us_daily_prices p
    LEFT JOIN tickers t ON p.symbol = t.ticker
    WHERE t.ticker IS NULL
    """
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
        
    if not df.empty:
        print(f"\n[FOUND] {len(df)} Orphan Tickers!")
        print(df['symbol'].tolist())
    else:
        print("\n[OK] No orphan tickers found in Source DB.")

if __name__ == "__main__":
    check_orphans()
