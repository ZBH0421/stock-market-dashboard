from market_data_db import MarketDataDB
from sqlalchemy import text
import pandas as pd

def audit_db_state():
    db = MarketDataDB()
    with db.engine.connect() as conn:
        print("--- DB INDUSTRY AUDIT ---")
        query = text("""
            SELECT i.id, i.name, COUNT(t.ticker) as ticker_count
            FROM industries i
            LEFT JOIN tickers t ON i.id = t.industry_id
            GROUP BY i.id, i.name
            ORDER BY i.name
        """)
        df = pd.read_sql(query, conn)
        
        # Check specific problem children
        problems = ["Biotehnology", "Biotechnology", "Semiondutors", "Semiconductors", "Softwre", "Software"]
        
        print(f"Total Industries: {len(df)}")
        print("\n[Problematic Names Check]")
        found = df[df['name'].str.contains('|'.join(problems), case=False, regex=True)]
        if not found.empty:
            print(found)
        else:
            print("No obvious problematic names found via regex.")
            
        print("\n[All Industries]")
        print(df.to_string())

if __name__ == "__main__":
    audit_db_state()
