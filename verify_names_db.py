from market_data_db import MarketDataDB
from sqlalchemy import text
import pandas as pd

def check_names():
    db = MarketDataDB()
    with db.engine.connect() as conn:
        print("--- Name Verification Audit ---")
        
        # Check the ones we supposedly fixed
        targets = [
            "Biotehnology", "Biotechnology", 
            "Semiondutors", "Semiconductors",
            "Aerospe & Defense", "Aerospace & Defense",
            "Bnks - Regionl", "Banks - Regional"
        ]
        
        for t in targets:
            res = conn.execute(text("SELECT id, name FROM industries WHERE name = :n"), {"n": t}).fetchone()
            if res:
                print(f"FOUND: '{res[1]}' (ID: {res[0]})")
            else:
                print(f"MISSING: '{t}'")

if __name__ == "__main__":
    check_names()
