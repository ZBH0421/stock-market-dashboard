from market_data_db import MarketDataDB
from sqlalchemy import text

db = MarketDataDB()
with db.engine.connect() as conn:
    res = conn.execute(text("SELECT name FROM industries WHERE name LIKE '%Aerospace%'")).fetchone()
    if res:
        print(f"DB Value: '{res[0]}'")
    else:
        print("Not Found")
