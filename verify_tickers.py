from market_data_db import MarketDataDB
from sqlalchemy import text

def verify_specific_tickers():
    db = MarketDataDB()
    with db.engine.connect() as conn:
        print("--- Precision Ticker Check ---")
        
        # Exact names from Step 921 dump
        targets = {
            "Semiondutors": ["NVDA", "AMD", "QCOM"],
            "Softwre - Infrstruture": ["MSFT", "ORCL"],
            "Biotehnology": ["VRTX", "NUVL"]
        }
        
        for ind_name, expected_list in targets.items():
            print(f"\n[Industry: '{ind_name}']")
            
            # 1. Get ID
            ind_id = conn.execute(text("SELECT id FROM industries WHERE name = :n"), {"n": ind_name}).scalar()
            if not ind_id:
                print("   -> NOT FOUND in DB (Even with exact copy paste!)")
                continue
                
            # 2. Get Tickers
            found_tickers = [r[0] for r in conn.execute(text("SELECT ticker FROM tickers WHERE industry_id = :i"), {"i": ind_id}).fetchall()]
            print(f"   -> Total Tickers: {len(found_tickers)}")
            
            # 3. Check Expected
            for exp in expected_list:
                status = "✅ FOUND" if exp in found_tickers else "❌ MISSING"
                print(f"   -> {exp}: {status}")

            # 4. Price Check for first expected
            if expected_list and expected_list[0] in found_tickers:
                t = expected_list[0]
                rows = conn.execute(text("SELECT COUNT(*) FROM us_daily_prices WHERE symbol = :s"), {"s": t}).scalar()
                print(f"   -> Data Check ({t}): {rows} rows of price data.")

if __name__ == "__main__":
    verify_specific_tickers()
