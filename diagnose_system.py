
import sys
import pandas as pd
import json
import numpy as np
import math
from sqlalchemy import text, inspect
from market_data_db import MarketDataDB

# Setup logging
def log(msg, status="INFO"):
    symbols = {"INFO": "[INFO]", "SUCCESS": "[OK]", "ERROR": "[ERR]", "WARN": "[WARN]"}
    print(f"{symbols.get(status, '')} {msg}")

def run_diagnostics():
    print("==================================================")
    print("   ANTIGRAVITY SYSTEM DIAGNOSTIC TOOL v1.0")
    print("==================================================")
    
    # 1. Database Connection
    log("Testing Database Connection...")
    try:
        db = MarketDataDB()
        with db.engine.connect() as conn:
            log("Connection Successful!", "SUCCESS")
            
            # 2. Schema Verification
            log("Verifying Schema and Table Names...")
            inspector = inspect(db.engine)
            tables = inspector.get_table_names()
            log(f"Tables found: {tables}", "INFO")
            
            if 'us_daily_prices' in tables:
                log("Table 'us_daily_prices' exists.", "SUCCESS")
                price_table = 'us_daily_prices'
            elif 'daily_prices' in tables:
                log("Table 'daily_prices' found (expected 'us_daily_prices'?).", "WARN")
                price_table = 'daily_prices'
            else:
                log("CRITICAL: No daily prices table found!", "ERROR")
                return

            # Check Ticker Table
            if 'tickers' not in tables:
                log("CRITICAL: 'tickers' table missing!", "ERROR")
                return
            
            # 3. Data Integrity Check (Targeting 'Airlines' or first found)
            log("Checking Industry Data...")
            ind_res = conn.execute(text("SELECT id, name FROM industries LIMIT 1")).fetchone()
            if not ind_res:
                log("No industries found in database.", "ERROR")
                return
            
            ind_id, ind_name = ind_res
            log(f"Found Industry: {ind_name} (ID: {ind_id})", "SUCCESS")
            
            # 4. Ticker Fetch Test
            log(f"Fetching Tickers for Industry '{ind_name}'...")
            t_query = text("SELECT ticker, market_cap FROM tickers WHERE industry_id = :iid")
            tickers_df = pd.read_sql(t_query, conn, params={"iid": ind_id})
            
            if tickers_df.empty:
                log(f"No tickers found for {ind_name}.", "ERROR")
                return
                
            tickers = tickers_df['ticker'].tolist()
            log(f"Found {len(tickers)} tickers: {tickers[:5]}...", "SUCCESS")
            
            # 5. Price Fetch Test (The Controversial Part)
            log(f"Testing Price Fetch from '{price_table}'...")
            
            # Test A: ANY Clause (Original)
            try:
                log("Test A: Using 'ANY(:tickers)' syntax...")
                q_any = text(f"SELECT symbol, date, close FROM {price_table} WHERE symbol = ANY(:tickers) LIMIT 5")
                conn.execute(q_any, {"tickers": tickers})
                log("Test A Passed (Syntax valid).", "SUCCESS")
            except Exception as e:
                log(f"Test A Failed: {e}", "ERROR")

            # Test B: IN Clause (User suggestion)
            try:
                log("Test B: Using 'IN :tickers' syntax with tuple...")
                q_in = text(f"SELECT symbol, date, close FROM {price_table} WHERE symbol IN :tickers LIMIT 5")
                conn.execute(q_in, {"tickers": tuple(tickers)})
                log("Test B Passed (Syntax valid).", "SUCCESS")
            except Exception as e:
                log(f"Test B Failed: {e}", "ERROR")

            # Data Volume Check
            q_count = text(f"""
                SELECT COUNT(*) FROM {price_table} 
                WHERE symbol IN :tickers 
                AND date >= CURRENT_DATE - INTERVAL '400 days'
            """)
            count = conn.execute(q_count, {"tickers": tuple(tickers)}).scalar()
            log(f"Price records found (last 400d): {count}", "INFO")
            
            if count == 0:
                log("CRITICAL: SQL is valid but NO DATA returned. Check date range or 'symbol' column content (e.g. BRK.B vs BRK-B)", "ERROR")
                # Sample what's actually in there
                sample = conn.execute(text(f"SELECT symbol FROM {price_table} LIMIT 3")).fetchall()
                log(f"Sample symbols in DB: {sample}", "INFO")

            # 6. Serialization Test
            log("Testing JSON Serialization (NumPy check)...")
            try:
                # Simulate a numpy return
                test_data = {
                    "market_cap": np.int64(100),
                    "pe_ratio": np.float64(12.5),
                    "nan_check": float('nan')
                }
                
                # Try simple dump - expect fail
                try:
                    json.dumps(test_data)
                    log("Standard json.dumps worked (Unexpected for NaN).", "WARN")
                except Exception:
                    log("Standard json.dumps failed as expected.", "INFO")

                # Test safe_float logic
                def safe_float(val):
                    if pd.isna(val) or val is None: return None
                    try:
                        f = float(val)
                        if math.isnan(f) or math.isinf(f): return None
                        return f
                    except: return None
                
                cleaned_data = {
                    "market_cap": int(test_data['market_cap']),
                    "pe_ratio": safe_float(test_data['pe_ratio']),
                    "nan_check": safe_float(test_data['nan_check'])
                }
                
                json_str = json.dumps(cleaned_data)
                log(f"Sanitized JSON: {json_str}", "SUCCESS")
                
            except Exception as e:
                log(f"Serialization Test Failed: {e}", "ERROR")
                
    except Exception as e:
        log(f"Database Connection Failed: {e}", "ERROR")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_diagnostics()
