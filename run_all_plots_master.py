from market_data_db import MarketDataDB
from batch_plot import batch_plot
from sqlalchemy import text
import pandas as pd
import time

def run_all_plots():
    print("Starting MASSIVE Batch Plotting Task for ALL Industries...")
    
    db = MarketDataDB()
    
    # 1. Fetch ALL Industries from DB
    with db.engine.connect() as conn:
        query = text("SELECT name FROM industries ORDER BY name")
        industries = pd.read_sql(query, conn)['name'].tolist()
        
    total_ind = len(industries)
    print(f"Found {total_ind} industries to process.")
    
    if not industries:
        print("No industries found in database.")
        return

    # 2. Iterate and Plot
    print(f"\n{'='*60}")
    print("Beginning Full Scale Visualization Task")
    print(f"{'='*60}")
    
    start_time = time.time()

    for idx, ind in enumerate(industries):
        print(f"\n[{idx+1}/{total_ind}] Industry: {ind}")
        try:
            # batch_plot handles folder creation ({ind}_plot) and querying tickers
            batch_plot(target_industry=ind)
        except Exception as e:
            print(f"Error processing {ind}: {e}")

    elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"[DONE] All {total_ind} industries processed in {elapsed/60:.2f} minutes.")
    print(f"{'='*60}")

if __name__ == "__main__":
    run_all_plots()
