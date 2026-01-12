from market_data_db import MarketDataDB
from batch_plot import batch_plot
from sqlalchemy import text
import pandas as pd

def run_b_plots():
    print("Starting Batch Plotting for 'B' Industries...")
    
    db = MarketDataDB()
    
    # 1. Fetch Target Industries from DB that start with 'B'
    with db.engine.connect() as conn:
        query = text("SELECT name FROM industries WHERE name LIKE 'B%' ORDER BY name")
        industries = pd.read_sql(query, conn)['name'].tolist()
        
    print(f"Found {len(industries)} industries starting with 'B':")
    for ind in industries:
        print(f"  - {ind}")
        
    if not industries:
        print("No industries found.")
        return

    # 2. Iterate and Plot
    print(f"\n{'='*60}")
    print("Beginning Batch Visualization Task")
    print(f"{'='*60}")

    for ind in industries:
        print(f"\n>>> Generating plots for Industry: {ind}")
        try:
            # batch_plot handles folder creation ({ind}_plot) and querying tickers
            batch_plot(target_industry=ind)
        except Exception as e:
            print(f"Error processing {ind}: {e}")

    print("\n[DONE] All 'B' Industries processed.")

if __name__ == "__main__":
    run_b_plots()
