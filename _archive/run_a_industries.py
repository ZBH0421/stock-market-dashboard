import pandas as pd
from batch_run import BatchController
from batch_plot import batch_plot
import os

def main():
    # Configuration
    excel_file = "US_Stocks_Classified.xlsx"
    start_date = "2024-01-01"
    end_date = "2025-12-24"
    
    print(f"Starting Batch Job for 'A' Industries...")
    
    # 1. Identify Target Industries
    try:
        xls = pd.ExcelFile(excel_file)
        all_sheets = xls.sheet_names
        # Filter for industries starting with 'A' (Case sensitive)
        target_industries = [s for s in all_sheets if s.startswith('A')]
        
        print(f"Found {len(target_industries)} industries starting with 'A':")
        for ind in target_industries:
            print(f"  - {ind}")
            
    except Exception as e:
        print(f"❌ Error reading Excel file: {e}")
        return

    # 2. Process Each Industry
    controller = BatchController(excel_file)
    
    for industry in target_industries:
        print(f"\n{'='*50}")
        print(f"Processing: {industry}")
        print(f"{'='*50}")
        
        # Step A: Fetch & Store Data
        print(f"\n[Phase 1] Fetching Data for {industry}...")
        try:
            controller.run(start_date, end_date, target_industry=industry)
        except Exception as e:
            print(f"Failed to fetch data for {industry}: {e}")
            continue

        # Step B: Generate Plots
        print(f"\n[Phase 2] Generating Plots for {industry}...")
        try:
            batch_plot(target_industry=industry)
        except Exception as e:
            print(f"Failed to generate plots for {industry}: {e}")

    print("\nAll 'A' Industries processed successfully.")

if __name__ == "__main__":
    main()
