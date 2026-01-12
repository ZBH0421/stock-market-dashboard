from batch_run import BatchController
import os

if __name__ == "__main__":
    # --- Configuration ---
    EXCEL_FILE = "US_Stocks_Classified.xlsx"
    START_DATE = "2024-01-01"
    END_DATE = "2025-12-24"
    
    print(f"Starting Full Batch Job (All Industries)")
    print(f"Target: {EXCEL_FILE}")
    print(f"Range: {START_DATE} to {END_DATE}")
    
    if not os.path.exists(EXCEL_FILE):
        print(f"Error: File {EXCEL_FILE} not found in current directory.")
        exit(1)
        
    try:
        controller = BatchController(EXCEL_FILE)
        # target_industry=None means fetch ALL
        controller.run(START_DATE, END_DATE, target_industry=None)
        print("\nFull Batch Job Completed Successfully.")
    except Exception as e:
        print(f"\nBatch Job Failed: {e}")
