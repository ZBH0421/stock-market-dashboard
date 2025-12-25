from batch_run import BatchController

# Configuration
EXCEL_FILE = "US_Stocks_Classified.xlsx"
START_DATE = "2024-01-01"
END_DATE = "2025-12-24"

print(f"--- FETCHING MISSING INDUSTRIES ---")
print(f"Source: {EXCEL_FILE}")
print(f"Mode: Missing Only (Smart Skip)")

try:
    controller = BatchController(EXCEL_FILE)
    # Run with missing_only=True to automatically skip existing names
    controller.run(START_DATE, END_DATE, missing_only=True)
    print("\n✅ Missing data fetch completed successfully!")
except Exception as e:
    print(f"\n❌ Batch Process Failed: {e}")
