from batch_run import BatchController
import argparse
import os
import sys

def main():
    # --- Configuration ---
    EXCEL_FILE = "US_Stocks_Classified.xlsx"
    
    parser = argparse.ArgumentParser(description="Populate Stock Market Data for specific industries.")
    parser.add_argument("industry", type=str, nargs='?', default=None, 
                        help="Industry name (Sheet name in Excel). e.g., 'Semiconductors'. If empty, runs all.")
    parser.add_argument("--start", type=str, default="2024-01-01", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", type=str, default="2025-12-25", help="End date YYYY-MM-DD")
    
    args = parser.parse_args()

    if not os.path.exists(EXCEL_FILE):
        print(f"❌ Error: {EXCEL_FILE} not found!")
        sys.exit(1)

    print(f"🚀 Initializing Data Ingestion...")
    print(f"📅 Range: {args.start} to {args.end}")
    
    try:
        controller = BatchController(EXCEL_FILE)
        
        if args.industry:
            print(f"🎯 Target Industry: {args.industry}")
            controller.run(args.start, args.end, target_industry=args.industry)
        else:
            print(f"🌍 Mode: FULL RESET (Fetching all industries)")
            controller.run(args.start, args.end, target_industry=None)
            
        print("\n✅ Data population completed successfully.")
        print("Now you can refresh home.html to see the new industry!")

    except Exception as e:
        print(f"\n❌ Execution Failed: {e}")

if __name__ == "__main__":
    main()
