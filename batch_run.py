from market_data_fetcher import MarketDataFetcher
from market_data_db import MarketDataDB
import pandas as pd
import time
import os
from tqdm import tqdm

class BatchController:
    """
    Orchestrates the batch processing of stock data from an Excel file to the database.
    """
    def __init__(self, excel_path: str):
        self.excel_path = excel_path
        
        # Initialize Core Components
        try:
            self.db = MarketDataDB()
            self.fetcher = MarketDataFetcher()
        except Exception as e:
            raise RuntimeError(f"Failed to initialize components: {e}")

    def run(self, start_date: str, end_date: str, target_industry: str = None, missing_only: bool = False):
        """
        Main execution loop.
        Iterates through all sheets in the Excel file, fetches data for each ticker,
        and saves it to the database.
        
        Args:
            target_industry (str): If provided, only process this specific industry (Sheet).
            missing_only (bool): If True, skip industries that already exist in the DB.
        """
        print(f"Reading Excel file: {self.excel_path}...")
        
        try:
            # Load Excel File mapping (efficient for mapping sheet names)
            xls = pd.ExcelFile(self.excel_path)
            sheet_names = xls.sheet_names
            print(f"Found {len(sheet_names)} sheets (Industries).")

            # Pre-fetch existing industries if in missing_only mode
            existing_industries = set()
            if missing_only:
                with self.db.engine.connect() as conn:
                    # Generic SQL for compatibility
                    from sqlalchemy import text
                    rows = conn.execute(text("SELECT name FROM industries")).fetchall()
                    existing_industries = set(r[0] for r in rows)
                print(f"Mode: Missing Only. Found {len(existing_industries)} existing industries in DB.")
            
        except Exception as e:
            print(f"Critical Error: Could not read Excel file. {e}")
            return

        # Iterate through each Industry (Sheet)
        for sheet_name in sheet_names:
            # --- Filter Logic ---
            if target_industry and sheet_name != target_industry:
                continue # Skip irrelevant sheets
            
            print(f"\n------------------------------------------------")
            print(f"Processing Industry: {sheet_name}")
            
            # --- Name Resolution (Peeking) ---
            # We need to know the 'Real Name' to check if it's missing.
            # Read just a few rows to get the Industry column.
            try:
                df_peek = pd.read_excel(xls, sheet_name=sheet_name, nrows=5)
            except Exception as e:
                print(f"Error peeking sheet '{sheet_name}': {e}")
                continue

            # Determine Correct Industry Name
            final_industry_name = sheet_name
            if 'Industry' in df_peek.columns and not df_peek['Industry'].dropna().empty:
                candidate_name = df_peek['Industry'].dropna().iloc[0]
                if isinstance(candidate_name, str) and len(candidate_name) > 3:
                    final_industry_name = candidate_name.strip()
            
            print(f"   -> Identified as: '{final_industry_name}'")

            # --- Missing Only Check ---
            if missing_only and final_industry_name in existing_industries:
                print(f"   -> [SKIP] Already exists in DB.")
                continue

            print(f"------------------------------------------------")
            
            try:
                # Read FULL sheet now
                df_sheet = pd.read_excel(xls, sheet_name=sheet_name)
                
                # Check for 'Ticker' column
                if 'Ticker' not in df_sheet.columns:
                    print(f"Warning: Sheet '{sheet_name}' missing 'Ticker' column (Skipping).")
                    continue
                
                # 2. Register Industry (with potentially corrected name)
                industry_id = self.db.get_or_create_industry(final_industry_name)
                
                # Check for 'Ticker' column
                if 'Ticker' not in df_sheet.columns:
                    print(f"Warning: Sheet '{sheet_name}' missing 'Ticker' column (Skipping).")
                    continue
                
                # Get unique clean tickers
                tickers = df_sheet['Ticker'].dropna().unique().tolist()
                print(f"Found {len(tickers)} tickers in {sheet_name}")
                
                # Ticker Loop with Progress Bar
                # desc defaults to progress bar description
                pbar = tqdm(tickers, desc=f"Fetching {sheet_name}")
                
                for ticker in pbar:
                    pbar.set_postfix({'ticker': ticker})
                    
                    # 2. Get Info (Fundamentals)
                    info = self.fetcher.get_ticker_info(ticker)
                    
                    # 3. Register Ticker (Hierarchical + Metadata)
                    self.db.register_ticker(ticker, industry_id, info=info)
                    
                    # 4. Fetch & Save Prices
                    self._process_single_ticker(ticker, start_date, end_date)
                    
                    # Rate Limiting
                    time.sleep(1.0) 
            
            except Exception as e:
                print(f"Error processing sheet '{sheet_name}': {e}")
                continue
    
    def _process_single_ticker(self, ticker: str, start_date: str, end_date: str):
        """
        Handles the Fetch -> Transform -> Save logic for a single ticker.
        Safely handles errors to prevent loop interruption.
        """
        try:
            # 1. Fetch
            df = self.fetcher.fetch_us_daily_close(ticker, start_date, end_date)
            
            if df is None or df.empty:
                # Fetcher already logs warning, so we just return
                return

            # 2. Transform (Inject Symbol for DB)
            df['symbol'] = ticker
            
            # 3. Save
            self.db.save_daily_data(df)
            
        except Exception as e:
            # Catch-all for unexpected errors (e.g., DB connection drop, weird data format)
            print(f"    System Error processing {ticker}: {e}")

if __name__ == "__main__":
    # --- Configuration ---
    EXCEL_FILE = "US_Stocks_Classified.xlsx"
    START_DATE = "2024-01-01"
    END_DATE = "2025-12-24"
    
    print(f"Starting Batch Job")
    print(f"Target: {EXCEL_FILE}")
    print(f"Range: {START_DATE} to {END_DATE}")
    
    if not os.path.exists(EXCEL_FILE):
        print(f"Error: File {EXCEL_FILE} not found in current directory.")
        exit(1)
        
    # --- Filter ---
    TARGET_INDUSTRY = "Airlines" # User requested Airlines specifically
    
    try:
        controller = BatchController(EXCEL_FILE)
        controller.run(START_DATE, END_DATE, target_industry=TARGET_INDUSTRY)
        print("\nBatch Job Completed Successfully.")
    except Exception as e:
        print(f"\nBatch Job Failed: {e}")
