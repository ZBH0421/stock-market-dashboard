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

    def run(self, start_date: str, end_date: str, target_industry: str = None):
        """
        Main execution loop.
        Iterates through all sheets in the Excel file, fetches data for each ticker,
        and saves it to the database.
        
        Args:
            target_industry (str): If provided, only process this specific industry (Sheet).
        """
        print(f"Reading Excel file: {self.excel_path}...")
        
        try:
            # Load Excel File mapping (efficient for mapping sheet names)
            xls = pd.ExcelFile(self.excel_path)
            sheet_names = xls.sheet_names
            print(f"Found {len(sheet_names)} sheets (Industries).")
            
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
            print(f"------------------------------------------------")
            
            try:
                # 1. Register Industry
                industry_id = self.db.get_or_create_industry(sheet_name)
                
                # Read specific sheet
                df_sheet = pd.read_excel(xls, sheet_name=sheet_name)
                
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
