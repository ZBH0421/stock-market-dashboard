# -*- coding: utf-8 -*-
"""
自動化股票價格抓取工具
從 Excel 檔案讀取多個產業分類的股票代號，使用 yfinance 抓取過去 2 個月的收盤價
"""

import pandas as pd
import yfinance as yf
import time
from datetime import datetime, timedelta


def fetch_stock_data(ticker: str, industry: str, period_months: int = 2) -> pd.DataFrame:
    """
    抓取單一股票的歷史收盤價
    
    Args:
        ticker: 股票代號
        industry: 產業分類
        period_months: 抓取期間（月數）
    
    Returns:
        包含 Date, Ticker, Close, Industry 的 DataFrame
    """
    try:
        # 計算日期範圍
        end_date = datetime.now()
        start_date = end_date - timedelta(days=period_months * 30)
        
        # 使用 yfinance 抓取數據
        stock = yf.Ticker(ticker)
        hist = stock.history(start=start_date.strftime('%Y-%m-%d'), 
                            end=end_date.strftime('%Y-%m-%d'))
        
        if hist.empty:
            print(f"[WARN] {ticker} - No data found")
            return pd.DataFrame()
        
        # 整理 DataFrame
        df = hist[['Close']].reset_index()
        df['Ticker'] = ticker
        df['Industry'] = industry
        
        # 確保欄位順序
        df = df[['Date', 'Ticker', 'Close', 'Industry']]
        
        print(f"[OK] {ticker} ({industry}) - {len(df)} records")
        return df
        
    except Exception as e:
        print(f"[ERROR] {ticker} - {str(e)}")
        return pd.DataFrame()


def read_excel_tickers(excel_path: str) -> dict:
    """
    讀取 Excel 檔案中所有 Sheet 的 Ticker
    
    Args:
        excel_path: Excel 檔案路徑
    
    Returns:
        字典格式 {sheet_name: [ticker1, ticker2, ...]}
    """
    industry_tickers = {}
    
    try:
        # 讀取所有 Sheet 名稱
        xlsx = pd.ExcelFile(excel_path)
        sheet_names = xlsx.sheet_names
        
        print(f"[INFO] Found {len(sheet_names)} industry sheets")
        
        for sheet_name in sheet_names:
            df = pd.read_excel(xlsx, sheet_name=sheet_name)
            
            # 檢查是否有 Ticker 欄位
            if 'Ticker' in df.columns:
                tickers = df['Ticker'].dropna().unique().tolist()
                industry_tickers[sheet_name] = tickers
                print(f"  - {sheet_name}: {len(tickers)} stocks")
            else:
                print(f"  - {sheet_name}: 'Ticker' column not found")
                
    except Exception as e:
        print(f"[ERROR] Cannot read Excel file: {str(e)}")
        
    return industry_tickers


def main():
    """主程式"""
    # ========== 設定參數 ==========
    EXCEL_PATH = "US_Stocks_Classified.xlsx"  # Excel 檔案路徑
    OUTPUT_PATH = "all_stocks_data.csv"        # 輸出 CSV 路徑
    SLEEP_SECONDS = 1.5                        # 每次請求間隔（秒）
    PERIOD_MONTHS = 2                          # 抓取期間（月）
    # ==============================
    
    print("=" * 50)
    print("Stock Price Fetcher - Starting...")
    print("=" * 50)
    
    # Step 1: 讀取 Excel 中的所有 Ticker
    print("\n[Step 1] Reading Excel file...")
    industry_tickers = read_excel_tickers(EXCEL_PATH)
    
    if not industry_tickers:
        print("[ERROR] No tickers found, exiting...")
        return
    
    # 計算總股票數
    total_tickers = sum(len(tickers) for tickers in industry_tickers.values())
    print(f"\n[INFO] Total {total_tickers} stocks to fetch")
    
    # Step 2: 逐一抓取股價
    print("\n[Step 2] Fetching stock prices...")
    all_data = []
    processed = 0
    
    for industry, tickers in industry_tickers.items():
        print(f"\n{'-' * 40}")
        print(f"Industry: {industry}")
        print(f"{'-' * 40}")
        
        for ticker in tickers:
            processed += 1
            print(f"[{processed}/{total_tickers}] ", end="")
            
            # 抓取股票數據
            df = fetch_stock_data(ticker, industry, PERIOD_MONTHS)
            
            if not df.empty:
                all_data.append(df)
            
            # 加入延遲避免被封鎖
            time.sleep(SLEEP_SECONDS)
    
    # Step 3: 整合並儲存資料
    print("\n[Step 3] Saving data...")
    
    if all_data:
        # 合併所有 DataFrame
        final_df = pd.concat(all_data, ignore_index=True)
        
        # 確保日期格式正確
        final_df['Date'] = pd.to_datetime(final_df['Date']).dt.strftime('%Y-%m-%d')
        
        # 儲存為 CSV（覆蓋模式）
        final_df.to_csv(OUTPUT_PATH, index=False, encoding='utf-8-sig')
        
        print(f"\n[SUCCESS] Data saved to: {OUTPUT_PATH}")
        print(f"  - Total records: {len(final_df)}")
        print(f"  - Stocks fetched: {final_df['Ticker'].nunique()}")
        print(f"  - Industries: {final_df['Industry'].nunique()}")
    else:
        print("[ERROR] No data fetched successfully")
    
    print("\n" + "=" * 50)
    print("Done!")
    print("=" * 50)


if __name__ == "__main__":
    main()
