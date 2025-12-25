from typing import Optional
import pandas as pd
import yfinance as yf
import numpy as np

class MarketDataFetcher:
    """
    負責從外部數據源（目前為 Yahoo Finance）獲取金融市場數據的模組。
    設計目標為提供標準化、清洗過的數據，以供下游資料庫儲存與分析使用。
    """

    def fetch_us_daily_close(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """
        抓取指定美股代碼的每日收盤與成交量數據。

        Process Flow:
            1. 下載數據 (yfinance)
            2. 驗證數據完整性 (是否為空)
            3. 標準化欄位名稱 (snake_case)
            4. 清洗時間索引 (移除時區)
            5. 強制轉型 (Float64/Int64)

        Args:
            symbol (str): 股票代碼 (e.g., "AAPL", "NVDA")
            start_date (str): 開始日期 YYYY-MM-DD
            end_date (str): 結束日期 YYYY-MM-DD

        Returns:
            Optional[pd.DataFrame]: 
                - 成功: 返回清洗後的 DataFrame，包含 Index(Date) 與 ['close', 'adj_close', 'volume']
                - 失敗或無數據: 返回 None
        """
        print(f"[Fetcher] Downloading {symbol} from {start_date} to {end_date}...")
        
        try:
            # Handle special tickers (e.g. BRK.B -> BRK-B)
            fetch_symbol = symbol.replace('.', '-')
            
            # 1. 下載數據
            df = yf.download(
                fetch_symbol, 
                start=start_date, 
                end=end_date, 
                interval='1d', 
                auto_adjust=False, 
                progress=False,
                multi_level_index=False 
            )

            # 2. 驗證數據
            if df.empty:
                print(f"[Fetcher] Warning: No data found for {symbol}")
                return None

            # 3. 欄位映射 (確保包含必要欄位)
            required_cols = ['Close', 'Adj Close', 'Volume']
            if not set(required_cols).issubset(df.columns):
                print(f"[Fetcher] Warning: Missing required columns for {symbol}")
                return None

            df = df[required_cols].copy()
            df.columns = ['close', 'adj_close', 'volume'] # snake_case

            # 4. 時區處理 (標準化 UTC+0)
            if df.index.tz is not None:
                df.index = df.index.tz_localize(None)
            
            df.index.name = 'market_date'

            # 5. 型別轉換 (保留 float64 完整精度)
            df['close'] = df['close'].astype('float64')
            df['adj_close'] = df['adj_close'].astype('float64')
            
            # Volume 處理
            df['volume'] = df['volume'].fillna(0).astype('int64')

            print(f"[Fetcher] Successfully fetched {len(df)} rows for {symbol}")
            return df

        except Exception as e:
            print(f"[Fetcher] Error fetching {symbol}: {e}")
            return None

    def get_ticker_info(self, symbol: str) -> dict:
        """
        Fetches fundamentals for a ticker (Market Cap, Revenue, PE, etc.)
        Note: Accessing .info is slower but necessary for these metrics.
        """
        try:
            # Handle special tickers (e.g. BRK.B -> BRK-B)
            fetch_symbol = symbol.replace('.', '-')
            ticker = yf.Ticker(fetch_symbol)
            # We need standard .info for these fields
            info = ticker.info
            
            return {
                'company_name': info.get('shortName'),
                'market_cap': info.get('marketCap'),
                'revenue': info.get('totalRevenue'),
                'gross_profit': info.get('grossProfits'),
                'net_income': info.get('netIncomeToCommon'),
                'pe_ratio': info.get('trailingPE'),
                'profit_margin': info.get('profitMargins'),
                # 'dividend_yield': info.get('dividendYield'), # User requested to exclude this
                'currency': info.get('currency', 'USD')
            }
        except Exception as e:
            print(f"[Fetcher] Warning: Could not fetch info for {symbol}: {e}")
            return {}

# 簡單測試
if __name__ == "__main__":
    fetcher = MarketDataFetcher()
    
    # Test Data Fetch
    df = fetcher.fetch_us_daily_close("AAPL", "2023-01-01", "2023-01-10")
    if df is not None:
        print("\nFetch Result:")
        print(df.head())
    
    # Test Info Fetch
    print("\nInfo Result:")
    print(fetcher.get_ticker_info("AAPL"))
    
    # 測試參數
    test_symbol = "AAPL"
    test_start = "2023-10-01"
    test_end = "2023-11-01"

    print(f"\n--- Testing: {test_symbol} ---")
    df_result = fetcher.fetch_us_daily_close(test_symbol, test_start, test_end)

    if df_result is not None:
        print("\n[Result DataFrame Head]:")
        print(df_result.head())
        
        print("\n[Data Types]:")
        print(df_result.dtypes)
        
        print("\n[Index Attributes]:")
        print(f"Index Name: {df_result.index.name}")
        print(f"Has Timezone: {df_result.index.tz is not None}")
        
        # 驗證欄位名稱
        expected_cols = {'close', 'adj_close', 'volume'}
        assert set(df_result.columns) == expected_cols, f"Column mismatch! Got {df_result.columns}"
        
        # 驗證時區
        assert df_result.index.tz is None, "Timezone was not removed!"
        
        print("\n[TEST] Test Passed: Data format is strictly compliant.")
    else:
        print("\n[TEST] Test Failed: No data returned.")
