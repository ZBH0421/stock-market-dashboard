import yfinance as yf
import json

def check_metrics(symbol: str):
    print(f"Fetching metrics for {symbol}...")
    try:
        ticker = yf.Ticker(symbol)
        
        # 1. Try fast_info first (faster, sometimes limited)
        print("\n--- fast_info ---")
        try:
            for key, val in ticker.fast_info.items():
                if key in ['market_cap', 'last_price']: # Just print a few sample
                    print(f"{key}: {val}")
        except:
            print("No fast_info")

        # 2. Try .info (richer data, standard source for fundamentals)
        print("\n--- .info (Selected Fields) ---")
        info = ticker.info
        
        # Target metrics
        targets = [
            'totalRevenue', 'revenuePerShare',
            'grossProfits', 'profitMargins', 'netIncomeToCommon', # Profits related
            'trailingPE', 'forwardPE', # PE Ratio
            'dividendYield', 'dividendRate' # Dividend
        ]
        
        found_data = {}
        for key in targets:
            found_data[key] = info.get(key, 'N/A')
            
        print(json.dumps(found_data, indent=2))
        
        # Check standard names usually used mapping
        # Revenue -> totalRevenue
        # Profits -> grossProfits or netIncomeToCommon? Usually refer to Net Income or Gross.
        # PE -> trailingPE
        # Margin -> profitMargins
        # Dividend -> dividendYield

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_metrics("DAL")
