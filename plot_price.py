import matplotlib.pyplot as plt
from market_data_db import MarketDataDB
from sqlalchemy import text
import pandas as pd

def plot_price(ticker):
    print(f"Generating price plot for {ticker}...")
    try:
        db = MarketDataDB()
        
        # 1. Fetch Data
        with db.engine.connect() as conn:
            query = text("""
                SELECT date, close
                FROM us_daily_prices
                WHERE symbol = :ticker
                ORDER BY date ASC
            """)
            df = pd.read_sql(query, conn, params={"ticker": ticker})
        
        if df.empty:
            print(f"No data found for {ticker}")
            return

        # 2. Plot
        plt.figure(figsize=(12, 6))
        
        # Calculate Smoothing (7-Day Moving Average)
        df['ma7'] = df['close'].rolling(window=7).mean()
        
        # Plot Raw Price (Faint)
        plt.plot(df['date'], df['close'], label='Daily Close (Raw)', color='gray', alpha=0.3, linewidth=1)
        
        # Plot Smoothed Price (Prominent)
        plt.plot(df['date'], df['ma7'], label='7-Day Moving Average', color='#1f77b4', linewidth=2.5)
        
        plt.title(f'{ticker} Stock Price - Smoothed (7-Day MA)', fontsize=14)
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('Price (USD)', fontsize=12)
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.legend()
        plt.xticks(rotation=45)
        
        # 3. Save
        output_file = f"{ticker.lower()}_price_plot.png"
        plt.tight_layout()
        plt.savefig(output_file)
        print(f"\n[SUCCESS] Plot saved to: {output_file}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    plot_price("DAL")
