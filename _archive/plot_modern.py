import mplfinance as mpf
from market_data_db import MarketDataDB
from sqlalchemy import text
import pandas as pd
import os

def plot_modern(ticker: str):
    """
    Generates a professional financial chart using mplfinance.
    Style: Candlestick, Volume, Moving Averages, Dark Theme.
    """
    print(f"Creating modern chart for {ticker}...")
    
    try:
        db = MarketDataDB()
        
        # 1. Fetch Data (Need Open, High, Low, Close, Volume)
        # Note: Our current DB schema only has 'close', 'adj_close', 'volume'.
        # Since we don't have true Open/High/Low stored, we will simulate them for visualization
        # OR better, stick to line chart if OHLC is missing?
        # WAIT: Fetcher uses `yf.download`. Let's check if we save OHLC.
        # Checking schema... `us_daily_prices` table: symbol, date, close, adj_close, volume.
        # We DO NOT have Open, High, Low in the DB. This limits true candlestick plotting.
        # However, user wants "modern". We can plot a high-quality Line/Area chart with Volume.
        # OR we can update schema to store OHLC in future. 
        # For now, to fulfill request immediately without re-fetching everything, 
        # I will use a high-quality Area/Line chart with Volume and MAs which still looks very pro.
        
        with db.engine.connect() as conn:
            query = text("""
                SELECT date, close, volume 
                FROM us_daily_prices 
                WHERE symbol = :ticker 
                ORDER BY date ASC
            """)
            df = pd.read_sql(query, conn, params={"ticker": ticker})
            
        if df.empty:
            print(f"No data for {ticker}")
            return
            
        # 2. Prepare Data for mplfinance
        df['Date'] = pd.to_datetime(df['date'])
        df.set_index('Date', inplace=True)
        # mpf expects columns: 'Open', 'High', 'Low', 'Close', 'Volume'
        # We only have Close. We can fake OHLC = Close for visualization logic 
        # but that renders flat candles. 
        # Standard solution with limited data: Use type='line' or 'renko' if possible.
        # Let's use a nice filled line (Area) chart which looks very modern.
        
        # Rename for mpf
        df.rename(columns={'close': 'Close', 'volume': 'Volume'}, inplace=True)
        # Fake Open/High/Low to satisfy library validation even if plotting line
        df['Open'] = df['Close']
        df['High'] = df['Close']
        df['Low'] = df['Close']

        # 3. Create Plot
        # Style: 'nightclouds', 'yahoo', 'charles'
        style = mpf.make_mpf_style(base_mpf_style='nightclouds', rc={'font.size': 10})
        
        output_file = f"{ticker}_modern.png"
        
        mpf.plot(
            df, 
            type='line',           # Line is safer since we lack OHLC
            volume=True,           # Add volume subplot
            mav=(7, 20, 50),       # Moving Averages: 7, 20, 50
            title=f"\n{ticker} Price Analysis",
            style=style,
            tight_layout=True,
            savefig=output_file
        )
        
        print(f"[SUCCESS] Modern chart saved to: {output_file}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    plot_modern("DAL")
