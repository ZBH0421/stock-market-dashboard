import mplfinance as mpf
from market_data_db import MarketDataDB
from sqlalchemy import text
import pandas as pd
import os

def plot_styles_showcase(ticker: str):
    """
    Generates the same chart in 5 different styles for comparison.
    """
    print(f"Generating style showcase for {ticker}...")
    
    try:
        db = MarketDataDB()
        
        # 1. Fetch Data
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
            
        # 2. Prepare Data
        df['Date'] = pd.to_datetime(df['date'])
        df.set_index('Date', inplace=True)
        df.rename(columns={'close': 'Close', 'volume': 'Volume'}, inplace=True)
        # Fake OHLC for Line chart
        df['Open'] = df['Close']
        df['High'] = df['Close']
        df['Low'] = df['Close']

        # 3. Define Styles to Showcase (5 types)
        styles_to_try = [
            'charles',      # 1. Classic Green/Red candlesticks on white
            'yahoo',        # 2. Yahoo Finance style
            'binance',      # 3. Crypto exchange style (Teal/Pink on dark)
            'mike',         # 4. Dark Blue/Black classic
            'classic',      # 5. Old school monochrome
            'blueskies',    # 6. Light and airy
        ]
        
        output_dir = "style_showcase"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        print(f"Saving charts to '{output_dir}/'...")
        
        for style_name in styles_to_try:
            print(f"  - Generating {style_name}...")
            output_file = os.path.join(output_dir, f"{ticker}_{style_name}.png")
            
            # Create Custom Style based on the template
            # Adjusting font size for readability
            my_style = mpf.make_mpf_style(base_mpf_style=style_name, rc={'font.size': 10})
            
            mpf.plot(
                df, 
                type='line',
                volume=True,
                mav=(7, 20, 50),
                title=f"\n{ticker} - Style: {style_name}",
                style=my_style,
                tight_layout=True,
                savefig=output_file
            )
            
        print(f"\n[SUCCESS] Generated {len(styles_to_try)} style examples.")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    plot_styles_showcase("DAL")
