import mplfinance as mpf
from market_data_db import MarketDataDB
from sqlalchemy import text
import pandas as pd
import os
from tqdm import tqdm

def batch_plot(target_industry: str = None):
    """
    Generates plots for tickers.
    If target_industry is provided, creates a folder named "{Industry}_plot" and plots only tickers in that industry.
    Otherwise, plots all tickers to "plots" folder.
    """
    
    # 1. Output Directory Strategy
    if target_industry:
        output_dir = f"{target_industry}_plot"
    else:
        output_dir = "plots"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created directory: {output_dir}")
    else:
        print(f"Using directory: {output_dir}")

    db = MarketDataDB()
    
    # 2. Get Tickers (Filtered)
    print(f"Fetching list of tickers (Industry: {target_industry or 'ALL'})...")
    
    with db.engine.connect() as conn:
        if target_industry:
            # Join with industries table to filter
            query = text("""
                SELECT t.ticker 
                FROM tickers t
                JOIN industries i ON t.industry_id = i.id
                WHERE i.name = :industry
            """)
            tickers = pd.read_sql(query, conn, params={"industry": target_industry})['ticker'].tolist()
        else:
            # Fallback: All tickers
            query = text("SELECT DISTINCT ticker FROM tickers")
            tickers = pd.read_sql(query, conn)['ticker'].tolist()
    
    if not tickers:
        print(f"[Warning] No tickers found for industry: {target_industry}")
        return

    print(f"Found {len(tickers)} tickers to visualize.")
    
    # 3. Batch Loop
    pbar = tqdm(tickers, desc="Generating Charts")
    for ticker in pbar:
        try:
            pbar.set_postfix({'ticker': ticker})
            _generate_single_plot(db, ticker, output_dir)
        except Exception as e:
            print(f"Failed to plot {ticker}: {e}")

    print(f"\n[SUCCESS] All plots saved to '{output_dir}/'")

def _generate_single_plot(db, ticker, output_dir):
    # Fetch Data
    with db.engine.connect() as conn:
        query = text("""
            SELECT date, close, volume
            FROM us_daily_prices
            WHERE symbol = :ticker
            ORDER BY date ASC
        """)
        df = pd.read_sql(query, conn, params={"ticker": ticker})
    
    if df.empty:
        return

    # Prepare Data for mplfinance
    df['Date'] = pd.to_datetime(df['date'])
    df.set_index('Date', inplace=True)
    
    # Rename for mpf
    df.rename(columns={'close': 'Close', 'volume': 'Volume'}, inplace=True)
    
    # Fake OHLC for visualization (Line Chart Mode)
    df['Open'] = df['Close']
    df['High'] = df['Close']
    df['Low'] = df['Close']

    # Style
    style = mpf.make_mpf_style(base_mpf_style='nightclouds', rc={'font.size': 10})
    
    output_file = os.path.join(output_dir, f"{ticker}.png")
    
    # Plot
    mpf.plot(
        df, 
        type='line',
        volume=True,
        mav=(7, 20, 50),
        title=f"\n{ticker}",
        style=style,
        tight_layout=True,
        savefig=output_file
    )

if __name__ == "__main__":
    # Change this to target different industries
    TARGET_INDUSTRY = "Aluminum"
    batch_plot(TARGET_INDUSTRY)
