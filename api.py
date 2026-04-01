from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from market_data_db import MarketDataDB
from sqlalchemy import text
import pandas as pd
import math
import uvicorn
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

PERIOD_MAP = {
    "1D": 1,
    "1W": 7,
    "1M": 30,
    "3M": 90,
    "6M": 180,
    "1Y": 365,
    "5Y": 1825,
}

# Initialize FastAPI app
app = FastAPI(
    title="Market Data API",
    description="API for serving real-time stock market data.",
    version="1.0.0"
)

# Enable CORS (Cross-Origin Resource Sharing)
# Production: Set ALLOWED_ORIGINS="https://your-frontend.com,https://another-domain.com"
origins_raw = os.getenv("ALLOWED_ORIGINS", "*")
origins = [origin.strip() for origin in origins_raw.split(",")]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Database
db = MarketDataDB()

@app.get("/")
def read_root():
    return {"status": "ok", "message": "Market Data API is running"}

@app.get("/health")
def health_check():
    """Health check endpoint for Render/Kubernetes probes"""
    return {"status": "healthy"}

@app.get("/api/industries")
def get_industries():
    try:
        with db.engine.connect() as conn:
            query = text("SELECT DISTINCT name FROM industries ORDER BY name")
            res = conn.execute(query).fetchall()
            return {"industries": [r[0] for r in res]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/industry/{industry_name}")
def get_industry_data(industry_name: str):
    from urllib.parse import unquote
    industry_name = unquote(industry_name)
    try:
        with db.engine.connect() as conn:
            # 1. Get Industry ID (Try exact match first, then case-insensitive)
            ind_query = text("SELECT id, name FROM industries WHERE name = :name")
            res = conn.execute(ind_query, {"name": industry_name}).fetchone()
            
            if not res:
                # Case-insensitive fallback
                ind_query_ci = text("SELECT id, name FROM industries WHERE LOWER(name) = LOWER(:name)")
                res = conn.execute(ind_query_ci, {"name": industry_name}).fetchone()
                
            if not res:
                raise HTTPException(status_code=404, detail="Industry not found")
            
            ind_id = res[0]
            industry_display_name = res[1]

            # 2. Get Tickers
            t_query = text("""
                SELECT ticker, company_name, market_cap, pe_ratio, revenue
                FROM tickers WHERE industry_id = :iid ORDER BY market_cap DESC NULLS LAST
            """)
            tickers_df = pd.read_sql(t_query, conn, params={"iid": ind_id})
            if tickers_df.empty:
                return {"industry": industry_display_name, "stocks": [], "ticker_count": 0, "donut_data": {"series":[], "labels":[]}, "total_market_cap": 0}

            tickers_list = tickers_df['ticker'].tolist()
            
            # 3. Fetch Prices
            p_query = text("""
                SELECT symbol as ticker, date as market_date, close as close_price, volume
                FROM us_daily_prices WHERE symbol IN :tickers 
                AND date >= CURRENT_DATE - INTERVAL '730 days'
                ORDER BY market_date ASC
            """)
            prices_df = pd.read_sql(p_query, conn, params={"tickers": tuple(tickers_list)})
            if prices_df.empty:
                 return {"industry": industry_display_name, "stocks": [], "ticker_count": len(tickers_list), "donut_data": {"series":[], "labels":[]}, "total_market_cap": 0}

            prices_df['market_date'] = pd.to_datetime(prices_df['market_date'])
            
            # --- Vectorized Performance Calculations ---
            # Pre-calculate latest prices and volume for all tickers
            ticker_groups = prices_df.groupby('ticker')
            latest_prices = prices_df.sort_values('market_date').drop_duplicates('ticker', keep='last')
            ticker_to_latest = latest_prices.set_index('ticker')

            def get_bulk_pct_changes(months=0, days=0):
                offsets = pd.DateOffset(months=months) if months > 0 else pd.DateOffset(days=days)
                t_dates = latest_prices[['ticker', 'market_date']].copy()
                t_dates['target'] = t_dates['market_date'] - offsets
                
                sp = pd.merge_asof(
                    t_dates.sort_values('target'),
                    prices_df.sort_values('market_date'),
                    left_on='target', right_on='market_date', by='ticker', direction='backward'
                )
                m = pd.merge(latest_prices[['ticker', 'close_price']], sp[['ticker', 'close_price']], on='ticker', suffixes=('_now', '_start'))
                m['change'] = ((m['close_price_now'] - m['close_price_start']) / m['close_price_start']) * 100
                return m.set_index('ticker')['change'].to_dict()

            c_1d = get_bulk_pct_changes(days=1)
            c_1m = get_bulk_pct_changes(months=1)
            c_2m = get_bulk_pct_changes(months=2)
            c_3m = get_bulk_pct_changes(months=3)
            c_6m = get_bulk_pct_changes(months=6)
            c_12m = get_bulk_pct_changes(months=12)

            cur_yr = pd.Timestamp.now().year
            y_starts = prices_df[prices_df['market_date'].dt.year == cur_yr].sort_values('market_date').drop_duplicates('ticker', keep='first')
            m_ytd = pd.merge(latest_prices[['ticker', 'close_price']], y_starts[['ticker', 'close_price']], on='ticker', suffixes=('_now', '_start'))
            m_ytd['change'] = ((m_ytd['close_price_now'] - m_ytd['close_price_start']) / m_ytd['close_price_start']) * 100
            c_ytd = m_ytd.set_index('ticker')['change'].to_dict()

            # --- Result Assembly ---
            def sf(v):
                if pd.isna(v) or v is None: return None
                return float(v) if math.isfinite(v) else None

            def si(v):
                if pd.isna(v) or v is None: return 0
                try:
                    return int(float(v))
                except: return 0

            result_data = []
            for _, row in tickers_df.iterrows():
                t = row['ticker']
                hist = []
                if t in ticker_groups.groups:
                    group = ticker_groups.get_group(t).tail(1000)
                    hist = [{"x": r['market_date'].strftime('%Y-%m-%d'), "y": sf(r['close_price'])} for _, r in group.iterrows()]

                result_data.append({
                    "symbol": t,
                    "company": row['company_name'] or t,
                    "price": sf(ticker_to_latest.at[t, 'close_price'] if t in ticker_to_latest.index else None),
                    "market_cap": si(row['market_cap']),
                    "pe_ratio": sf(row['pe_ratio']),
                    "volume": si(ticker_to_latest.at[t, 'volume'] if t in ticker_to_latest.index else 0),
                    "revenue": si(row['revenue']),
                    "change_1d": sf(c_1d.get(t)),
                    "change_1m": sf(c_1m.get(t)),
                    "change_2m": sf(c_2m.get(t)),
                    "change_3m": sf(c_3m.get(t)),
                    "change_6m": sf(c_6m.get(t)),
                    "change_12m": sf(c_12m.get(t)),
                    "change_ytd": sf(c_ytd.get(t)),
                    "history": hist
                })

            top_5 = tickers_df.head(5)
            others_mcap = tickers_df.iloc[5:]['market_cap'].sum() if len(tickers_df) > 5 else 0
            donut_series = [sf(x) or 0.0 for x in top_5['market_cap'].tolist()]
            donut_labels = top_5['ticker'].tolist()
            if others_mcap > 0:
                donut_series.append(sf(others_mcap))
                donut_labels.append("Others")

            return {
                "industry": industry_display_name,
                "total_market_cap": sf(tickers_df['market_cap'].sum()),
                "ticker_count": len(tickers_list),
                "donut_data": {"series": donut_series, "labels": donut_labels},
                "stocks": result_data
            }
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/market-flow")
def get_market_flow(days: int = 5):
    """
    Calculates estimated 'Money Flow' (Net Dollar Volume) into industries.
    Logic: Sum(Volume * PriceChange) over the last N days.
    Returns: List of industries sorted by net flow.
    """
    try:
        with db.engine.connect() as conn:
            # 1. Get Tickers and Industries
            tickers_df = pd.read_sql(text("SELECT t.ticker, i.name as industry FROM tickers t JOIN industries i ON t.industry_id = i.id"), conn)
            
            # 2. Get Recent Prices (Fetch extra days to ensure we have previous close for the first day of the period)
            query = text(f"""
                SELECT symbol, date, close, volume 
                FROM us_daily_prices 
                WHERE date >= CURRENT_DATE - INTERVAL '{days + 5} days'
                ORDER BY symbol, date ASC
            """)
            prices_df = pd.read_sql(query, conn)
            
            if prices_df.empty:
                return {"items": []}

            # 3. Calculate Daily Change & Money Flow
            # Ensure sorting
            prices_df = prices_df.sort_values(['symbol', 'date'])
            prices_df['prev_close'] = prices_df.groupby('symbol')['close'].shift(1)
            prices_df['price_change'] = prices_df['close'] - prices_df['prev_close']
            prices_df['pct_change'] = (prices_df['price_change'] / prices_df['prev_close']) * 100
            
            # Net Flow = Volume * Price Change
            prices_df['net_flow'] = prices_df['volume'] * prices_df['price_change']
            
            # 4. Filter for the requested period (last N days)
            # Find the max date in the DB
            max_date = prices_df['date'].max()
            start_cutoff = max_date - pd.Timedelta(days=days-1) # Inclusive of max_date
            
            period_df = prices_df[prices_df['date'] >= start_cutoff].copy()
            
            # 5. Merge Industry
            merged = period_df.merge(tickers_df, left_on='symbol', right_on='ticker')
            
            # 6. Aggregations
            # Group by Industry
            grouped = merged.groupby('industry').agg({
                'net_flow': 'sum',
                'volume': 'sum',
                'pct_change': 'mean' # Average movement of stocks in industry
            }).reset_index()
            
            # Sort by absolute flow magnitude or net flow? Let's sort by Net Flow descending
            grouped = grouped.sort_values('net_flow', ascending=False)
            
            # Format results
            result = []
            for _, row in grouped.iterrows():
                result.append({
                    "industry": row['industry'],
                    "net_flow": float(row['net_flow']),
                    "total_volume": int(row['volume']),
                    "avg_pct_change": float(row['pct_change']) if pd.notna(row['pct_change']) else 0.0
                })
                
            return {
                "period_days": days,
                "end_date": str(max_date.date()) if pd.notna(max_date) else None,
                "items": result
            }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stock/{ticker}")
def get_stock_details(ticker: str):
    """
    Fetches detailed information for a specific stock.
    Includes: Fundamentals, Price History, and News (via YFinance).
    """
    try:
        ticker = ticker.upper()
        
        # 1. Fetch from Database
        with db.engine.connect() as conn:
            # Fundamentals
            t_query = text("""
                SELECT company_name, market_cap, pe_ratio, revenue, 
                       gross_profit, net_income, profit_margin, dividend_yield
                FROM tickers 
                WHERE ticker = :ticker
            """)
            t_res = conn.execute(t_query, {"ticker": ticker}).fetchone()
            
            # Price History (Last 2 Years)
            p_query = text("""
                SELECT date, close, volume 
                FROM us_daily_prices 
                WHERE symbol = :ticker 
                AND date >= CURRENT_DATE - INTERVAL '730 days'
                ORDER BY date ASC
            """)
            p_df = pd.read_sql(p_query, conn, params={"ticker": ticker})
            
        if not t_res and p_df.empty:
            raise HTTPException(status_code=404, detail="Stock not found")
            
        # 2. Fetch News (Live from Yahoo Finance)
        news_items = []
        # try:
        #     import yfinance as yf
        #     bot = yf.Ticker(ticker)
        #     raw_news = bot.news
        #     for n in raw_news:
        #         # Different YF versions have different keys, handle gracefully
        #         news_items.append({
        #             "title": n.get('title'),
        #             "link": n.get('link'),
        #             "publisher": n.get('publisher'),
        #             "date": n.get('providerPublishTime') # Timestamp
        #         })
        # except Exception as e:
        #     print(f"[Warning] Failed to fetch news for {ticker}: {e}")
            
        # 3. Assemble Response
        
        # Helper to safely cast numpy/pandas types
        def safe_float(val):
            if val is None or pd.isna(val): return None
            return float(val)

        def safe_int(val):
            if val is None or pd.isna(val): return None
            return int(val)

        # Calculate latest price and change
        latest_price = None
        change_1d = None
        change_pct = None
        
        history = []
        if not p_df.empty:
            p_df['date_str'] = p_df['date'].astype(str)
            # Convert to list of dicts first to avoid numpy types in iteration if possible, 
            # but manually building is safer for serialization
            hist_records = p_df[['date_str', 'close', 'volume']].to_dict(orient='records')
            history = [{"x": r['date_str'], "y": safe_float(r['close']), "v": safe_int(r['volume'])} for r in hist_records]
            
            latest_row = p_df.iloc[-1]
            latest_price = safe_float(latest_row['close'])
            
            if len(p_df) > 1:
                prev_row = p_df.iloc[-2]
                change_1d = latest_price - safe_float(prev_row['close'])
                change_pct = (change_1d / safe_float(prev_row['close'])) * 100

        info = {
            "symbol": ticker,
            "company": t_res[0] if t_res else ticker,
            "price": latest_price,
            "change": change_1d,
            "change_percent": change_pct,
            "market_cap": safe_int(t_res[1]) if t_res else None,
            "pe_ratio": safe_float(t_res[2]) if t_res else None,
            "revenue": safe_int(t_res[3]) if t_res else None,
            "volume": safe_int(latest_row['volume']) if not p_df.empty else None, 
            "gross_profit": safe_int(t_res[4]) if t_res else None,
            "net_income": safe_int(t_res[5]) if t_res else None,
            "profit_margin": safe_float(t_res[6]) if t_res else None,
            "dividend_yield": safe_float(t_res[7]) if t_res else None
        }

        return {
            "info": info,
            "history": history,
            "news": news_items
        }

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/galaxy")
def get_galaxy_data():
    """
    Fetches bulk data for the 3D Galaxy visualization.
    Returns: List of {symbol, industry, pe, revenue, market_cap, performance_1y}
    """
    print(f"--- [CHECKPOINT] /api/galaxy CALLED at {pd.Timestamp.now()} ---")
    try:
        print("--- [CHECKPOINT] Connecting to Database... ---")
        with db.engine.connect() as conn:
            print("--- [CHECKPOINT] DB Connected. Executing Fundamentals Query... ---")
            # 1. Fetch Fundamental Data (PE, Rev, Cap) from Tickers table
            # We filter outstocks with no Market Cap or unreasonable values to keep the chart clean
            query_fundamentals = text("""
                SELECT t.ticker, t.company_name, i.name as industry, 
                       t.pe_ratio, t.revenue, t.market_cap
                FROM tickers t
                JOIN industries i ON t.industry_id = i.id
                WHERE t.market_cap IS NOT NULL 
                  AND t.market_cap > 0
            """)
            funds_df = pd.read_sql(query_fundamentals, conn)
            
            if funds_df.empty:
                 return {"timestamp": str(pd.Timestamp.now()), "count": 0, "stars": []}

            # 2. Approximate 12M Performance (Optional, but adds nice 'Size' dimension)
            # To be fast, we only get 'Current Close' and 'Close 1 Year Ago'
            # Using a simplified approach: Get latest price vs price 365 days ago
            
            # Subquery to get latest date and date ~1 year ago
            # For speed in prototype, we might skip precise 12M perf if it's too heavy.
            # Let's try to get it.
            
            perf_query = text("""
                WITH latest_prices AS (
                    SELECT symbol, close as price_now 
                    FROM us_daily_prices 
                    WHERE date = (SELECT MAX(date) FROM us_daily_prices)
                ),
                old_prices AS (
                    SELECT symbol, close as price_old
                    FROM us_daily_prices 
                    WHERE date >= (CURRENT_DATE - INTERVAL '370 days') 
                      AND date <= (CURRENT_DATE - INTERVAL '360 days')
                )
                -- We deduplicate old_prices to get just one entry per symbol (the first one found in range)
                SELECT l.symbol, l.price_now, 
                       (SELECT price_old FROM old_prices o WHERE o.symbol = l.symbol LIMIT 1) as price_old
                FROM latest_prices l
            """)
            
            perf_df = pd.read_sql(perf_query, conn)
            
            # Calculate % Change
            perf_df['change_1y'] = ((perf_df['price_now'] - perf_df['price_old']) / perf_df['price_old']) * 100
            
            # 3. Merge
            merged = funds_df.merge(perf_df[['symbol', 'change_1y']], left_on='ticker', right_on='symbol', how='left')
            
            # Fill NaN
            merged['change_1y'] = merged['change_1y'].fillna(0)
            merged['pe_ratio'] = merged['pe_ratio'].fillna(0)
            merged['revenue'] = merged['revenue'].fillna(0)
            
            # Data Cleaning for Plotly
            # 1. Filter out Hyper-Inflation/Non-USD Currencies (Revenue > 1 Trillion)
            #    Toyota (TM), Honda (HMC), etc report in JPY. Walmart is ~0.6T USD. 
            #    So >1T is a safe cutoff for now.
            merged = merged[merged['revenue'] < 1_000_000_000_000] 
            
            # 2. Filter out Negative/Zero Revenue (Breaks Log Scale)
            merged = merged[merged['revenue'] > 0]
            
            # 3. Filter PE Outliers
            merged = merged[merged['pe_ratio'] < 500] 
            merged = merged[merged['pe_ratio'] > -500] 

            # 4. Filter Performance Anomalies (The "Big Spheres")
            # Exclude gains > 2000% (20x) to remove penny stock glitches
            merged = merged[merged['change_1y'] < 2000]

            stars = []
            for _, row in merged.iterrows():
                stars.append({
                    "symbol": row['ticker'],
                    "industry": row['industry'],
                    "company": row['company_name'],
                    "x_pe": float(row['pe_ratio']),
                    "y_rev": float(row['revenue']),
                    "z_cap": float(row['market_cap']),
                    "color_group": row['industry'], # For Plotly categorization
                    "size_perf": float(row['change_1y'])
                })
                
            return {
                "timestamp": str(pd.Timestamp.now()),
                "count": len(stars),
                "stars": stars
            }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/sector-pe-history")
def get_sector_pe_history(days: int = 365):
    """
    Returns market-cap-weighted trailing PE per sector per day.
    Uses stored trailingEps to compute historical PE from daily close prices.
    Filters: trailing_eps > 0, close > 0, market_cap > 0, pe between 0 and 200.
    """
    try:
        with db.engine.connect() as conn:
            query = text(f"""
                SELECT
                    i.name AS industry,
                    p.date,
                    SUM((p.close / t.trailing_eps) * p.market_cap) / SUM(p.market_cap) AS weighted_pe
                FROM us_daily_prices p
                JOIN tickers t ON p.symbol = t.ticker
                JOIN industries i ON t.industry_id = i.id
                WHERE
                    t.trailing_eps > 0
                    AND p.close > 0
                    AND p.market_cap > 0
                    AND (p.close / t.trailing_eps) BETWEEN 0 AND 200
                    AND p.date >= CURRENT_DATE - INTERVAL '{days} days'
                GROUP BY i.name, p.date
                ORDER BY i.name, p.date
            """)
            rows = conn.execute(query).fetchall()

        if not rows:
            return {"sectors": []}

        from collections import defaultdict
        sector_data = defaultdict(list)
        for row in rows:
            industry, date, pe = row
            if pe is not None:
                sector_data[industry].append({
                    "date": str(date),
                    "pe": round(float(pe), 2)
                })

        sectors = [
            {"name": name, "data": points}
            for name, points in sorted(sector_data.items())
            if len(points) >= 5
        ]

        return {"sectors": sectors}

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/gics-overview")
def get_gics_overview(period: str = "1D"):
    if period not in PERIOD_MAP:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid period '{period}'. Must be one of: {list(PERIOD_MAP.keys())}"
        )

    days = PERIOD_MAP[period]

    try:
        with db.engine.connect() as conn:
            query = text(f"""
                WITH
                end_date AS (
                    SELECT MAX(date) AS d FROM us_daily_prices
                ),
                current_prices AS (
                    SELECT p.symbol, p.close AS current_close, p.market_cap
                    FROM us_daily_prices p, end_date
                    WHERE p.date = end_date.d
                      AND p.close > 0
                      AND p.market_cap > 0
                ),
                start_prices AS (
                    SELECT DISTINCT ON (p.symbol)
                           p.symbol, p.close AS start_close
                    FROM us_daily_prices p, end_date
                    WHERE p.date <= end_date.d - INTERVAL '1 day' * {days}
                      AND p.close > 0
                    ORDER BY p.symbol, p.date DESC
                )
                SELECT
                    i.name AS industry,
                    COUNT(DISTINCT c.symbol) AS stock_count,
                    SUM((c.current_close - s.start_close) / s.start_close * c.market_cap)
                        / NULLIF(SUM(c.market_cap), 0) * 100 AS pct_change,
                    SUM(
                        CASE WHEN t.trailing_eps > 0
                             AND (c.current_close / t.trailing_eps) BETWEEN 0 AND 200
                        THEN (c.current_close / t.trailing_eps) * c.market_cap
                        END
                    ) / NULLIF(SUM(
                        CASE WHEN t.trailing_eps > 0
                             AND (c.current_close / t.trailing_eps) BETWEEN 0 AND 200
                        THEN c.market_cap END
                    ), 0) AS avg_pe
                FROM current_prices c
                JOIN start_prices s ON c.symbol = s.symbol
                JOIN tickers t ON c.symbol = t.ticker
                JOIN industries i ON t.industry_id = i.id
                GROUP BY i.name
                ORDER BY i.name
            """)

            rows = conn.execute(query).fetchall()

        items = []
        for row in rows:
            industry, stock_count, pct_change, avg_pe = row
            items.append({
                "industry": industry,
                "stock_count": int(stock_count) if stock_count is not None else 0,
                "pct_change": round(float(pct_change), 4) if pct_change is not None else None,
                "avg_pe": round(float(avg_pe), 2) if avg_pe is not None else None,
            })

        return {"period": period, "items": items}

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
