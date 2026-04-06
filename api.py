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
                    SUM((p.close / t.trailing_eps) * t.market_cap) / SUM(t.market_cap) AS weighted_pe
                FROM us_daily_prices p
                JOIN tickers t ON p.symbol = t.ticker
                JOIN industries i ON t.industry_id = i.id
                WHERE
                    t.trailing_eps > 0
                    AND p.close > 0
                    AND t.market_cap > 0
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

@app.get("/api/market-pe")
def get_market_pe():
    """
    Returns the current market-cap-weighted trailing PE across all tickers,
    plus the 52-week high and low of the same metric.
    """
    try:
        with db.engine.connect() as conn:
            # Current market PE (latest trading day)
            current = conn.execute(text("""
                SELECT
                    SUM((p.close / t.trailing_eps) * t.market_cap) / SUM(t.market_cap) AS market_pe
                FROM us_daily_prices p
                JOIN tickers t ON p.symbol = t.ticker
                WHERE
                    t.trailing_eps > 0
                    AND p.close > 0
                    AND t.market_cap > 0
                    AND (p.close / t.trailing_eps) BETWEEN 0 AND 200
                    AND p.date = (SELECT MAX(date) FROM us_daily_prices)
            """)).scalar()

            # 52-week history for range
            history = conn.execute(text("""
                SELECT
                    p.date,
                    SUM((p.close / t.trailing_eps) * t.market_cap) / SUM(t.market_cap) AS market_pe
                FROM us_daily_prices p
                JOIN tickers t ON p.symbol = t.ticker
                WHERE
                    t.trailing_eps > 0
                    AND p.close > 0
                    AND t.market_cap > 0
                    AND (p.close / t.trailing_eps) BETWEEN 0 AND 200
                    AND p.date >= CURRENT_DATE - INTERVAL '365 days'
                GROUP BY p.date
                ORDER BY p.date
            """)).fetchall()

        pe_values = [float(r[1]) for r in history if r[1] is not None]
        return {
            "market_pe": round(float(current), 2) if current else None,
            "year_high": round(max(pe_values), 2) if pe_values else None,
            "year_low": round(min(pe_values), 2) if pe_values else None,
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/shiller-cape")
def get_shiller_cape():
    """
    Returns the latest Shiller CAPE (Cyclically Adjusted PE) from Robert Shiller's
    Yale dataset (ie_data.xls). Cached for 24 hours.
    """
    import urllib.request
    import tempfile
    import os
    import time

    CACHE_FILE = "/tmp/shiller_cape_cache.json"
    CACHE_TTL = 86400  # 24 hours

    # Return cached value if fresh
    if os.path.exists(CACHE_FILE):
        age = time.time() - os.path.getmtime(CACHE_FILE)
        if age < CACHE_TTL:
            with open(CACHE_FILE) as f:
                import json
                return json.load(f)

    try:
        url = "http://www.econ.yale.edu/~shiller/data/ie_data.xls"
        with tempfile.NamedTemporaryFile(suffix=".xls", delete=False) as tmp:
            tmp_path = tmp.name

        urllib.request.urlretrieve(url, tmp_path)

        import xlrd
        wb = xlrd.open_workbook(tmp_path)
        ws = wb.sheet_by_name("Data")
        os.unlink(tmp_path)

        # Column 0 = Date (YYYY.MM decimal), Column 12 = CAPE (P/E10)
        # Header rows occupy rows 0-7; data starts at row 8 (0-indexed: row 7)
        cape_values = []
        for row_idx in range(7, ws.nrows):
            row = ws.row_values(row_idx)
            date_val = row[0] if row else None
            cape_val = row[12] if len(row) > 12 else None
            if date_val and cape_val and cape_val != 'NA':
                try:
                    cape_float = float(cape_val)
                    if 5 < cape_float < 200:
                        cape_values.append((str(date_val), cape_float))
                except (ValueError, TypeError):
                    pass

        if not cape_values:
            raise ValueError("No CAPE data found in spreadsheet")

        latest_date, latest_cape = cape_values[-1]
        import json
        result = {
            "cape": round(latest_cape, 2),
            "date": latest_date,
            "source": "Robert Shiller / Yale (ie_data.xls)"
        }
        with open(CACHE_FILE, "w") as f:
            json.dump(result, f)
        return result

    except Exception as e:
        import traceback
        traceback.print_exc()
        # Return cached stale value if available rather than error
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE) as f:
                import json
                return json.load(f)
        raise HTTPException(status_code=500, detail=f"Could not fetch Shiller CAPE: {e}")


def _safe_float(v, ndigits):
    if v is None:
        return None
    f = float(v)
    if not math.isfinite(f):
        return None
    return round(f, ndigits)

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
                    SELECT p.symbol, p.close AS current_close,
                           COALESCE(NULLIF(p.market_cap, 0), t.market_cap) AS market_cap
                    FROM us_daily_prices p
                    JOIN tickers t ON p.symbol = t.ticker, end_date
                    WHERE p.date = end_date.d
                      AND p.close > 0
                      AND COALESCE(NULLIF(p.market_cap, 0), t.market_cap) > 0
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
                "pct_change": _safe_float(pct_change, 4),
                "avg_pe": _safe_float(avg_pe, 2),
            })

        return {"period": period, "items": items}

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/brief")
def get_brief():
    """
    Returns the daily intelligence brief as JSON.
    Serves cached /tmp/brief_{date}.json if fresh (< 2h).
    Returns 202 with {status: 'generating'} if not yet available.
    """
    import datetime, time, subprocess, sys
    from pathlib import Path

    today = datetime.date.today().isoformat()
    cache = Path(f"/tmp/brief_{today}.json")

    if cache.exists():
        age = time.time() - cache.stat().st_mtime
        if age < 7200:
            import json as _json
            try:
                return _json.loads(cache.read_text())
            except (ValueError, OSError):
                # Bug fix #2: corrupted cache — delete and fall through to regenerate
                cache.unlink(missing_ok=True)

    # Not cached — trigger background generation and return 202
    script = Path(__file__).parent / "generate_brief.py"
    subprocess.Popen(
        [sys.executable, str(script), "--force"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    from fastapi.responses import JSONResponse
    return JSONResponse(status_code=202, content={
        "status": "generating",
        "message": "Brief is being generated, retry in 30 seconds."
    })


@app.post("/api/brief/regenerate")
def regenerate_brief():
    """Force regenerate today's brief (clears cache)."""
    import datetime, subprocess, sys
    from pathlib import Path

    today = datetime.date.today().isoformat()
    cache = Path(f"/tmp/brief_{today}.json")
    if cache.exists():
        cache.unlink()

    script = Path(__file__).parent / "generate_brief.py"
    subprocess.Popen(
        [sys.executable, str(script), "--force"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    return {"status": "generating", "message": "Regenerating brief in background."}


# ---------------------------------------------------------------------------
# GICS sector mapping: industry name -> 11 GICS sector
# ---------------------------------------------------------------------------
_GICS_MAP = {
    "Advertising Agencies": "Communication Services",
    "Aerospace & Defense": "Industrials",
    "Agricultural Inputs": "Materials",
    "Airlines": "Industrials",
    "Airports & Air Services": "Industrials",
    "Aluminum": "Materials",
    "Apparel Manufacturing": "Consumer Discretionary",
    "Apparel Retail": "Consumer Discretionary",
    "Asset Management": "Financials",
    "Auto & Truck Dealerships": "Consumer Discretionary",
    "Auto Manufacturers": "Consumer Discretionary",
    "Auto Parts": "Consumer Discretionary",
    "Banks - Diversified": "Financials",
    "Banks - Regional": "Financials",
    "Beverages - Brewers": "Consumer Staples",
    "Beverages - Non-Alcoholic": "Consumer Staples",
    "Beverages - Wineries & Distilleries": "Consumer Staples",
    "Biotechnology": "Health Care",
    "Broadcasting": "Communication Services",
    "Building Materials": "Industrials",
    "Building Products & Equipment": "Industrials",
    "Business Equipment & Supplies": "Industrials",
    "Capital Markets": "Financials",
    "Chemicals": "Materials",
    "Coking Coal": "Energy",
    "Communication Equipment": "Information Technology",
    "Computer Hardware": "Information Technology",
    "Confectioners": "Consumer Staples",
    "Conglomerates": "Industrials",
    "Consulting Services": "Industrials",
    "Consumer Electronics": "Consumer Discretionary",
    "Copper": "Materials",
    "Credit Services": "Financials",
    "Department Stores": "Consumer Discretionary",
    "Diagnostics & Research": "Health Care",
    "Discount Stores": "Consumer Staples",
    "Drug Manufacturers - General": "Health Care",
    "Drug Manufacturers - Specialty & Generic": "Health Care",
    "Education & Training Services": "Consumer Discretionary",
    "Electrical Equipment & Parts": "Industrials",
    "Electronic Components": "Information Technology",
    "Electronic Gaming & Multimedia": "Communication Services",
    "Electronics & Computer Distribution": "Information Technology",
    "Engineering & Construction": "Industrials",
    "Entertainment": "Communication Services",
    "Farm & Heavy Construction Machinery": "Industrials",
    "Farm Products": "Consumer Staples",
    "Financial Conglomerates": "Financials",
    "Financial Data & Stock Exchanges": "Financials",
    "Food Distribution": "Consumer Staples",
    "Footwear & Accessories": "Consumer Discretionary",
    "Furnishings, Fixtures & Appliances": "Consumer Discretionary",
    "Gambling": "Consumer Discretionary",
    "Gold": "Materials",
    "Grocery Stores": "Consumer Staples",
    "Health Information Services": "Health Care",
    "Healthcare Plans": "Health Care",
    "Home Improvement Retail": "Consumer Discretionary",
    "Household & Personal Products": "Consumer Staples",
    "Industrial Distribution": "Industrials",
    "Information Technology Services": "Information Technology",
    "Infrastructure Operations": "Utilities",
    "Insurance - Diversified": "Financials",
    "Insurance - Life": "Financials",
    "Insurance - Property & Casualty": "Financials",
    "Insurance - Reinsurance": "Financials",
    "Insurance - Specialty": "Financials",
    "Insurance Brokers": "Financials",
    "Integrated Freight & Logistics": "Industrials",
    "Internet Content & Information": "Communication Services",
    "Internet Retail": "Consumer Discretionary",
    "Leisure": "Consumer Discretionary",
    "Lodging": "Consumer Discretionary",
    "Lumber & Wood Production": "Materials",
    "Luxury Goods": "Consumer Discretionary",
    "Marine Shipping": "Industrials",
    "Medical Care Facilities": "Health Care",
    "Medical Devices": "Health Care",
    "Medical Distribution": "Health Care",
    "Medical Instruments & Supplies": "Health Care",
    "Metal Fabrication": "Industrials",
    "Mortgage Finance": "Financials",
    "Oil & Gas Drilling": "Energy",
    "Oil & Gas Equipment & Services": "Energy",
    "Oil & Gas Exploration & Production": "Energy",
    "Oil & Gas Integrated": "Energy",
    "Oil & Gas Midstream": "Energy",
    "Oil & Gas Refining & Marketing": "Energy",
    "Other Industrial Metals & Mining": "Materials",
    "Other Precious Metals & Mining": "Materials",
    "Packaged Foods": "Consumer Staples",
    "Packaging & Containers": "Materials",
    "Paper & Paper Products": "Materials",
    "Personal Services": "Consumer Discretionary",
    "Pharmaceutical Retailers": "Health Care",
    "Pollution & Treatment Controls": "Industrials",
    "Publishing": "Communication Services",
    "REIT - Diversified": "Real Estate",
    "REIT - Healthcare Facilities": "Real Estate",
    "REIT - Hotel & Motel": "Real Estate",
    "REIT - Industrial": "Real Estate",
    "REIT - Mortgage": "Real Estate",
    "REIT - Office": "Real Estate",
    "REIT - Residential": "Real Estate",
    "REIT - Retail": "Real Estate",
    "REIT - Specialty": "Real Estate",
    "Railroads": "Industrials",
    "Real Estate - Development": "Real Estate",
    "Real Estate - Diversified": "Real Estate",
    "Real Estate Services": "Real Estate",
    "Recreational Vehicles": "Consumer Discretionary",
    "Rental & Leasing Services": "Industrials",
    "Residential Construction": "Consumer Discretionary",
    "Resorts & Casinos": "Consumer Discretionary",
    "Restaurants": "Consumer Discretionary",
    "Scientific & Technical Instruments": "Information Technology",
    "Security & Protection Services": "Industrials",
    "Semiconductor Equipment & Materials": "Information Technology",
    "Semiconductors": "Information Technology",
    "Shell Companies": None,
    "Silver": "Materials",
    "Software - Application": "Information Technology",
    "Software - Infrastructure": "Information Technology",
    "Solar": "Utilities",
    "Specialty Business Services": "Industrials",
    "Specialty Chemicals": "Materials",
    "Specialty Industrial Machinery": "Industrials",
    "Specialty Retail": "Consumer Discretionary",
    "Staffing & Employment Services": "Industrials",
    "Steel": "Materials",
    "Telecom Services": "Communication Services",
    "Textile Manufacturing": "Consumer Discretionary",
    "Thermal Coal": "Energy",
    "Tobacco": "Consumer Staples",
    "Tools & Accessories": "Industrials",
    "Travel Services": "Consumer Discretionary",
    "Trucking": "Industrials",
    "Uranium": "Energy",
    "Utilities - Diversified": "Utilities",
    "Utilities - Independent Power Producers": "Utilities",
    "Utilities - Regulated Electric": "Utilities",
    "Utilities - Regulated Gas": "Utilities",
    "Utilities - Regulated Water": "Utilities",
    "Utilities - Renewable": "Utilities",
    "Waste Management": "Industrials",
}


@app.get("/api/sector-rotation")
def get_sector_rotation():
    """
    Returns RS-Ratio and RS-Momentum for 11 GICS sectors for an RRG chart.
    RS-Ratio  = 13W sector return vs market, normalized around 100
    RS-Momentum = 4W sector return vs market, normalized around 100
    Quadrant: Leading (RS>100 & Mom>100), Weakening (RS>100 & Mom<100),
              Lagging (RS<100 & Mom<100), Improving (RS<100 & Mom>100)
    """
    import numpy as np

    try:
        with db.engine.connect() as conn:
            # Pull last 70 trading days of data (need 65 for 13W + buffer)
            query = text("""
                WITH ranked_dates AS (
                    SELECT DISTINCT date
                    FROM us_daily_prices
                    ORDER BY date DESC
                    LIMIT 70
                ),
                date_bounds AS (
                    SELECT MIN(date) AS start_date, MAX(date) AS end_date
                    FROM ranked_dates
                ),
                prices AS (
                    SELECT
                        p.symbol,
                        p.date,
                        p.close,
                        t.market_cap,
                        i.name AS industry
                    FROM us_daily_prices p
                    JOIN tickers t ON p.symbol = t.ticker
                    JOIN industries i ON t.industry_id = i.id
                    JOIN date_bounds db ON p.date BETWEEN db.start_date AND db.end_date
                    WHERE t.market_cap IS NOT NULL AND t.market_cap > 0
                )
                SELECT symbol, date, close, market_cap, industry
                FROM prices
                ORDER BY date
            """)
            rows = conn.execute(query).fetchall()

        if not rows:
            raise HTTPException(status_code=503, detail="No price data available")

        # Build DataFrame
        df = pd.DataFrame(rows, columns=["symbol", "date", "close", "market_cap", "industry"])
        df["date"] = pd.to_datetime(df["date"])
        df["close"] = df["close"].astype(float)
        df["market_cap"] = df["market_cap"].astype(float)

        # Map industry -> GICS sector, drop unmapped
        df["sector"] = df["industry"].map(_GICS_MAP)
        df = df[df["sector"].notna()].copy()

        # Get sorted trading dates
        all_dates = sorted(df["date"].unique())
        if len(all_dates) < 21:
            raise HTTPException(status_code=503, detail="Not enough trading days")

        date_today = all_dates[-1]
        date_4w = all_dates[-21] if len(all_dates) >= 21 else all_dates[0]
        date_13w = all_dates[-66] if len(all_dates) >= 66 else all_dates[0]

        def sector_return(sector_name, start_date):
            """Market-cap weighted return for sector from start_date to latest."""
            sub = df[df["sector"] == sector_name]
            start = sub[sub["date"] == start_date][["symbol", "close", "market_cap"]].copy()
            end = sub[sub["date"] == date_today][["symbol", "close", "market_cap"]].copy()
            merged = start.merge(end, on="symbol", suffixes=("_s", "_e"))
            if merged.empty:
                return None
            weights = merged["market_cap_s"]
            returns = (merged["close_e"] / merged["close_s"] - 1)
            total_w = weights.sum()
            if total_w == 0:
                return None
            return float((returns * weights).sum() / total_w)

        def market_return(start_date):
            """Market-cap weighted return for all sectors from start_date to latest."""
            start = df[df["date"] == start_date][["symbol", "close", "market_cap"]].copy()
            end = df[df["date"] == date_today][["symbol", "close", "market_cap"]].copy()
            merged = start.merge(end, on="symbol", suffixes=("_s", "_e"))
            if merged.empty:
                return None
            weights = merged["market_cap_s"]
            returns = (merged["close_e"] / merged["close_s"] - 1)
            total_w = weights.sum()
            if total_w == 0:
                return None
            return float((returns * weights).sum() / total_w)

        mkt_13w = market_return(date_13w)
        mkt_4w = market_return(date_4w)

        sectors = [s for s in _GICS_MAP.values() if s is not None]
        sectors = sorted(set(sectors))

        raw = []
        for s in sectors:
            r13 = sector_return(s, date_13w)
            r4 = sector_return(s, date_4w)
            if r13 is None or r4 is None or mkt_13w is None or mkt_4w is None:
                continue
            # Relative strength vs market (guard against near-total-loss market)
            rs_13w = (1 + r13) / (1 + mkt_13w) if abs(1 + mkt_13w) > 1e-6 else None
            rs_4w = (1 + r4) / (1 + mkt_4w) if abs(1 + mkt_4w) > 1e-6 else None
            if rs_13w is None or rs_4w is None:
                continue
            raw.append({
                "sector": s,
                "rs_13w_raw": rs_13w,
                "rs_4w_raw": rs_4w,
                "return_13w": round(r13 * 100, 2),
                "return_4w": round(r4 * 100, 2),
            })

        if not raw:
            raise HTTPException(status_code=503, detail="Could not compute sector returns")

        # Normalize RS values to be centered around 100
        rs13_vals = [r["rs_13w_raw"] for r in raw]
        rs4_vals = [r["rs_4w_raw"] for r in raw]
        mean13, std13 = float(np.mean(rs13_vals)), float(np.std(rs13_vals))
        mean4, std4 = float(np.mean(rs4_vals)), float(np.std(rs4_vals))

        results = []
        for r in raw:
            rs_ratio = 100 + (r["rs_13w_raw"] - mean13) / std13 * 10 if std13 > 0 else 100.0
            rs_mom = 100 + (r["rs_4w_raw"] - mean4) / std4 * 10 if std4 > 0 else 100.0
            rs_ratio = round(rs_ratio, 2)
            rs_mom = round(rs_mom, 2)
            if rs_ratio >= 100 and rs_mom >= 100:
                quadrant = "Leading"
            elif rs_ratio >= 100 and rs_mom < 100:
                quadrant = "Weakening"
            elif rs_ratio < 100 and rs_mom < 100:
                quadrant = "Lagging"
            else:
                quadrant = "Improving"
            results.append({
                "sector": r["sector"],
                "rs_ratio": rs_ratio,
                "rs_momentum": rs_mom,
                "quadrant": quadrant,
                "return_13w": r["return_13w"],
                "return_4w": r["return_4w"],
            })

        results.sort(key=lambda x: x["rs_ratio"], reverse=True)

        actual_13w_days = len(all_dates) - 1 - list(all_dates).index(date_13w)
        return {
            "as_of": date_today.strftime("%Y-%m-%d"),
            "window_13w_days": actual_13w_days,
            "window_13w_full": actual_13w_days >= 65,
            "market_return_13w": round(mkt_13w * 100, 2) if mkt_13w is not None else None,
            "market_return_4w": round(mkt_4w * 100, 2) if mkt_4w is not None else None,
            "sectors": results,
        }

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
