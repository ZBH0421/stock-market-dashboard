from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from market_data_db import MarketDataDB
from sqlalchemy import text
import pandas as pd
import math
import uvicorn

# Initialize FastAPI app
app = FastAPI(
    title="Market Data API",
    description="API for serving real-time stock market data.",
    version="1.0.0"
)

# Enable CORS (Cross-Origin Resource Sharing)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Database
db = MarketDataDB()

@app.get("/")
def read_root():
    return {"status": "ok", "message": "Market Data API is running"}

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

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
