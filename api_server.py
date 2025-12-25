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
    description="API for serving stock market data.",
    version="1.1.0"
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
    try:
        industry_name = industry_name.capitalize()
        
        with db.engine.connect() as conn:
            # 1. Get Industry ID
            ind_query = text("SELECT id FROM industries WHERE name = :name")
            res = conn.execute(ind_query, {"name": industry_name}).fetchone()
            if not res:
                raise HTTPException(status_code=404, detail="Industry not found")
            ind_id = res[0]

            # 2. Get Tickers
            t_query = text("""
                SELECT ticker, company_name, market_cap, 
                       pe_ratio, revenue
                FROM tickers 
                WHERE industry_id = :iid
                ORDER BY market_cap DESC NULLS LAST
            """)
            tickers_df = pd.read_sql(t_query, conn, params={"iid": ind_id})
            
            if tickers_df.empty:
                return {"industry": industry_name, "total_market_cap": 0, "ticker_count": 0, "donut_data": {"series":[], "labels":[]}, "stocks": []}

            tickers = tickers_df['ticker'].tolist()
            
            # 3. Fetch Prices (Look back 2 years)
            table_name = "us_daily_prices" 
            p_real_query = text(f"""
                SELECT symbol as ticker, date as market_date, 
                       close as close_price, volume
                FROM {table_name} 
                WHERE symbol IN :tickers 
                  AND date >= CURRENT_DATE - INTERVAL '730 days'
                ORDER BY symbol, date
            """)
            
            prices_df = pd.read_sql(p_real_query, conn, params={"tickers": tuple(tickers)})
            prices_df['market_date'] = pd.to_datetime(prices_df['market_date'])

            # 4. Helpers
            def safe_float(val):
                if pd.isna(val) or val is None: return None
                try:
                    f_val = float(val)
                    return f_val if not (math.isnan(f_val) or math.isinf(f_val)) else None
                except: return None

            def get_pct_change(sub_df, months=0, days=0):
                if sub_df.empty: return None
                latest = sub_df.iloc[-1]
                latest_date = latest['market_date']
                
                if months > 0:
                    start_date = latest_date - pd.DateOffset(months=months)
                elif days > 0:
                    start_date = latest_date - pd.DateOffset(days=days)
                else:
                    return None
                
                match = sub_df[sub_df['market_date'] <= start_date]
                if match.empty:
                    start_row = sub_df.iloc[0]
                else:
                    start_row = match.iloc[-1]
                
                sp = start_row['close_price']
                cp = latest['close_price']
                if sp == 0: return 0.0
                return ((cp - sp) / sp) * 100

            result_data = []
            total_mcap = safe_float(tickers_df['market_cap'].sum()) or 0.0
            
            for _, row in tickers_df.iterrows():
                t = row['ticker']
                t_prices = prices_df[prices_df['ticker'] == t].sort_values('market_date').drop_duplicates('market_date')
                latest_vol = t_prices.iloc[-1]['volume'] if not t_prices.empty else 0

                info = {
                    "symbol": t,
                    "company": row['company_name'] or t,
                    "market_cap": int(row['market_cap']) if pd.notnull(row['market_cap']) else 0,
                    "pe_ratio": safe_float(row['pe_ratio']),
                    "volume": int(latest_vol) if pd.notnull(latest_vol) else 0,
                    "revenue": int(row['revenue']) if pd.notnull(row['revenue']) else 0
                }

                if not t_prices.empty:
                    info.update({
                        "change_1d": safe_float(get_pct_change(t_prices, days=1)),
                        "change_1m": safe_float(get_pct_change(t_prices, months=1)),
                        "change_2m": safe_float(get_pct_change(t_prices, months=2)),
                        "change_3m": safe_float(get_pct_change(t_prices, months=3)),
                        "change_6m": safe_float(get_pct_change(t_prices, months=6)),
                        "change_12m": safe_float(get_pct_change(t_prices, months=12)),
                        "history": [{"x": r['market_date'].strftime('%Y-%m-%d'), "y": safe_float(r['close_price'])} for _, r in t_prices.tail(1000).iterrows()]
                    })
                    cur_yr = pd.Timestamp.now().year
                    ytd = t_prices[t_prices['market_date'].dt.year == cur_yr]
                    if len(ytd) > 1:
                        v = ((ytd.iloc[-1]['close_price'] - ytd.iloc[0]['close_price']) / ytd.iloc[0]['close_price']) * 100
                        info['change_ytd'] = safe_float(v)
                    else: info['change_ytd'] = None
                else:
                    info.update({"change_1d":None, "change_1m":None, "change_2m":None, "change_3m":None, "change_6m":None, "change_12m":None, "change_ytd":None, "history":[]})

                result_data.append(info)

            top_5 = tickers_df.head(5)
            others_mcap = tickers_df.iloc[5:]['market_cap'].sum() if len(tickers_df) > 5 else 0
            donut_series = [safe_float(x) or 0.0 for x in top_5['market_cap'].tolist()]
            donut_labels = top_5['ticker'].tolist()
            if others_mcap > 0:
                donut_series.append(safe_float(others_mcap))
                donut_labels.append("Others")

            return {
                "industry": industry_name,
                "total_market_cap": total_mcap,
                "ticker_count": len(tickers),
                "metadata": {
                    "lookback_days": 730,
                    "history_buffer": 1000,
                    "server_time": pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
                },
                "donut_data": {"series": donut_series, "labels": donut_labels},
                "stocks": result_data
            }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
