#!/usr/bin/env python3
"""
Precomputes all weekly RRG snapshots and caches to /tmp/sector_rotation_history_{date}.json.
Run: python generate_rotation_history.py
Lock file prevents concurrent runs.
"""
import sys
import json
import datetime
import os
import time
import traceback
import numpy as np
import pandas as pd
from pathlib import Path
from sqlalchemy import text
from market_data_db import MarketDataDB

# ── GICS sector mapping (same 145 industries as api.py) ──────────────────────
GICS_MAP = {
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

ALL_SECTORS = sorted(set(v for v in GICS_MAP.values() if v is not None))


def _weighted_return(ret_series: pd.Series, mcap_series: pd.Series) -> float | None:
    """Market-cap weighted return for a subset of symbols."""
    common = ret_series.index.intersection(mcap_series.index)
    r = ret_series[common]
    w = mcap_series[common]
    valid = r.notna() & w.notna() & (w > 0)
    r, w = r[valid], w[valid]
    total_w = w.sum()
    if total_w == 0 or len(r) == 0:
        return None
    return float((r * w).sum() / total_w)


def compute_snapshots(step: int = 5) -> list[dict]:
    """Load all price data and compute RS-Ratio / RS-Momentum for every step-th trading day."""
    db = MarketDataDB()

    with db.engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT p.symbol, p.date, p.close, t.market_cap, i.name AS industry
            FROM us_daily_prices p
            JOIN tickers t ON p.symbol = t.ticker
            JOIN industries i ON t.industry_id = i.id
            WHERE t.market_cap IS NOT NULL AND t.market_cap > 0
            ORDER BY p.date, p.symbol
        """)).fetchall()

    df = pd.DataFrame(rows, columns=["symbol", "date", "close", "market_cap", "industry"])
    df["date"] = pd.to_datetime(df["date"])
    df["close"] = df["close"].astype(float)
    df["market_cap"] = df["market_cap"].astype(float)
    df["sector"] = df["industry"].map(GICS_MAP)
    df = df[df["sector"].notna()].copy()

    # Static market caps per symbol (use the value we have)
    mcap = df.groupby("symbol")["market_cap"].first()
    sector_of = df.groupby("symbol")["sector"].first()

    # Pivot: rows=date, cols=symbol, values=close
    pivot = df.pivot_table(index="date", columns="symbol", values="close", aggfunc="last")
    dates = sorted(pivot.index)

    if len(dates) < 66:
        return []

    snapshots = []
    week_index = 0
    for t_idx in range(65, len(dates), step):
        date_t = dates[t_idx]
        date_4w = dates[t_idx - 20]
        date_13w = dates[t_idx - 65]

        close_t = pivot.loc[date_t]
        close_4w = pivot.loc[date_4w]
        close_13w = pivot.loc[date_13w]

        # Per-symbol returns
        ret_13w = (close_t / close_13w - 1).dropna()
        ret_4w = (close_t / close_4w - 1).dropna()

        mkt_13w = _weighted_return(ret_13w, mcap)
        mkt_4w = _weighted_return(ret_4w, mcap)
        if mkt_13w is None or mkt_4w is None:
            continue
        if abs(1 + mkt_13w) < 1e-6 or abs(1 + mkt_4w) < 1e-6:
            continue

        sectors_raw = []
        for sector in ALL_SECTORS:
            s_syms = sector_of[sector_of == sector].index
            r13 = _weighted_return(ret_13w, mcap.loc[mcap.index.intersection(s_syms)])
            r4 = _weighted_return(ret_4w, mcap.loc[mcap.index.intersection(s_syms)])
            if r13 is None or r4 is None:
                continue
            rs_13w = (1 + r13) / (1 + mkt_13w)
            rs_4w = (1 + r4) / (1 + mkt_4w)
            sectors_raw.append({
                "sector": sector,
                "rs_13w_raw": rs_13w,
                "rs_4w_raw": rs_4w,
                "return_13w": r13 * 100,
                "return_4w": r4 * 100,
            })

        if not sectors_raw:
            continue

        rs13_vals = [x["rs_13w_raw"] for x in sectors_raw]
        rs4_vals = [x["rs_4w_raw"] for x in sectors_raw]
        mean13, std13 = float(np.mean(rs13_vals)), float(np.std(rs13_vals))
        mean4, std4 = float(np.mean(rs4_vals)), float(np.std(rs4_vals))

        sectors_out = []
        for r in sectors_raw:
            rs_ratio = 100.0 + (r["rs_13w_raw"] - mean13) / std13 * 10 if std13 > 0 else 100.0
            rs_mom = 100.0 + (r["rs_4w_raw"] - mean4) / std4 * 10 if std4 > 0 else 100.0
            rs_ratio = round(rs_ratio, 2)
            rs_mom = round(rs_mom, 2)
            if rs_ratio >= 100 and rs_mom >= 100:
                quadrant = "Leading"
            elif rs_ratio >= 100:
                quadrant = "Weakening"
            elif rs_mom >= 100:
                quadrant = "Improving"
            else:
                quadrant = "Lagging"
            sectors_out.append({
                "sector": r["sector"],
                "rs_ratio": rs_ratio,
                "rs_momentum": rs_mom,
                "quadrant": quadrant,
                "return_13w": round(r["return_13w"], 2),
                "return_4w": round(r["return_4w"], 2),
            })

        snapshots.append({
            "date": date_t.strftime("%Y-%m-%d"),
            "week_index": week_index,
            "sectors": sectors_out,
        })
        week_index += 1

    return snapshots


def _atomic_write(path: Path, data: dict) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data))
    tmp.rename(path)


def generate(force: bool = False, interval: str = "weekly") -> None:
    today = datetime.date.today().isoformat()
    suffix = "_daily" if interval == "daily" else ""
    cache = Path(f"/tmp/sector_rotation_history{suffix}_{today}.json")
    lock = Path(f"/tmp/sector_rotation_history{suffix}_{today}.lock")

    if cache.exists() and not force:
        return

    if lock.exists():
        # Another process is generating — wait up to 120s
        for _ in range(24):
            time.sleep(5)
            if cache.exists():
                return
        lock.unlink(missing_ok=True)

    lock.write_text(str(os.getpid()))
    try:
        step = 1 if interval == "daily" else 5
        snapshots = compute_snapshots(step=step)
        result = {
            "generated_at": today,
            "interval": interval,
            "total_snapshots": len(snapshots),
            "snapshots": snapshots,
        }
        _atomic_write(cache, result)
    except Exception:
        traceback.print_exc()
        # Don't write error cache — let the next request retry
    finally:
        lock.unlink(missing_ok=True)


if __name__ == "__main__":
    force = "--force" in sys.argv
    idx = sys.argv.index("--interval") if "--interval" in sys.argv else -1
    interval = sys.argv[idx + 1] if 0 <= idx < len(sys.argv) - 1 else "weekly"
    if interval not in ("daily", "weekly"):
        sys.exit(f"Unknown --interval value: {interval!r}. Use 'weekly' or 'daily'.")
    print(f"Generating rotation history (force={force}, interval={interval})...")
    t0 = time.time()
    generate(force=force, interval=interval)
    print(f"Done in {time.time() - t0:.1f}s")
