#!/usr/bin/env python3
"""
Precomputes weekly RRG snapshots for industries within a single GICS sector.
Each snapshot includes two benchmark variants: vs full market, vs parent sector.

Usage:
    python generate_industry_rotation_history.py --sector Energy
    python generate_industry_rotation_history.py --sector "Information Technology" --force
"""
import sys
import json
import datetime
import os
import time
import traceback
import argparse
import numpy as np
import pandas as pd
from pathlib import Path
from sqlalchemy import text
from market_data_db import MarketDataDB
from generate_rotation_history import GICS_MAP, ALL_SECTORS, _weighted_return, _atomic_write


def _sector_slug(sector: str) -> str:
    return sector.lower().replace(" ", "_").replace("&", "and")


def compute_industry_snapshots(sector: str) -> list[dict]:
    """Compute weekly RS snapshots for all industries within a sector."""
    industries_in_sector = sorted([
        ind for ind, sec in GICS_MAP.items()
        if sec == sector and ind != "Shell Companies"
    ])
    if len(industries_in_sector) < 2:
        return []

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

    mcap = df.groupby("symbol")["market_cap"].first()
    industry_of = df.groupby("symbol")["industry"].first()
    sector_of = df.groupby("symbol")["sector"].first()
    sector_syms = sector_of[sector_of == sector].index

    pivot = df.pivot_table(index="date", columns="symbol", values="close", aggfunc="last")
    dates = sorted(pivot.index)

    if len(dates) < 66:
        return []

    def norm_list(arr):
        m, s = float(np.mean(arr)), float(np.std(arr))
        if s == 0:
            return [100.0] * len(arr)
        return [round(100.0 + (v - m) / s * 10, 2) for v in arr]

    def quadrant(ratio, mom):
        if ratio >= 100 and mom >= 100:
            return "Leading"
        if ratio >= 100:
            return "Weakening"
        if mom >= 100:
            return "Improving"
        return "Lagging"

    snapshots = []
    week_index = 0
    for t_idx in range(65, len(dates), 5):
        date_t = dates[t_idx]
        close_t   = pivot.loc[date_t]
        close_4w  = pivot.loc[dates[t_idx - 20]]
        close_13w = pivot.loc[dates[t_idx - 65]]

        ret_13w = (close_t / close_13w - 1).dropna()
        ret_4w  = (close_t / close_4w  - 1).dropna()

        mkt_13w = _weighted_return(ret_13w, mcap)
        mkt_4w  = _weighted_return(ret_4w,  mcap)
        if mkt_13w is None or mkt_4w is None:
            continue
        if abs(1 + mkt_13w) < 1e-6 or abs(1 + mkt_4w) < 1e-6:
            continue

        sec_13w = _weighted_return(ret_13w, mcap.loc[mcap.index.intersection(sector_syms)])
        sec_4w  = _weighted_return(ret_4w,  mcap.loc[mcap.index.intersection(sector_syms)])
        if sec_13w is None or abs(1 + sec_13w) < 1e-6:
            sec_13w = mkt_13w
        if sec_4w is None or abs(1 + sec_4w) < 1e-6:
            sec_4w = mkt_4w

        raw = []
        for ind in industries_in_sector:
            ind_syms = industry_of[industry_of == ind].index
            ind_mcap = mcap.loc[mcap.index.intersection(ind_syms)]
            r13 = _weighted_return(ret_13w, ind_mcap)
            r4  = _weighted_return(ret_4w,  ind_mcap)
            if r13 is None or r4 is None:
                continue
            raw.append({
                "industry":     ind,
                "rs_13w_mkt":   (1 + r13) / (1 + mkt_13w),
                "rs_4w_mkt":    (1 + r4)  / (1 + mkt_4w),
                "rs_13w_sec":   (1 + r13) / (1 + sec_13w),
                "rs_4w_sec":    (1 + r4)  / (1 + sec_4w),
                "return_13w":   round(r13 * 100, 2),
                "return_4w":    round(r4  * 100, 2),
                "stock_count":  int(len(ind_mcap)),
            })

        if len(raw) < 2:
            continue

        r13m = norm_list([x["rs_13w_mkt"] for x in raw])
        r4m  = norm_list([x["rs_4w_mkt"]  for x in raw])
        r13s = norm_list([x["rs_13w_sec"] for x in raw])
        r4s  = norm_list([x["rs_4w_sec"]  for x in raw])

        industries_out = []
        for i, r in enumerate(raw):
            industries_out.append({
                "industry":            r["industry"],
                "rs_ratio_market":     r13m[i],
                "rs_momentum_market":  r4m[i],
                "quadrant_market":     quadrant(r13m[i], r4m[i]),
                "rs_ratio_sector":     r13s[i],
                "rs_momentum_sector":  r4s[i],
                "quadrant_sector":     quadrant(r13s[i], r4s[i]),
                "return_13w":          r["return_13w"],
                "return_4w":           r["return_4w"],
                "stock_count":         r["stock_count"],
            })

        snapshots.append({
            "date":         date_t.strftime("%Y-%m-%d"),
            "week_index":   week_index,
            "industries":   industries_out,
        })
        week_index += 1

    return snapshots


def generate(sector: str, force: bool = False) -> None:
    slug  = _sector_slug(sector)
    today = datetime.date.today().isoformat()
    cache = Path(f"/tmp/industry_rotation_history_{slug}_{today}.json")
    lock  = Path(f"/tmp/industry_rotation_history_{slug}_{today}.lock")

    if cache.exists() and not force:
        return

    if lock.exists():
        for _ in range(24):
            time.sleep(5)
            if cache.exists():
                return
        lock.unlink(missing_ok=True)

    lock.write_text(str(os.getpid()))
    try:
        snapshots = compute_industry_snapshots(sector)
        _atomic_write(cache, {
            "generated_at":    today,
            "sector":          sector,
            "total_snapshots": len(snapshots),
            "snapshots":       snapshots,
        })
    except Exception:
        traceback.print_exc()
    finally:
        lock.unlink(missing_ok=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sector", required=True)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    if args.sector not in ALL_SECTORS:
        print(f"Unknown sector '{args.sector}'. Valid: {ALL_SECTORS}", file=sys.stderr)
        sys.exit(1)

    print(f"Generating industry rotation history: {args.sector} (force={args.force})…")
    t0 = time.time()
    generate(args.sector, args.force)
    print(f"Done in {time.time() - t0:.1f}s")
