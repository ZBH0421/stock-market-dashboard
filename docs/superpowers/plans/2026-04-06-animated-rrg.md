# Animated RRG Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add animated RRG playback to `rotation.html` — plays ~99 weekly snapshots with fading trails, adjustable speed/trail/range, and a single-row control bar.

**Architecture:** A new background script (`generate_rotation_history.py`) precomputes all weekly snapshots from DB and caches to `/tmp`. A new API endpoint serves this cache (202 while generating). `rotation.html` fetches all snapshots upfront, renders them with ApexCharts + SVG overlay for trails, and drives animation via `setInterval`.

**Tech Stack:** Python/pandas (vectorized pivot computation), FastAPI, ApexCharts scatter chart, vanilla JS SVG overlay, Bootstrap 5.

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `generate_rotation_history.py` | Create | Load all prices, compute 99 weekly RS snapshots, write JSON cache |
| `api.py` | Modify | Add `GET /api/sector-rotation-history` — serve cache or trigger background generation |
| `rotation.html` | Rewrite | Full animated RRG: control bar, SVG trail overlay, playback engine |
| `tests/test_rotation_history.py` | Create | Unit tests for API endpoint |

---

## Task 1: `generate_rotation_history.py` — Computation Script

**Files:**
- Create: `generate_rotation_history.py`

This script loads all price data once into a pivot table for vectorized computation, then iterates weekly snapshots.

- [ ] **Step 1: Create `generate_rotation_history.py`**

```python
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


def compute_snapshots() -> list[dict]:
    """Load all price data and compute RS-Ratio / RS-Momentum for every 5th trading day."""
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
    for t_idx in range(65, len(dates), 5):
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


def generate(force: bool = False) -> None:
    today = datetime.date.today().isoformat()
    cache = Path(f"/tmp/sector_rotation_history_{today}.json")
    lock = Path(f"/tmp/sector_rotation_history_{today}.lock")

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
        snapshots = compute_snapshots()
        result = {
            "generated_at": today,
            "total_snapshots": len(snapshots),
            "snapshots": snapshots,
        }
        _atomic_write(cache, result)
    except Exception:
        import traceback
        traceback.print_exc()
        _atomic_write(cache, {"error": "generation failed", "generated_at": today, "total_snapshots": 0, "snapshots": []})
    finally:
        lock.unlink(missing_ok=True)


if __name__ == "__main__":
    force = "--force" in sys.argv
    print(f"Generating rotation history (force={force})...")
    t0 = time.time()
    generate(force=force)
    print(f"Done in {time.time() - t0:.1f}s")
```

- [ ] **Step 2: Run the script manually to verify it works**

```bash
cd /home/ubuntu/stock-market-dashboard
venv/bin/python generate_rotation_history.py --force
```

Expected output (approx):
```
[DB] Initialized MarketDataDB...
Generating rotation history (force=True)...
Done in 15.0s
```

Then verify the cache:
```bash
venv/bin/python -c "
import json
from pathlib import Path
import datetime
today = datetime.date.today().isoformat()
d = json.loads(Path(f'/tmp/sector_rotation_history_{today}.json').read_text())
print('snapshots:', d['total_snapshots'])
print('first date:', d['snapshots'][0]['date'])
print('last date:', d['snapshots'][-1]['date'])
print('sectors in first:', [s['sector'] for s in d['snapshots'][0]['sectors']])
"
```

Expected:
```
snapshots: 99
first date: 2024-04-xx
last date: 2026-04-02
sectors in first: ['Communication Services', 'Consumer Discretionary', ...]
```

- [ ] **Step 3: Commit**

```bash
git add generate_rotation_history.py
git commit -m "feat: add generate_rotation_history.py — precompute weekly RRG snapshots"
```

---

## Task 2: API Endpoint `/api/sector-rotation-history`

**Files:**
- Modify: `api.py` (add endpoint after the existing `/api/sector-rotation` block, before `if __name__ == "__main__":`)
- Create: `tests/test_rotation_history.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_rotation_history.py`:

```python
import sys, os, json, datetime
from pathlib import Path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi.testclient import TestClient
from api import app

client = TestClient(app)


def _write_fake_cache(snapshots: list) -> Path:
    today = datetime.date.today().isoformat()
    cache = Path(f"/tmp/sector_rotation_history_{today}.json")
    cache.write_text(json.dumps({
        "generated_at": today,
        "total_snapshots": len(snapshots),
        "snapshots": snapshots,
    }))
    return cache


def _clear_cache():
    today = datetime.date.today().isoformat()
    Path(f"/tmp/sector_rotation_history_{today}.json").unlink(missing_ok=True)
    Path(f"/tmp/sector_rotation_history_{today}.lock").unlink(missing_ok=True)


def test_returns_200_when_cache_exists():
    fake_snapshot = {
        "date": "2025-01-01",
        "week_index": 0,
        "sectors": [{"sector": "Energy", "rs_ratio": 105.0, "rs_momentum": 102.0, "quadrant": "Leading", "return_13w": 5.0, "return_4w": 2.0}],
    }
    _write_fake_cache([fake_snapshot])
    try:
        res = client.get("/api/sector-rotation-history")
        assert res.status_code == 200
        data = res.json()
        assert "snapshots" in data
        assert data["total_snapshots"] == 1
        assert data["snapshots"][0]["date"] == "2025-01-01"
    finally:
        _clear_cache()


def test_returns_202_when_no_cache():
    _clear_cache()
    res = client.get("/api/sector-rotation-history")
    assert res.status_code == 202
    assert res.json()["status"] == "generating"


def test_snapshot_sector_fields():
    fake_snapshot = {
        "date": "2025-06-01",
        "week_index": 0,
        "sectors": [
            {"sector": "Energy", "rs_ratio": 110.5, "rs_momentum": 104.3, "quadrant": "Leading", "return_13w": 8.2, "return_4w": 3.1},
            {"sector": "Financials", "rs_ratio": 92.1, "rs_momentum": 97.0, "quadrant": "Lagging", "return_13w": -4.1, "return_4w": -1.2},
        ],
    }
    _write_fake_cache([fake_snapshot])
    try:
        res = client.get("/api/sector-rotation-history")
        assert res.status_code == 200
        s = res.json()["snapshots"][0]["sectors"][0]
        assert "sector" in s
        assert "rs_ratio" in s
        assert "rs_momentum" in s
        assert "quadrant" in s
        assert "return_13w" in s
        assert "return_4w" in s
        assert isinstance(s["rs_ratio"], float)
        assert isinstance(s["return_13w"], float)
    finally:
        _clear_cache()
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
cd /home/ubuntu/stock-market-dashboard
venv/bin/python -m pytest tests/test_rotation_history.py -v
```

Expected: all 3 tests FAIL with `404` or `AttributeError` (endpoint doesn't exist yet).

- [ ] **Step 3: Add the endpoint to `api.py`**

Insert the following block in `api.py` immediately before the `if __name__ == "__main__":` line:

```python
@app.get("/api/sector-rotation-history")
def get_sector_rotation_history():
    """
    Returns all weekly RRG snapshots for animated playback.
    Serves cache /tmp/sector_rotation_history_{date}.json if present today.
    Returns 202 {status: 'generating'} while computing.
    """
    import datetime, subprocess, sys
    from pathlib import Path
    from fastapi.responses import JSONResponse
    import json as _json

    today = datetime.date.today().isoformat()
    cache = Path(f"/tmp/sector_rotation_history_{today}.json")

    if cache.exists():
        try:
            return _json.loads(cache.read_text())
        except (ValueError, OSError):
            cache.unlink(missing_ok=True)

    script = Path(__file__).parent / "generate_rotation_history.py"
    subprocess.Popen(
        [sys.executable, str(script), "--force"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    return JSONResponse(status_code=202, content={
        "status": "generating",
        "message": "Computing history snapshots, retry in 30 seconds.",
    })
```

- [ ] **Step 4: Run tests — all should pass**

```bash
venv/bin/python -m pytest tests/test_rotation_history.py -v
```

Expected:
```
PASSED tests/test_rotation_history.py::test_returns_200_when_cache_exists
PASSED tests/test_rotation_history.py::test_returns_202_when_no_cache
PASSED tests/test_rotation_history.py::test_snapshot_sector_fields
3 passed
```

- [ ] **Step 5: Restart API and smoke-test the endpoint**

```bash
sudo systemctl restart stock-api && sleep 2
curl -s http://localhost:8000/api/sector-rotation-history | python3 -c "import json,sys; d=json.load(sys.stdin); print('status:', d.get('status') or f'ok — {d[\"total_snapshots\"]} snapshots')"
```

Expected (if cache from Task 1 exists): `ok — 99 snapshots`
Expected (if no cache): `status: generating` — wait 30s, retry, should return 200.

- [ ] **Step 6: Run full test suite to catch regressions**

```bash
venv/bin/python -m pytest tests/test_rotation_history.py tests/test_gics_overview.py -v
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add api.py tests/test_rotation_history.py
git commit -m "feat: add /api/sector-rotation-history endpoint with cache + 202 while generating"
```

---

## Task 3: Rewrite `rotation.html` — Animated RRG with Controls + SVG Trails

**Files:**
- Rewrite: `rotation.html`

This task replaces the existing static scatter chart with a full animated version. The existing `/api/sector-rotation` endpoint and ranking table are preserved — the page now loads history data from `/api/sector-rotation-history` for the animated chart, and falls back to `/api/sector-rotation` for the table.

**Key design decisions:**
- All history snapshots are loaded upfront on page load (JSON ~50KB)
- ApexCharts scatter renders the **current frame's** 11 sector dots
- A transparent SVG is absolutely overlaid on the ApexCharts grid area to draw fading trail lines
- Coordinate mapping uses `chart.w.globals` (gridWidth, gridHeight, translateX, translateY) plus the known axis range (xMin/xMax/yMin/yMax)
- Animation runs via `setInterval`; scrubber drags update `currentIdx` directly

**Trail opacity formula:** For a trail of N weeks, the k-th oldest segment (k=0 = oldest) has `opacity = 0.05 + 0.75 * (k / (N-1))`.

- [ ] **Step 1: Rewrite `rotation.html`**

Replace the entire file with the following:

```html
<!DOCTYPE html>
<html lang="en" data-bs-theme="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Sector Rotation — Market Intelligence</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body { font-family: 'Inter', system-ui, sans-serif; background: #0f172a; color: #e2e8f0; }
        .card { background: #1e293b; border: 1px solid #334155; border-radius: 12px; }
        .card-header { background: transparent; border-bottom: 1px solid #334155; font-weight: 600; padding: 1rem 1.25rem; }

        /* Quadrant chips */
        .q-chip { display: inline-block; padding: 3px 10px; border-radius: 20px; font-size: 0.72rem; font-weight: 600; margin: 2px; }
        .q-leading   { background: #14532d33; color: #4ade80; border: 1px solid #166534; }
        .q-weakening { background: #713f1233; color: #fb923c; border: 1px solid #92400e; }
        .q-lagging   { background: #7f1d1d33; color: #f87171; border: 1px solid #991b1b; }
        .q-improving { background: #1e3a5f33; color: #60a5fa; border: 1px solid #1d4ed8; }

        /* Table */
        .sector-table th { font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em; color: #64748b; border-color: #334155; }
        .sector-table td { border-color: #334155; vertical-align: middle; font-size: 0.875rem; }
        .sector-table tbody tr:hover { background: #1e293b88; }
        .return-pos { color: #4ade80; }
        .return-neg { color: #f87171; }

        /* Control bar */
        .ctrl-bar {
            background: #1e293b; border: 1px solid #334155; border-radius: 10px;
            padding: 10px 14px; margin-bottom: 8px;
            display: flex; align-items: center; gap: 10px; flex-wrap: wrap;
        }
        .ctrl-btn {
            background: #334155; border: none; color: #e2e8f0; border-radius: 7px;
            padding: 6px 11px; cursor: pointer; font-size: 13px; transition: background .15s;
        }
        .ctrl-btn:hover { background: #475569; }
        .ctrl-btn.primary { background: #3b82f6; color: #fff; }
        .ctrl-btn.primary:hover { background: #2563eb; }
        .range-btn { background: #334155; border: none; color: #94a3b8; border-radius: 6px; padding: 4px 10px; font-size: 12px; cursor: pointer; }
        .range-btn.active { background: #1d4ed8; color: #fff; }

        /* Timeline scrubber */
        .timeline-wrap { flex: 1; min-width: 140px; }
        .timeline-track {
            height: 5px; background: #334155; border-radius: 3px;
            position: relative; cursor: pointer; margin-bottom: 3px;
        }
        .timeline-fill { height: 5px; background: #3b82f6; border-radius: 3px; pointer-events: none; }
        .timeline-thumb {
            width: 13px; height: 13px; background: #3b82f6; border-radius: 50%;
            position: absolute; top: -4px; transform: translateX(-50%);
            cursor: grab; box-shadow: 0 0 0 3px #3b82f622;
            pointer-events: none;
        }
        .timeline-date { color: #64748b; font-size: 11px; }

        /* Sliders */
        .slider-group { display: flex; align-items: center; gap: 6px; }
        .slider-group label { color: #64748b; font-size: 11px; white-space: nowrap; }
        .slider-group input[type=range] { width: 80px; accent-color: #3b82f6; cursor: pointer; }
        .slider-group span { color: #94a3b8; font-size: 11px; min-width: 32px; }

        /* Chart container — needs position:relative for SVG overlay */
        .chart-wrap { position: relative; }
        #rrgChart { min-height: 500px; }
        #trailSvg {
            position: absolute; top: 0; left: 0; width: 100%; height: 100%;
            pointer-events: none; overflow: visible;
        }

        .loading-spinner { display: flex; align-items: center; justify-content: center; height: 500px; }
    </style>
</head>
<body>
    <nav class="navbar navbar-dark px-4 py-3" style="background: #1e293b; border-bottom: 1px solid #334155;">
        <div class="d-flex align-items-center gap-3">
            <a class="navbar-brand mb-0" href="/">&#8592; Home</a>
            <a class="text-secondary text-decoration-none" href="stocks.html" style="font-size:0.9rem;">Stocks</a>
            <a class="text-secondary text-decoration-none" href="pe_history.html" style="font-size:0.9rem;">PE History</a>
            <a class="text-secondary text-decoration-none" href="brief.html" style="font-size:0.9rem;">Daily Brief</a>
        </div>
        <span class="text-info fw-semibold" style="font-size:0.9rem;">Sector Rotation</span>
    </nav>

    <div class="container-fluid py-4 px-4">
        <div class="mb-3">
            <h4 class="mb-0 fw-bold">Relative Rotation Graph</h4>
            <p class="text-secondary mb-0" style="font-size:0.85rem;">
                RS-Ratio (13W relative strength) vs RS-Momentum (4W relative strength).
                <span id="asOf" class="text-secondary">Loading…</span>
            </p>
        </div>

        <!-- Control bar -->
        <div class="ctrl-bar" id="ctrlBar" style="display:none;">
            <!-- Playback buttons -->
            <button class="ctrl-btn" id="btnFirst" title="Jump to start">⏮</button>
            <button class="ctrl-btn primary" id="btnPlay" title="Play / Pause">▶ Play</button>
            <button class="ctrl-btn" id="btnLast" title="Jump to end">⏭</button>

            <!-- Timeline scrubber -->
            <div class="timeline-wrap">
                <div class="timeline-track" id="timelineTrack">
                    <div class="timeline-fill" id="timelineFill" style="width:100%"></div>
                    <div class="timeline-thumb" id="timelineThumb" style="left:100%"></div>
                </div>
                <div class="timeline-date" id="timelineDate">—</div>
            </div>

            <!-- Speed slider -->
            <div class="slider-group">
                <label>速度</label>
                <input type="range" id="speedSlider" min="2" max="30" value="10">
                <span id="speedLabel">1.0s</span>
            </div>

            <!-- Trail slider -->
            <div class="slider-group">
                <label>拖尾</label>
                <input type="range" id="trailSlider" min="4" max="26" value="8">
                <span id="trailLabel">8週</span>
            </div>

            <!-- Time range -->
            <div class="d-flex gap-1">
                <button class="range-btn" data-weeks="13">3M</button>
                <button class="range-btn" data-weeks="26">6M</button>
                <button class="range-btn active" data-weeks="52">1Y</button>
                <button class="range-btn" data-weeks="0">全部</button>
            </div>
        </div>

        <!-- Chart -->
        <div class="card mb-4">
            <div class="card-body p-2">
                <div class="chart-wrap">
                    <div id="rrgChart">
                        <div class="loading-spinner" id="loadingSpinner">
                            <div class="text-center">
                                <div class="spinner-border text-info mb-2"></div>
                                <div class="text-secondary" id="loadingMsg">載入歷史資料中…</div>
                            </div>
                        </div>
                    </div>
                    <svg id="trailSvg"></svg>
                </div>
            </div>
        </div>

        <!-- Sector ranking table (shows current frame) -->
        <div class="card">
            <div class="card-header d-flex align-items-center gap-2">
                Sector Rankings
                <span class="text-secondary fw-normal" style="font-size:0.8rem;" id="tableDate"></span>
            </div>
            <div class="card-body p-0">
                <div class="table-responsive">
                    <table class="table sector-table mb-0">
                        <thead>
                            <tr>
                                <th class="px-3">Sector</th>
                                <th>Quadrant</th>
                                <th class="text-end">RS-Ratio</th>
                                <th class="text-end">RS-Mom</th>
                                <th class="text-end">13W Return</th>
                                <th class="text-end px-3">4W Return</th>
                            </tr>
                        </thead>
                        <tbody id="sectorTableBody">
                            <tr><td colspan="6" class="text-center text-secondary py-4">Loading…</td></tr>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>

        <p class="text-secondary mt-3" style="font-size:0.75rem;">
            RS-Ratio and RS-Momentum are z-score normalized and centered at 100.
            Values above 100 indicate outperformance vs market-cap weighted benchmark.
            Quadrant convention follows Verdussen (2004). Data updates daily.
        </p>
    </div>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/apexcharts"></script>
    <script>
    // ── Constants ────────────────────────────────────────────────────────────
    const QUADRANT_COLORS = {
        Leading: '#4ade80', Weakening: '#fb923c',
        Lagging: '#f87171', Improving: '#60a5fa',
    };
    const SECTOR_COLORS = {
        'Communication Services': '#f472b6',
        'Consumer Discretionary': '#fb923c',
        'Consumer Staples': '#a3e635',
        'Energy': '#fbbf24',
        'Financials': '#60a5fa',
        'Health Care': '#34d399',
        'Industrials': '#818cf8',
        'Information Technology': '#38bdf8',
        'Materials': '#f97316',
        'Real Estate': '#e879f9',
        'Utilities': '#94a3b8',
    };

    // ── State ────────────────────────────────────────────────────────────────
    let allSnapshots = [];      // full history from API
    let playSnapshots = [];     // subset based on time range
    let currentIdx = 0;         // index within playSnapshots
    let playTimer = null;       // setInterval handle
    let trailWeeks = 8;
    let speedMs = 1000;
    let chart = null;
    let chartGlobals = null;    // cached after first render
    let axisRange = null;       // { xMin, xMax, yMin, yMax }

    // ── Helpers ──────────────────────────────────────────────────────────────
    function fmtReturn(v) {
        const cls = v >= 0 ? 'return-pos' : 'return-neg';
        return `<span class="${cls}">${v >= 0 ? '+' : ''}${v.toFixed(1)}%</span>`;
    }
    function quadrantChip(q) {
        const cls = { Leading:'q-leading', Weakening:'q-weakening', Lagging:'q-lagging', Improving:'q-improving' }[q] || '';
        return `<span class="q-chip ${cls}">${q}</span>`;
    }

    // Map data coordinate → SVG pixel (using cached chart globals)
    function dataToSvgPx(x, y) {
        const g = chartGlobals;
        const { xMin, xMax, yMin, yMax } = axisRange;
        const px = g.translateX + (x - xMin) / (xMax - xMin) * g.gridWidth;
        const py = g.translateY + (1 - (y - yMin) / (yMax - yMin)) * g.gridHeight;
        return { px, py };
    }

    // ── Trail SVG ────────────────────────────────────────────────────────────
    function drawTrails() {
        const svg = document.getElementById('trailSvg');
        svg.innerHTML = '';
        if (!chartGlobals || currentIdx < 0) return;

        const startIdx = Math.max(0, currentIdx - trailWeeks + 1);
        const trail = playSnapshots.slice(startIdx, currentIdx + 1); // oldest → newest

        // For each sector, draw its path across the trail frames
        const sectors = Object.keys(SECTOR_COLORS);
        const N = trail.length;

        sectors.forEach(sector => {
            const points = trail.map(snap => {
                const s = snap.sectors.find(x => x.sector === sector);
                return s ? dataToSvgPx(s.rs_ratio, s.rs_momentum) : null;
            }).filter(Boolean);

            if (points.length < 2) return;
            const color = SECTOR_COLORS[sector];

            // Draw segments with increasing opacity (oldest = most transparent)
            for (let i = 0; i < points.length - 1; i++) {
                const opacity = N <= 1 ? 0.8 : 0.05 + 0.75 * (i / (N - 2));
                const seg = document.createElementNS('http://www.w3.org/2000/svg', 'line');
                seg.setAttribute('x1', points[i].px);
                seg.setAttribute('y1', points[i].py);
                seg.setAttribute('x2', points[i + 1].px);
                seg.setAttribute('y2', points[i + 1].py);
                seg.setAttribute('stroke', color);
                seg.setAttribute('stroke-width', '1.5');
                seg.setAttribute('stroke-opacity', opacity.toFixed(3));
                svg.appendChild(seg);
            }

            // Draw dots along the trail (smaller, fading)
            for (let i = 0; i < points.length - 1; i++) {
                const opacity = 0.05 + 0.5 * (i / (N - 1));
                const dot = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
                dot.setAttribute('cx', points[i].px);
                dot.setAttribute('cy', points[i].py);
                dot.setAttribute('r', 3);
                dot.setAttribute('fill', color);
                dot.setAttribute('fill-opacity', opacity.toFixed(3));
                svg.appendChild(dot);
            }
        });
    }

    // ── Chart rendering ──────────────────────────────────────────────────────
    function computeAxisRange(snapshots) {
        let allX = [], allY = [];
        snapshots.forEach(snap => snap.sectors.forEach(s => {
            allX.push(s.rs_ratio); allY.push(s.rs_momentum);
        }));
        const xPad = Math.max(Math.abs(Math.min(...allX) - 100), Math.abs(Math.max(...allX) - 100)) + 4;
        const yPad = Math.max(Math.abs(Math.min(...allY) - 100), Math.abs(Math.max(...allY) - 100)) + 4;
        return { xMin: 100 - xPad, xMax: 100 + xPad, yMin: 100 - yPad, yMax: 100 + yPad };
    }

    function buildSeries(sectors) {
        const quadrants = ['Leading', 'Weakening', 'Lagging', 'Improving'];
        return quadrants.map(q => ({
            name: q,
            data: sectors.filter(s => s.quadrant === q).map(s => ({
                x: s.rs_ratio, y: s.rs_momentum,
                label: s.sector, return_13w: s.return_13w, return_4w: s.return_4w,
            })),
        }));
    }

    function initChart(snap) {
        axisRange = computeAxisRange(allSnapshots);
        const { xMin, xMax, yMin, yMax } = axisRange;
        const series = buildSeries(snap.sectors);

        const options = {
            chart: {
                type: 'scatter', height: 500, background: '#1e293b',
                toolbar: { show: false }, zoom: { enabled: false },
                animations: { enabled: false },
                events: {
                    mounted(ctx) {
                        chartGlobals = ctx.w.globals;
                        drawTrails();
                    },
                    updated(ctx) {
                        chartGlobals = ctx.w.globals;
                        drawTrails();
                    },
                },
            },
            theme: { mode: 'dark' },
            series,
            colors: ['#4ade80', '#fb923c', '#f87171', '#60a5fa'],
            markers: { size: 10, strokeWidth: 2, hover: { sizeOffset: 3 } },
            xaxis: {
                min: xMin, max: xMax,
                title: { text: 'RS-Ratio (13W relative strength)', style: { color: '#94a3b8', fontSize: '12px' } },
                labels: { style: { colors: '#94a3b8' }, formatter: v => v.toFixed(1) },
                axisBorder: { color: '#334155' }, axisTicks: { color: '#334155' },
            },
            yaxis: {
                min: yMin, max: yMax,
                title: { text: 'RS-Momentum (4W relative strength)', style: { color: '#94a3b8', fontSize: '12px' } },
                labels: { style: { colors: '#94a3b8' }, formatter: v => v.toFixed(1) },
            },
            grid: { borderColor: '#334155', xaxis: { lines: { show: false } }, yaxis: { lines: { show: false } } },
            annotations: {
                xaxis: [{ x: 100, strokeDashArray: 2, borderColor: '#475569' }],
                yaxis: [{ y: 100, strokeDashArray: 2, borderColor: '#475569' }],
                points: [
                    { x: xMax - 2, y: yMax - 1, label: { text: 'Leading',   style: { color: '#4ade80', background: 'transparent', border: 0, fontSize: '11px', fontWeight: 700 } } },
                    { x: xMax - 2, y: yMin + 1, label: { text: 'Weakening', style: { color: '#fb923c', background: 'transparent', border: 0, fontSize: '11px', fontWeight: 700 } } },
                    { x: xMin + 2, y: yMin + 1, label: { text: 'Lagging',   style: { color: '#f87171', background: 'transparent', border: 0, fontSize: '11px', fontWeight: 700 } } },
                    { x: xMin + 2, y: yMax - 1, label: { text: 'Improving', style: { color: '#60a5fa', background: 'transparent', border: 0, fontSize: '11px', fontWeight: 700 } } },
                ],
            },
            tooltip: {
                custom({ seriesIndex, dataPointIndex, w }) {
                    const d = w.config.series[seriesIndex].data[dataPointIndex];
                    const q = w.config.series[seriesIndex].name;
                    const color = QUADRANT_COLORS[q];
                    const s13 = d.return_13w >= 0 ? '+' : '';
                    const s4  = d.return_4w  >= 0 ? '+' : '';
                    return `<div style="padding:10px 14px;background:#1e293b;border:1px solid #334155;border-radius:8px;font-size:13px;">
                        <div style="font-weight:700;color:${color};margin-bottom:6px;">${d.label}</div>
                        <div style="color:#94a3b8;">RS-Ratio: <span style="color:#e2e8f0;">${d.x.toFixed(2)}</span></div>
                        <div style="color:#94a3b8;">RS-Mom:   <span style="color:#e2e8f0;">${d.y.toFixed(2)}</span></div>
                        <div style="color:#94a3b8;">13W: <span style="color:${d.return_13w>=0?'#4ade80':'#f87171'};">${s13}${d.return_13w.toFixed(1)}%</span></div>
                        <div style="color:#94a3b8;">4W:  <span style="color:${d.return_4w>=0?'#4ade80':'#f87171'};">${s4}${d.return_4w.toFixed(1)}%</span></div>
                    </div>`;
                },
            },
            legend: { position: 'top', labels: { colors: '#94a3b8' }, markers: { width: 10, height: 10, radius: 12 } },
            dataLabels: {
                enabled: true,
                formatter: (val, opts) => opts.w.config.series[opts.seriesIndex].data[opts.dataPointIndex].label,
                offsetY: -14,
                style: { fontSize: '10px', colors: ['#e2e8f0'] },
                background: { enabled: false },
            },
        };

        document.getElementById('rrgChart').innerHTML = '';
        chart = new ApexCharts(document.getElementById('rrgChart'), options);
        chart.render();
    }

    function updateChart(snap) {
        if (!chart) { initChart(snap); return; }
        chart.updateSeries(buildSeries(snap.sectors), false);
    }

    // ── Frame rendering ──────────────────────────────────────────────────────
    function renderFrame(idx) {
        currentIdx = Math.max(0, Math.min(idx, playSnapshots.length - 1));
        const snap = playSnapshots[currentIdx];
        if (!snap) return;

        // Update chart (triggers drawTrails via 'updated' event)
        updateChart(snap);

        // Update scrubber
        const pct = playSnapshots.length > 1 ? (currentIdx / (playSnapshots.length - 1)) * 100 : 100;
        document.getElementById('timelineFill').style.width = pct + '%';
        document.getElementById('timelineThumb').style.left = pct + '%';
        document.getElementById('timelineDate').textContent = snap.date + ` (week ${snap.week_index + 1}/${allSnapshots.length})`;
        document.getElementById('asOf').textContent = `— as of ${snap.date}`;
        document.getElementById('tableDate').textContent = snap.date;

        // Update table (only when not playing to avoid jank)
        if (!playTimer) renderTable(snap.sectors);
    }

    function renderTable(sectors) {
        const sorted = [...sectors].sort((a, b) => b.rs_ratio - a.rs_ratio);
        document.getElementById('sectorTableBody').innerHTML = sorted.map(s => `
            <tr>
                <td class="px-3 fw-semibold">${s.sector}</td>
                <td>${quadrantChip(s.quadrant)}</td>
                <td class="text-end">${s.rs_ratio.toFixed(2)}</td>
                <td class="text-end">${s.rs_momentum.toFixed(2)}</td>
                <td class="text-end">${fmtReturn(s.return_13w)}</td>
                <td class="text-end px-3">${fmtReturn(s.return_4w)}</td>
            </tr>
        `).join('');
    }

    // ── Playback ─────────────────────────────────────────────────────────────
    function stopPlay() {
        if (playTimer) { clearInterval(playTimer); playTimer = null; }
        document.getElementById('btnPlay').textContent = '▶ Play';
        document.getElementById('btnPlay').classList.remove('primary');
        document.getElementById('btnPlay').classList.add('ctrl-btn');
        // Update table now that we stopped
        if (playSnapshots[currentIdx]) renderTable(playSnapshots[currentIdx].sectors);
    }

    function startPlay() {
        if (currentIdx >= playSnapshots.length - 1) currentIdx = 0;
        document.getElementById('btnPlay').textContent = '⏸ Pause';
        document.getElementById('btnPlay').classList.add('primary');
        playTimer = setInterval(() => {
            if (currentIdx >= playSnapshots.length - 1) {
                stopPlay();
                return;
            }
            renderFrame(currentIdx + 1);
        }, speedMs);
    }

    function togglePlay() {
        if (playTimer) stopPlay();
        else startPlay();
    }

    // ── Time range ───────────────────────────────────────────────────────────
    function setTimeRange(weeks) {
        const wasPlaying = !!playTimer;
        stopPlay();
        if (weeks === 0 || weeks >= allSnapshots.length) {
            playSnapshots = allSnapshots;
        } else {
            playSnapshots = allSnapshots.slice(-weeks);
        }
        currentIdx = playSnapshots.length - 1;
        renderFrame(currentIdx);
        if (wasPlaying) startPlay();
    }

    // ── Timeline drag ─────────────────────────────────────────────────────────
    function initTimelineDrag() {
        const track = document.getElementById('timelineTrack');
        let dragging = false;

        function seekTo(e) {
            const rect = track.getBoundingClientRect();
            const x = (e.touches ? e.touches[0].clientX : e.clientX) - rect.left;
            const pct = Math.max(0, Math.min(1, x / rect.width));
            const idx = Math.round(pct * (playSnapshots.length - 1));
            renderFrame(idx);
        }

        track.addEventListener('mousedown', e => { dragging = true; seekTo(e); });
        track.addEventListener('touchstart', e => { dragging = true; seekTo(e); }, { passive: true });
        document.addEventListener('mousemove', e => { if (dragging) seekTo(e); });
        document.addEventListener('touchmove', e => { if (dragging) seekTo(e); }, { passive: true });
        document.addEventListener('mouseup', () => { dragging = false; });
        document.addEventListener('touchend', () => { dragging = false; });
    }

    // ── Controls wiring ──────────────────────────────────────────────────────
    function initControls() {
        document.getElementById('btnPlay').addEventListener('click', togglePlay);
        document.getElementById('btnFirst').addEventListener('click', () => { stopPlay(); renderFrame(0); });
        document.getElementById('btnLast').addEventListener('click', () => { stopPlay(); renderFrame(playSnapshots.length - 1); });

        // Speed slider: value 2–30 maps to 3.0s–0.2s (inverted, formula: 6/value seconds)
        // value=2 → 6/2=3.0s, value=6 → 1.0s, value=30 → 0.2s
        const speedSlider = document.getElementById('speedSlider');
        speedSlider.addEventListener('input', () => {
            const secs = Math.max(0.2, Math.min(3.0, 6 / parseInt(speedSlider.value)));
            speedMs = Math.round(secs * 1000);
            document.getElementById('speedLabel').textContent = secs.toFixed(1) + 's';
            if (playTimer) { stopPlay(); startPlay(); }
        });

        // Trail slider
        const trailSlider = document.getElementById('trailSlider');
        trailSlider.addEventListener('input', () => {
            trailWeeks = parseInt(trailSlider.value);
            document.getElementById('trailLabel').textContent = trailWeeks + '週';
            drawTrails();
        });

        // Range buttons
        document.querySelectorAll('.range-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                document.querySelectorAll('.range-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                setTimeRange(parseInt(btn.dataset.weeks));
            });
        });

        initTimelineDrag();
    }

    // ── Data loading ──────────────────────────────────────────────────────────
    async function pollUntilReady() {
        for (let attempt = 0; attempt < 20; attempt++) {
            await new Promise(r => setTimeout(r, attempt === 0 ? 0 : 10000));
            const res = await fetch('/api/sector-rotation-history');
            if (res.status === 200) return res.json();
            if (res.status !== 202) throw new Error('API error ' + res.status);
            document.getElementById('loadingMsg').textContent =
                `計算中… 請稍候 (${attempt * 10}s elapsed)`;
        }
        throw new Error('Timed out waiting for history data');
    }

    async function load() {
        try {
            const data = await pollUntilReady();
            allSnapshots = data.snapshots || [];
            if (allSnapshots.length === 0) throw new Error('No snapshot data returned');

            // Default: 1Y range
            const defaultWeeks = 52;
            playSnapshots = allSnapshots.length > defaultWeeks
                ? allSnapshots.slice(-defaultWeeks)
                : allSnapshots;
            currentIdx = playSnapshots.length - 1;

            document.getElementById('loadingSpinner').style.display = 'none';
            document.getElementById('ctrlBar').style.display = '';

            initControls();
            initChart(playSnapshots[currentIdx]);
            renderFrame(currentIdx);
        } catch (e) {
            document.getElementById('loadingSpinner').innerHTML =
                `<div class="text-center text-danger py-5">Failed to load: ${e.message}</div>`;
        }
    }

    load();
    </script>
</body>
</html>
```

- [ ] **Step 2: Open `http://localhost:8087/rotation.html` in browser and verify:**

  - Page shows spinner "載入歷史資料中…"
  - If cache exists from Task 1: chart appears within 2 seconds
  - If no cache: shows "計算中…" for ~30s, then chart appears
  - Chart shows 11 sectors as colored dots
  - Quadrant labels (Leading/Weakening/Lagging/Improving) visible in corners
  - Date in subtitle updates to most recent snapshot date

- [ ] **Step 3: Verify control bar interactions**

  - Click **▶ Play** → sectors animate, button changes to ⏸ Pause, date increments
  - Click **⏸ Pause** → animation stops, table updates
  - Click **⏮** → jumps to first frame, trail appears
  - Click **⏭** → jumps to last frame
  - Drag **timeline scrubber** → chart updates to correct date
  - Move **速度 slider** left → animation slows; right → speeds up
  - Move **拖尾 slider** → trail behind sectors grows/shrinks
  - Click **3M / 6M / 全部** → playback range changes, currentIdx resets to last frame

- [ ] **Step 4: Verify trail rendering**

  - Trail lines fade from near-invisible (oldest) to bright (newest)
  - Each sector has its own color (consistent across frames)
  - At frame 0 (start), no trail visible (nothing before first frame)
  - At frame 8+ with trail=8, trail shows 8 segments

- [ ] **Step 5: Commit**

```bash
git add rotation.html
git commit -m "feat: animated RRG — playback controls, SVG trail overlay, time range selector"
```

---

## Task 4: Run Full Test Suite and Push

- [ ] **Step 1: Run all tests**

```bash
cd /home/ubuntu/stock-market-dashboard
venv/bin/python -m pytest tests/test_rotation_history.py tests/test_gics_overview.py -v
```

Expected: all tests pass.

- [ ] **Step 2: Smoke-test the live API**

```bash
curl -s http://localhost:8000/api/sector-rotation-history | \
  python3 -c "import json,sys; d=json.load(sys.stdin); print(f'snapshots={d[\"total_snapshots\"]}, first={d[\"snapshots\"][0][\"date\"]}, last={d[\"snapshots\"][-1][\"date\"]}')"
```

Expected: `snapshots=99, first=2024-04-xx, last=2026-04-02`

- [ ] **Step 3: Push**

```bash
git push
```
