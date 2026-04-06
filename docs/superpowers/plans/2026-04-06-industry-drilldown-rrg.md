# Industry Drill-down RRG Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add industry-level drill-down to rotation.html — clicking a sector pill switches the RRG to show that sector's sub-industries, with toggleable vs-market / vs-sector benchmarks.

**Architecture:** New `generate_industry_rotation_history.py` precomputes weekly industry snapshots (reusing helpers from the existing sector script). A new FastAPI endpoint serves these with the same 202-polling pattern. The frontend normalizes industry data into the existing sector shape, so `buildSeries`/`drawTrails`/`renderFrame` need minimal changes.

**Tech Stack:** Python/pandas/numpy (same as existing), FastAPI, ApexCharts scatter, SVG overlay, Bootstrap 5 dark.

---

## File Map

| File | Action |
|------|--------|
| `generate_industry_rotation_history.py` | Create |
| `api.py` | Modify — add `/api/industry-rotation-history` endpoint |
| `rotation.html` | Modify — drill-down state, HTML, CSS, JS |
| `tests/test_industry_rotation.py` | Create |

---

## Codebase Context

**`generate_rotation_history.py`** exports:
- `GICS_MAP` — dict of 145 industry names → GICS sector (None for Shell Companies)
- `_weighted_return(ret_series, mcap_series) -> float | None`
- `_atomic_write(path, data)`
- `ALL_SECTORS` — sorted list of 11 sector strings

**`api.py`** pattern for 202 endpoints (lines ~1138-1167): read cache, spawn background process if missing, return 202.

**`rotation.html`** key globals: `allSnapshots`, `playSnapshots`, `currentIdx`, `visibleSectors` (Set), `SECTOR_COLORS` (dict), `chart`, `chartGlobals`, `axisRange`.

Key functions:
- `buildSeries(sectors)` — maps sector array → ApexCharts series; filters by `visibleSectors`
- `drawTrails()` — iterates `Object.keys(SECTOR_COLORS)`, draws SVG; uses `visibleSectors`
- `smoothSnapshots(snaps)` — 3-week trailing MA on `rs_ratio`/`rs_momentum` per `s.sector`
- `renderFrame(idx)` — calls `updateChart(snap)` + `renderTable(snap.sectors)`
- `initSectorToggles()` — creates sector pills with click-to-toggle behavior
- `computeAxisRange(snapshots)` — iterates `snap.sectors`

**Cache slug formula:** `sector.lower().replace(' ', '_').replace('&', 'and')`

---

## Task 1: `generate_industry_rotation_history.py`

**Files:**
- Create: `generate_industry_rotation_history.py`
- Test: `tests/test_industry_rotation.py` (partial — script output tested here)

- [ ] **Step 1: Write the failing test for snapshot structure**

```python
# tests/test_industry_rotation.py
import sys, os, json, datetime, subprocess
from pathlib import Path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def _slug(sector): return sector.lower().replace(' ', '_').replace('&', 'and')

def _clear_industry_cache(sector):
    today = datetime.date.today().isoformat()
    slug = _slug(sector)
    Path(f"/tmp/industry_rotation_history_{slug}_{today}.json").unlink(missing_ok=True)
    Path(f"/tmp/industry_rotation_history_{slug}_{today}.lock").unlink(missing_ok=True)

def test_invalid_sector_exits_nonzero():
    result = subprocess.run(
        ["venv/bin/python", "generate_industry_rotation_history.py", "--sector", "FakeSector"],
        capture_output=True, text=True,
        cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    assert result.returncode != 0

def test_generates_cache_file():
    sector = "Energy"
    _clear_industry_cache(sector)
    try:
        result = subprocess.run(
            ["venv/bin/python", "generate_industry_rotation_history.py", "--sector", sector, "--force"],
            capture_output=True, text=True, timeout=300,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        assert result.returncode == 0, result.stderr
        today = datetime.date.today().isoformat()
        cache = Path(f"/tmp/industry_rotation_history_{_slug(sector)}_{today}.json")
        assert cache.exists()
        data = json.loads(cache.read_text())
        assert data["sector"] == sector
        assert data["total_snapshots"] > 0
        snap = data["snapshots"][0]
        assert "date" in snap and "week_index" in snap and "industries" in snap
        ind = snap["industries"][0]
        for field in ["industry", "rs_ratio_market", "rs_momentum_market", "quadrant_market",
                      "rs_ratio_sector", "rs_momentum_sector", "quadrant_sector",
                      "return_13w", "return_4w", "stock_count"]:
            assert field in ind, f"Missing field: {field}"
        assert ind["quadrant_market"] in ("Leading", "Weakening", "Lagging", "Improving")
    finally:
        _clear_industry_cache(sector)
```

- [ ] **Step 2: Run test to verify it fails**

```bash
venv/bin/python -m pytest tests/test_industry_rotation.py::test_invalid_sector_exits_nonzero -v
```
Expected: FAIL (file doesn't exist yet)

- [ ] **Step 3: Create `generate_industry_rotation_history.py`**

```python
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
from generate_rotation_history import GICS_MAP, ALL_SECTORS, _weighted_return


def _atomic_write(path: Path, data: dict) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data))
    tmp.rename(path)


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
        # Fall back to market if sector return unavailable
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
            "generated_at":   today,
            "sector":         sector,
            "total_snapshots": len(snapshots),
            "snapshots":      snapshots,
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
```

- [ ] **Step 4: Run tests**

```bash
venv/bin/python -m pytest tests/test_industry_rotation.py -v
```
Expected: `test_invalid_sector_exits_nonzero` PASS, `test_generates_cache_file` PASS (takes ~2 min)

- [ ] **Step 5: Commit**

```bash
git add generate_industry_rotation_history.py tests/test_industry_rotation.py
git commit -m "feat: add generate_industry_rotation_history.py with dual benchmark support"
```

---

## Task 2: API endpoint `/api/industry-rotation-history`

**Files:**
- Modify: `api.py` (append before the `if __name__` block)
- Modify: `tests/test_industry_rotation.py` (add API tests)

- [ ] **Step 1: Add API tests**

Append to `tests/test_industry_rotation.py`:

```python
from fastapi.testclient import TestClient
from api import app

client = TestClient(app)


def _write_industry_cache(sector, snapshots):
    today = datetime.date.today().isoformat()
    slug = _slug(sector)
    cache = Path(f"/tmp/industry_rotation_history_{slug}_{today}.json")
    cache.write_text(json.dumps({
        "generated_at": today,
        "sector": sector,
        "total_snapshots": len(snapshots),
        "snapshots": snapshots,
    }))
    return cache


def _fake_snapshot():
    return {
        "date": "2025-01-01",
        "week_index": 0,
        "industries": [{
            "industry": "Oil & Gas Exploration & Production",
            "rs_ratio_market": 105.0, "rs_momentum_market": 102.0,
            "quadrant_market": "Leading",
            "rs_ratio_sector": 103.0, "rs_momentum_sector": 101.0,
            "quadrant_sector": "Leading",
            "return_13w": 5.0, "return_4w": 2.0, "stock_count": 42,
        }],
    }


def test_api_returns_200_with_cache():
    _clear_industry_cache("Energy")
    _write_industry_cache("Energy", [_fake_snapshot()])
    try:
        res = client.get("/api/industry-rotation-history?sector=Energy")
        assert res.status_code == 200
        data = res.json()
        assert data["sector"] == "Energy"
        assert data["total_snapshots"] == 1
    finally:
        _clear_industry_cache("Energy")


def test_api_returns_202_without_cache():
    _clear_industry_cache("Energy")
    res = client.get("/api/industry-rotation-history?sector=Energy")
    assert res.status_code == 202
    assert res.json()["status"] == "generating"


def test_api_returns_400_for_invalid_sector():
    res = client.get("/api/industry-rotation-history?sector=FakeSector")
    assert res.status_code == 400


def test_api_snapshot_fields():
    _clear_industry_cache("Energy")
    _write_industry_cache("Energy", [_fake_snapshot()])
    try:
        res = client.get("/api/industry-rotation-history?sector=Energy")
        assert res.status_code == 200
        ind = res.json()["snapshots"][0]["industries"][0]
        for field in ["industry", "rs_ratio_market", "rs_momentum_market", "quadrant_market",
                      "rs_ratio_sector", "rs_momentum_sector", "quadrant_sector",
                      "return_13w", "return_4w", "stock_count"]:
            assert field in ind, f"Missing: {field}"
    finally:
        _clear_industry_cache("Energy")
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
venv/bin/python -m pytest tests/test_industry_rotation.py::test_api_returns_400_for_invalid_sector -v
```
Expected: FAIL (endpoint not defined yet)

- [ ] **Step 3: Add endpoint to `api.py`**

Insert before the `if __name__ == "__main__":` line at the bottom of `api.py`:

```python
@app.get("/api/industry-rotation-history")
def get_industry_rotation_history(sector: str):
    """
    Returns weekly RRG snapshots for industries within a GICS sector.
    Each snapshot contains both vs-market and vs-sector RS values.
    Returns 202 while computing, 400 for unknown sector.
    """
    import datetime, subprocess, sys
    from pathlib import Path
    from fastapi.responses import JSONResponse
    import json as _json
    from generate_rotation_history import ALL_SECTORS as _ALL_SECTORS

    if sector not in _ALL_SECTORS:
        raise HTTPException(status_code=400, detail=f"Unknown sector: {sector!r}. Valid: {list(_ALL_SECTORS)}")

    def _slug(s): return s.lower().replace(" ", "_").replace("&", "and")

    today = datetime.date.today().isoformat()
    cache = Path(f"/tmp/industry_rotation_history_{_slug(sector)}_{today}.json")

    if cache.exists():
        try:
            return _json.loads(cache.read_text())
        except (ValueError, OSError):
            cache.unlink(missing_ok=True)

    script = Path(__file__).parent / "generate_industry_rotation_history.py"
    subprocess.Popen(
        [sys.executable, str(script), "--sector", sector, "--force"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    return JSONResponse(status_code=202, content={
        "status": "generating",
        "message": f"Computing {sector} industry snapshots, retry in 30 seconds.",
    })
```

- [ ] **Step 4: Restart API and run all tests**

```bash
sudo systemctl restart stock-api && sleep 2
venv/bin/python -m pytest tests/test_industry_rotation.py -v
```
Expected: all 6 tests PASS (the `test_generates_cache_file` test is slow — skip with `-k "not generates_cache"` if needed)

- [ ] **Step 5: Run full test suite**

```bash
venv/bin/python -m pytest tests/ -v -q
```
Expected: all 12 tests PASS

- [ ] **Step 6: Commit**

```bash
git add api.py tests/test_industry_rotation.py
git commit -m "feat: add /api/industry-rotation-history endpoint with 202 polling"
```

---

## Task 3: `rotation.html` — HTML/CSS additions

**Files:**
- Modify: `rotation.html`

Add HTML for breadcrumb and benchmark toggle, and CSS for both.

- [ ] **Step 1: Add CSS for breadcrumb and benchmark toggle**

In `rotation.html`, find the CSS block ending with `.loading-spinner { ... }` and add immediately after it (before `</style>`):

```css
/* Drill-down breadcrumb */
.drill-breadcrumb {
    display: flex; align-items: center; gap: 10px; flex-wrap: wrap;
    font-size: 0.85rem; color: #64748b; margin-bottom: 8px;
}
.drill-breadcrumb .drill-sector { color: #fbbf24; font-weight: 600; }
.drill-breadcrumb .drill-back {
    color: #60a5fa; cursor: pointer; text-decoration: none; font-size: 0.8rem;
    background: #1e3a5f; border: 1px solid #1d4ed8; border-radius: 6px;
    padding: 2px 10px;
}
.drill-breadcrumb .drill-back:hover { background: #1d4ed8; color: #fff; }

/* Benchmark toggle (in control bar) */
.benchmark-toggle { display: inline-flex; border: 1px solid #334155; border-radius: 7px; overflow: hidden; }
.benchmark-btn {
    padding: 4px 11px; font-size: 11px; color: #64748b;
    background: transparent; border: none; cursor: pointer; transition: background .15s;
}
.benchmark-btn.active { background: #1d4ed8; color: #fff; }
```

- [ ] **Step 2: Add breadcrumb HTML (above sector toggles)**

Find in `rotation.html`:
```html
        <!-- Sector visibility toggles -->
        <div class="sector-toggles" id="sectorToggles" style="display:none;"></div>
```

Replace with:
```html
        <!-- Drill-down breadcrumb (shown in industry mode) -->
        <div class="drill-breadcrumb" id="drillBreadcrumb" style="display:none;">
            <span>Sector Rotation</span>
            <span>›</span>
            <span class="drill-sector" id="drillSectorLabel"></span>
            <span id="drillBack" class="drill-back">← 返回</span>
        </div>

        <!-- Sector visibility toggles (shown in sector mode) -->
        <div class="sector-toggles" id="sectorToggles" style="display:none;"></div>
```

- [ ] **Step 3: Add benchmark toggle to control bar**

Find in `rotation.html`:
```html
            <div class="d-flex gap-1">
                <button class="range-btn" data-weeks="13">3M</button>
                <button class="range-btn" data-weeks="26">6M</button>
                <button class="range-btn active" data-weeks="52">1Y</button>
                <button class="range-btn" data-weeks="0">全部</button>
            </div>
        </div>
```

Replace with:
```html
            <div class="d-flex gap-1">
                <button class="range-btn" data-weeks="13">3M</button>
                <button class="range-btn" data-weeks="26">6M</button>
                <button class="range-btn active" data-weeks="52">1Y</button>
                <button class="range-btn" data-weeks="0">全部</button>
            </div>

            <div class="benchmark-toggle" id="benchmarkToggle" style="display:none;">
                <button class="benchmark-btn active" data-bm="market">vs 全市場</button>
                <button class="benchmark-btn" data-bm="sector">vs Sector</button>
            </div>
        </div>
```

- [ ] **Step 4: Verify HTML renders without JS errors**

Open `http://localhost:8087/rotation.html` — page should load normally, no console errors. The new elements are hidden so nothing looks different.

- [ ] **Step 5: Commit**

```bash
git add rotation.html
git commit -m "feat: rotation.html — add drill-down breadcrumb and benchmark toggle HTML/CSS"
```

---

## Task 4: `rotation.html` — Drill-down JS logic

**Files:**
- Modify: `rotation.html`

This task adds all JS for drill-down: state vars, color helpers, data normalisation, enterDrill/exitDrill, industry toggles, benchmark switching, and adapts existing functions.

- [ ] **Step 1: Add drill-down state variables**

Find in `rotation.html`:
```js
    let visibleSectors = new Set(Object.keys(SECTOR_COLORS));
```

Replace with:
```js
    let visibleSectors = new Set(Object.keys(SECTOR_COLORS));
    let drillSector = null;          // null = sector view; string = industry drill-down
    let drillBenchmark = 'market';   // 'market' | 'sector'
    let drillSnapshots = [];         // raw industry snapshots from API
    let visibleIndustries = new Set();
```

- [ ] **Step 2: Add color helpers**

Find in `rotation.html`:
```js
    function fmtReturn(v) {
```

Insert BEFORE that line:

```js
    function industryColor(name) {
        // Deterministic HSL color from name hash
        let hash = 0;
        for (let i = 0; i < name.length; i++) hash = (hash * 31 + name.charCodeAt(i)) & 0xffffffff;
        const h = Math.abs(hash) % 360;
        return `hsl(${h}, 65%, 60%)`;
    }

    function getItemColor(name) {
        return SECTOR_COLORS[name] || industryColor(name);
    }

    function industryToSectorFormat(snap, benchmark) {
        // Normalise industry snapshot into the same shape as a sector snapshot
        // so buildSeries / drawTrails / renderTable work without changes.
        return {
            ...snap,
            sectors: snap.industries.map(ind => ({
                sector:       ind.industry,
                rs_ratio:     benchmark === 'market' ? ind.rs_ratio_market   : ind.rs_ratio_sector,
                rs_momentum:  benchmark === 'market' ? ind.rs_momentum_market : ind.rs_momentum_sector,
                quadrant:     benchmark === 'market' ? ind.quadrant_market    : ind.quadrant_sector,
                return_13w:   ind.return_13w,
                return_4w:    ind.return_4w,
            })),
        };
    }
```

- [ ] **Step 3: Adapt `buildSeries` to handle drill mode**

Find:
```js
    function buildSeries(sectors) {
        return ['Leading', 'Weakening', 'Lagging', 'Improving'].map(q => ({
            name: q,
            data: sectors.filter(s => s.quadrant === q && visibleSectors.has(s.sector)).map(s => ({
                x: s.rs_ratio, y: s.rs_ratio,
                label: s.sector, return_13w: s.return_13w, return_4w: s.return_4w,
            })),
        }));
    }
```

Note: the actual code has `y: s.rs_momentum` not `y: s.rs_ratio`. Replace with:

```js
    function buildSeries(sectors) {
        const visible = drillSector ? visibleIndustries : visibleSectors;
        return ['Leading', 'Weakening', 'Lagging', 'Improving'].map(q => ({
            name: q,
            data: sectors.filter(s => s.quadrant === q && visible.has(s.sector)).map(s => ({
                x: s.rs_ratio, y: s.rs_momentum,
                label: s.sector, return_13w: s.return_13w, return_4w: s.return_4w,
            })),
        }));
    }
```

- [ ] **Step 4: Adapt `drawTrails` to use `getItemColor` and drill-mode visibility**

Find:
```js
        Object.keys(SECTOR_COLORS).filter(s => visibleSectors.has(s)).forEach(sector => {
            const points = trail.map(snap => {
                const s = snap.sectors.find(x => x.sector === sector);
                return s ? dataToSvgPx(s.rs_ratio, s.rs_momentum) : null;
            }).filter(Boolean);

            if (points.length < 2) return;
            const color = SECTOR_COLORS[sector];
```

Replace with:
```js
        const itemNames = drillSector
            ? (playSnapshots[0]?.sectors || []).map(s => s.sector).filter(n => visibleIndustries.has(n))
            : Object.keys(SECTOR_COLORS).filter(s => visibleSectors.has(s));

        itemNames.forEach(sector => {
            const points = trail.map(snap => {
                const s = snap.sectors.find(x => x.sector === sector);
                return s ? dataToSvgPx(s.rs_ratio, s.rs_momentum) : null;
            }).filter(Boolean);

            if (points.length < 2) return;
            const color = getItemColor(sector);
```

- [ ] **Step 5: Adapt `smoothSnapshots` to work on `s.sector` field (already correct — no change needed)**

Verify that `smoothSnapshots` in the file reads `s.sector` (not a hardcoded field). It already does:
```js
        const sectorNames = snaps[0].sectors.map(s => s.sector);
```
After `industryToSectorFormat`, industry names are in `s.sector`, so smoothing works automatically. No change needed.

- [ ] **Step 6: Change sector pill behavior — click → enterDrill**

Find `initSectorToggles()`:
```js
    function initSectorToggles() {
        const container = document.getElementById('sectorToggles');
        container.innerHTML = '';
        Object.entries(SECTOR_COLORS).forEach(([sector, color]) => {
            const pill = document.createElement('div');
            pill.className = 'sector-pill active';
            pill.dataset.sector = sector;
            pill.innerHTML = `<span class="pill-dot" style="background:${color}"></span>${sector}`;
            pill.addEventListener('click', () => {
                if (visibleSectors.has(sector)) {
                    visibleSectors.delete(sector);
                    pill.classList.remove('active');
                } else {
                    visibleSectors.add(sector);
                    pill.classList.add('active');
                }
                if (chart) chart.updateSeries(buildSeries(playSnapshots[currentIdx].sectors), false);
                drawTrails();
                renderEtfChart();
            });
            container.appendChild(pill);
        });
        container.style.display = '';
    }
```

Replace entirely with:
```js
    function initSectorToggles() {
        const container = document.getElementById('sectorToggles');
        container.innerHTML = '';
        Object.entries(SECTOR_COLORS).forEach(([sector, color]) => {
            const pill = document.createElement('div');
            pill.className = 'sector-pill active';
            pill.dataset.sector = sector;
            pill.title = `Click to drill into ${sector} industries`;
            pill.innerHTML = `<span class="pill-dot" style="background:${color}"></span>${sector} ▾`;
            pill.addEventListener('click', () => enterDrill(sector));
            container.appendChild(pill);
        });
        container.style.display = '';
    }
```

- [ ] **Step 7: Add `initIndustryToggles(industries)`**

Find `function initControls() {` and insert BEFORE it:

```js
    function initIndustryToggles(industries) {
        const container = document.getElementById('sectorToggles');
        container.innerHTML = '';
        industries.forEach(name => {
            const color = getItemColor(name);
            const pill = document.createElement('div');
            pill.className = 'sector-pill active';
            pill.dataset.sector = name;
            pill.innerHTML = `<span class="pill-dot" style="background:${color}"></span>${name}`;
            pill.addEventListener('click', () => {
                if (visibleIndustries.has(name)) {
                    visibleIndustries.delete(name);
                    pill.classList.remove('active');
                } else {
                    visibleIndustries.add(name);
                    pill.classList.add('active');
                }
                if (chart) chart.updateSeries(buildSeries(playSnapshots[currentIdx].sectors), false);
                drawTrails();
            });
            container.appendChild(pill);
        });
        container.style.display = '';
    }
```

- [ ] **Step 8: Add `pollIndustryReady`, `enterDrill`, `exitDrill`**

Insert the following block BEFORE `function initControls() {`:

```js
    async function pollIndustryReady(sector) {
        const encoded = encodeURIComponent(sector);
        for (let attempt = 0; attempt < 30; attempt++) {
            await new Promise(r => setTimeout(r, attempt === 0 ? 0 : 10000));
            const res = await fetch(`/api/industry-rotation-history?sector=${encoded}`);
            if (res.status === 200) return res.json();
            if (res.status !== 202) throw new Error('API error ' + res.status);
            document.getElementById('loadingMsg').textContent =
                `${sector} industry 資料計算中… (${attempt * 10}s)`;
        }
        throw new Error('Timed out waiting for industry data');
    }

    async function enterDrill(sector) {
        stopPlay();
        drillSector = sector;
        drillBenchmark = 'market';

        // Update UI
        document.getElementById('drillSectorLabel').textContent = sector;
        document.getElementById('drillBreadcrumb').style.display = '';
        document.getElementById('sectorToggles').style.display = 'none';
        document.getElementById('etfChartCard').style.display = 'none';
        document.getElementById('benchmarkToggle').style.display = '';
        document.querySelectorAll('.benchmark-btn').forEach(b => {
            b.classList.toggle('active', b.dataset.bm === 'market');
        });

        // Show spinner inside the RRG card
        document.getElementById('loadingMsg').textContent = `${sector} industry 資料載入中…`;
        document.getElementById('loadingSpinner').style.display = '';

        try {
            const data = await pollIndustryReady(sector);
            drillSnapshots = data.snapshots || [];
            if (drillSnapshots.length === 0) throw new Error('No industry snapshots');

            // Normalise to sector format using current benchmark
            const normalised = smoothSnapshots(
                drillSnapshots.map(s => industryToSectorFormat(s, drillBenchmark))
            );
            allSnapshots = normalised;
            playSnapshots = normalised.length > 52 ? normalised.slice(-52) : normalised;
            currentIdx = playSnapshots.length - 1;

            const industryNames = drillSnapshots[0].industries.map(i => i.industry);
            visibleIndustries = new Set(industryNames);

            document.getElementById('loadingSpinner').style.display = 'none';
            initIndustryToggles(industryNames);

            // Re-init chart with new axis range
            if (chart) { chart.destroy(); chart = null; chartGlobals = null; }
            initChart(playSnapshots[currentIdx]);
            renderFrame(currentIdx);
        } catch (e) {
            document.getElementById('loadingSpinner').innerHTML =
                `<div class="text-center text-danger py-4">Failed: ${e.message} — <span style="cursor:pointer;color:#60a5fa;" onclick="exitDrill()">返回</span></div>`;
        }
    }

    function exitDrill() {
        stopPlay();
        drillSector = null;
        drillBenchmark = 'market';
        drillSnapshots = [];
        visibleIndustries = new Set();

        // Restore UI
        document.getElementById('drillBreadcrumb').style.display = 'none';
        document.getElementById('benchmarkToggle').style.display = 'none';
        document.getElementById('etfChartCard').style.display = '';

        // Reload sector snapshots (stored in outer allSnapshots before drill)
        // They were overwritten — we need to reload from the original data
        // Re-fetch from API (cache hit, instant)
        fetch('/api/sector-rotation-history').then(r => r.json()).then(data => {
            allSnapshots = smoothSnapshots(data.snapshots || []);
            playSnapshots = allSnapshots.length > 52 ? allSnapshots.slice(-52) : allSnapshots;
            currentIdx = playSnapshots.length - 1;

            if (chart) { chart.destroy(); chart = null; chartGlobals = null; }
            initSectorToggles();
            initChart(playSnapshots[currentIdx]);
            renderFrame(currentIdx);
            renderEtfChart();
        });
    }
```

- [ ] **Step 9: Wire up benchmark toggle and ← 返回 button**

Find in `initControls()`:
```js
        initTimelineDrag();
    }
```

Replace with:
```js
        initTimelineDrag();

        document.getElementById('drillBack').addEventListener('click', exitDrill);

        document.getElementById('benchmarkToggle').addEventListener('click', e => {
            const btn = e.target.closest('[data-bm]');
            if (!btn || !drillSector || !drillSnapshots.length) return;
            drillBenchmark = btn.dataset.bm;
            document.querySelectorAll('.benchmark-btn').forEach(b => {
                b.classList.toggle('active', b.dataset.bm === drillBenchmark);
            });
            const normalised = smoothSnapshots(
                drillSnapshots.map(s => industryToSectorFormat(s, drillBenchmark))
            );
            allSnapshots = normalised;
            playSnapshots = normalised.length > 52 ? normalised.slice(-52) : normalised;
            currentIdx = playSnapshots.length - 1;
            axisRange = computeAxisRange(allSnapshots);
            if (chart) chart.updateSeries(buildSeries(playSnapshots[currentIdx].sectors), false);
            drawTrails();
            renderFrame(currentIdx);
        });
    }
```

- [ ] **Step 10: Verify in browser**

Open `http://localhost:8087/rotation.html`:
1. Page loads normally with sector view
2. Sector pills now show `▾` suffix and tooltip
3. Click **Energy** → spinner appears → after ~30s industry view loads
4. Breadcrumb shows "Sector Rotation › Energy ← 返回"
5. Industry pills appear (coloured, all active)
6. RRG shows Energy's sub-industries
7. Toggle [vs 全市場] / [vs Sector] — chart updates
8. Click ← 返回 → sector view restores instantly (cache hit)

- [ ] **Step 11: Run full test suite**

```bash
venv/bin/python -m pytest tests/ -v -q
```
Expected: all 12 tests PASS

- [ ] **Step 12: Commit**

```bash
git add rotation.html
git commit -m "feat: industry drill-down RRG — enter/exit drill, benchmark toggle, industry pills"
```

---

## Task 5: Final verification and push

- [ ] **Step 1: Run full test suite**

```bash
venv/bin/python -m pytest tests/ -v
```
Expected: 12 tests PASS

- [ ] **Step 2: Manual smoke test**

```bash
curl -s "http://localhost:8000/api/industry-rotation-history?sector=Energy" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print('Status:', d.get('status', 'ok'))
print('Sector:', d.get('sector'))
print('Snapshots:', d.get('total_snapshots'))
"
```
Expected: either `{"status": "generating", ...}` (202) or sector/snapshots printed.

```bash
curl -s "http://localhost:8000/api/industry-rotation-history?sector=FakeSector" | python3 -c "import sys,json; print(json.load(sys.stdin))"
```
Expected: `{"detail": "Unknown sector: 'FakeSector'..."}` with HTTP 400.

- [ ] **Step 3: Push**

```bash
git push
```
