# Stock RRG Position Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a mini RRG context card to `stock.html` that shows the current RRG quadrant and RS values for the stock's sector and industry.

**Architecture:** New API endpoint `GET /api/stock-rrg-position?ticker=AAPL` reads the ticker's industry from the DB, maps it to a GICS sector via `GICS_MAP`, then reads the latest sector rotation history cache and industry rotation history cache to return the current RS-Ratio, RS-Momentum, and quadrant for both the sector and the industry. `stock.html` fetches this endpoint and renders a small card below the price chart.

**Tech Stack:** FastAPI, PostgreSQL (read-only), Python pathlib (cache read), Bootstrap 5 dark, vanilla JS fetch.

---

## Codebase Context

**DB schema:**
- `tickers(ticker PK, industry_id FK, company_name, ...)`
- `industries(id PK, name)`
- Industry `name` matches keys in `GICS_MAP` from `generate_rotation_history.py`

**`generate_rotation_history.py`** exports:
- `GICS_MAP` — `{ industry_name: sector_name_or_None }` (145 entries; Shell Companies → None)
- `ALL_SECTORS` — list of 11 sector strings

**Cache locations:**
- Sector: `/tmp/sector_rotation_history_{today}.json` → shape: `{ snapshots: [{ date, sectors: [{ sector, rs_ratio, rs_momentum, quadrant, return_13w, return_4w }] }] }`
- Industry: `/tmp/industry_rotation_history_{slug}_{today}.json` → shape: `{ sector, snapshots: [{ date, industries: [{ industry, rs_ratio_market, rs_momentum_market, quadrant_market, rs_ratio_sector, rs_momentum_sector, quadrant_sector, return_13w, return_4w, stock_count }] }] }`
- Slug formula: `sector.lower().replace(" ", "_").replace("&", "and")`

**`api.py`** patterns:
- DB access: `with db.engine.connect() as conn: result = conn.execute(text(...), {...}).fetchone()`
- Module-level import: `from generate_rotation_history import GICS_MAP, ALL_SECTORS` (already imported as `_ROTATION_ALL_SECTORS`)
- Existing ticker endpoint: `GET /api/stock/{ticker}` at line 279

**`stock.html`** patterns:
- Gets ticker from `?ticker=AAPL` URL param (line 327)
- `loadStockData(ticker)` at line 341 — async, fetches `${API_BASE_URL}/api/stock/${symbol}`
- `API_BASE_URL` is set as a JS constant near the top of the script block
- Price chart card is rendered via JS into the DOM
- Dark theme via `[data-theme="dark"]`

---

## File Map

| File | Action |
|------|--------|
| `api.py` | Modify — add `GET /api/stock-rrg-position` endpoint |
| `stock.html` | Modify — fetch RRG position, render mini card |
| `tests/test_stock_rrg.py` | Create |

---

## Task 1: `GET /api/stock-rrg-position` endpoint

**Files:**
- Modify: `api.py` — append before `if __name__ == "__main__":` block
- Create: `tests/test_stock_rrg.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_stock_rrg.py`:

```python
import datetime
import json
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest
from fastapi.testclient import TestClient
from api import app

client = TestClient(app)
TODAY = datetime.date.today().isoformat()


def _slug(s):
    return s.lower().replace(" ", "_").replace("&", "and")


def _write_sector_cache(snapshots):
    p = Path(f"/tmp/sector_rotation_history_{TODAY}.json")
    p.write_text(json.dumps({"snapshots": snapshots}))
    return p


def _write_industry_cache(sector, industries_per_snap):
    slug = _slug(sector)
    p = Path(f"/tmp/industry_rotation_history_{slug}_{TODAY}.json")
    snapshots = [{"date": TODAY, "week_index": 0, "industries": industries_per_snap}]
    p.write_text(json.dumps({"sector": sector, "total_snapshots": 1, "snapshots": snapshots}))
    return p


def _clear_caches():
    Path(f"/tmp/sector_rotation_history_{TODAY}.json").unlink(missing_ok=True)
    from generate_rotation_history import ALL_SECTORS
    for s in ALL_SECTORS:
        Path(f"/tmp/industry_rotation_history_{_slug(s)}_{TODAY}.json").unlink(missing_ok=True)


def test_returns_404_for_unknown_ticker():
    res = client.get("/api/stock-rrg-position?ticker=ZZZZNOTREAL")
    assert res.status_code == 404


def test_returns_400_without_ticker():
    res = client.get("/api/stock-rrg-position")
    assert res.status_code == 422  # FastAPI validation


def test_returns_sector_rrg_from_cache():
    _clear_caches()
    _write_sector_cache([{
        "date": TODAY,
        "week_index": 0,
        "sectors": [{"sector": "Energy", "rs_ratio": 103.5, "rs_momentum": 101.2, "quadrant": "Leading", "return_13w": 5.1, "return_4w": 1.3}],
    }])
    _write_industry_cache("Energy", [{
        "industry": "Oil, Gas & Consumable Fuels",
        "rs_ratio_market": 104.0, "rs_momentum_market": 102.0, "quadrant_market": "Leading",
        "rs_ratio_sector": 101.0, "rs_momentum_sector": 100.5, "quadrant_sector": "Leading",
        "return_13w": 5.0, "return_4w": 1.2, "stock_count": 40,
    }])

    # Patch DB lookup to return a known industry
    with patch("api.db") as mock_db:
        mock_conn = MagicMock()
        mock_db.engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db.engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.execute.return_value.fetchone.return_value = ("Oil, Gas & Consumable Fuels",)

        res = client.get("/api/stock-rrg-position?ticker=XOM")

    assert res.status_code == 200
    data = res.json()
    assert data["sector"] == "Energy"
    assert data["industry"] == "Oil, Gas & Consumable Fuels"
    assert "sector_rrg" in data
    assert data["sector_rrg"]["quadrant"] == "Leading"
    assert "industry_rrg" in data
    assert data["industry_rrg"]["quadrant_market"] == "Leading"
    _clear_caches()


def test_returns_202_when_cache_missing():
    _clear_caches()
    with patch("api.db") as mock_db:
        mock_conn = MagicMock()
        mock_db.engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db.engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.execute.return_value.fetchone.return_value = ("Oil, Gas & Consumable Fuels",)
        with patch("subprocess.Popen"):
            res = client.get("/api/stock-rrg-position?ticker=XOM")
    assert res.status_code in (200, 202)
    _clear_caches()
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
venv/bin/python -m pytest tests/test_stock_rrg.py -v
```
Expected: all FAIL (endpoint doesn't exist yet)

- [ ] **Step 3: Implement the endpoint in `api.py`**

Find the line `if __name__ == "__main__":` at the bottom of `api.py`. Add the following BEFORE it:

```python
@app.get("/api/stock-rrg-position")
def get_stock_rrg_position(ticker: str):
    """
    Returns the current RRG position (sector + industry) for a given ticker.
    Reads from today's cached rotation history files.
    Returns 200 with data if caches exist, 202 if industry cache is being generated.
    """
    import json as _json
    import datetime
    import subprocess
    import sys
    from pathlib import Path
    from fastapi.responses import JSONResponse
    from generate_rotation_history import GICS_MAP

    ticker = ticker.upper()

    # 1. Look up industry from DB
    with db.engine.connect() as conn:
        row = conn.execute(
            text("""
                SELECT i.name
                FROM tickers t
                JOIN industries i ON t.industry_id = i.id
                WHERE t.ticker = :ticker
            """),
            {"ticker": ticker},
        ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail=f"Ticker {ticker!r} not found")

    industry_name = row[0]
    sector_name = GICS_MAP.get(industry_name)
    if not sector_name:
        raise HTTPException(status_code=404, detail=f"No GICS sector for industry {industry_name!r}")

    def _slug(s):
        return s.lower().replace(" ", "_").replace("&", "and")

    today = datetime.date.today().isoformat()

    # 2. Read sector RRG from cache
    sector_cache = Path(f"/tmp/sector_rotation_history_{today}.json")
    sector_rrg = None
    if sector_cache.exists():
        try:
            data = _json.loads(sector_cache.read_text())
            snaps = data.get("snapshots", [])
            if snaps:
                last = snaps[-1]
                for s in last.get("sectors", []):
                    if s["sector"] == sector_name:
                        sector_rrg = {
                            "rs_ratio": s["rs_ratio"],
                            "rs_momentum": s["rs_momentum"],
                            "quadrant": s["quadrant"],
                            "return_13w": s["return_13w"],
                            "return_4w": s["return_4w"],
                            "date": last["date"],
                        }
                        break
        except (ValueError, OSError):
            pass

    # 3. Read industry RRG from cache
    ind_cache = Path(f"/tmp/industry_rotation_history_{_slug(sector_name)}_{today}.json")
    industry_rrg = None
    if ind_cache.exists():
        try:
            data = _json.loads(ind_cache.read_text())
            snaps = data.get("snapshots", [])
            if snaps:
                last = snaps[-1]
                for ind in last.get("industries", []):
                    if ind["industry"] == industry_name:
                        industry_rrg = {
                            "rs_ratio_market": ind["rs_ratio_market"],
                            "rs_momentum_market": ind["rs_momentum_market"],
                            "quadrant_market": ind["quadrant_market"],
                            "rs_ratio_sector": ind["rs_ratio_sector"],
                            "rs_momentum_sector": ind["rs_momentum_sector"],
                            "quadrant_sector": ind["quadrant_sector"],
                            "return_13w": ind["return_13w"],
                            "return_4w": ind["return_4w"],
                            "stock_count": ind["stock_count"],
                            "date": last["date"],
                        }
                        break
        except (ValueError, OSError):
            pass
    elif sector_name in _ROTATION_ALL_SECTORS:
        # Trigger background generation
        script = Path(__file__).parent / "generate_industry_rotation_history.py"
        subprocess.Popen(
            [sys.executable, str(script), "--sector", sector_name],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )

    if not sector_rrg and not industry_rrg:
        return JSONResponse(status_code=202, content={
            "status": "generating",
            "sector": sector_name,
            "industry": industry_name,
        })

    return {
        "ticker": ticker,
        "sector": sector_name,
        "industry": industry_name,
        "sector_rrg": sector_rrg,
        "industry_rrg": industry_rrg,
    }
```

- [ ] **Step 4: Restart API and run tests**

```bash
sudo systemctl restart stock-api && sleep 2
venv/bin/python -m pytest tests/test_stock_rrg.py -v
```
Expected: all 4 tests PASS

- [ ] **Step 5: Commit**

```bash
git add api.py tests/test_stock_rrg.py
git commit -m "feat: GET /api/stock-rrg-position endpoint"
```

---

## Task 2: RRG position mini-card in `stock.html`

**Files:**
- Modify: `stock.html`

Add a "RRG Position" card that appears below the price chart when the endpoint returns data.

- [ ] **Step 1: Add CSS for RRG position card**

In `stock.html`, find the `<style>` block. Add before `</style>`:

```css
        /* RRG position card */
        .rrg-card { background: var(--bg-card); border: 1px solid var(--border-color); border-radius: 12px; padding: 16px 20px; margin-bottom: 16px; }
        .rrg-card-title { font-size: 0.8rem; font-weight: 600; color: var(--text-secondary); text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 12px; }
        .rrg-row { display: flex; gap: 12px; flex-wrap: wrap; }
        .rrg-block { flex: 1; min-width: 140px; background: var(--bg-body); border: 1px solid var(--border-color); border-radius: 8px; padding: 10px 14px; }
        .rrg-block-label { font-size: 0.72rem; color: var(--text-secondary); margin-bottom: 4px; }
        .rrg-block-name { font-size: 0.85rem; font-weight: 600; color: var(--text-primary); margin-bottom: 8px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
        .rrg-quadrant { display: inline-block; padding: 2px 10px; border-radius: 12px; font-size: 0.75rem; font-weight: 700; }
        .rrg-quadrant.Leading  { background: rgba(74,222,128,0.15); color: #4ade80; }
        .rrg-quadrant.Weakening{ background: rgba(251,146,60,0.15); color: #fb923c; }
        .rrg-quadrant.Lagging  { background: rgba(248,113,113,0.15); color: #f87171; }
        .rrg-quadrant.Improving{ background: rgba(96,165,250,0.15); color: #60a5fa; }
        .rrg-stat { font-size: 0.75rem; color: var(--text-secondary); margin-top: 6px; }
        .rrg-stat span { color: var(--text-primary); font-weight: 500; }
        .rrg-link { font-size: 0.75rem; color: var(--accent-blue); text-decoration: none; margin-top: 8px; display: inline-block; }
        .rrg-link:hover { text-decoration: underline; }
```

- [ ] **Step 2: Add RRG card placeholder in HTML**

In `stock.html`, find the main content area. Look for the price chart card (there should be a `<div id="priceChartCard">` or similar). Find where the price chart is rendered and add the RRG card placeholder after the price chart container. Search for `id="priceChart"` or the card that contains it.

Read `stock.html` lines 150-250 to find the right location, then add after the price chart card:

```html
        <!-- RRG Position Card (populated by JS) -->
        <div id="rrgPositionCard" style="display:none;"></div>
```

Place this after the price chart section but before the fundamentals section.

- [ ] **Step 3: Add `loadRrgPosition` function to stock.html JS**

In `stock.html`, find `async function loadStockData(symbol) {`. After the existing `loadStockData` function closes, add:

```js
        async function loadRrgPosition(ticker) {
            try {
                const res = await fetch(`${API_BASE_URL}/api/stock-rrg-position?ticker=${encodeURIComponent(ticker)}`);
                if (!res.ok) return; // 404 or 202 — just skip silently
                const data = await res.json();
                if (data.status === 'generating') return; // still computing
                renderRrgPosition(data);
            } catch (e) {
                // Network error — skip silently
            }
        }

        function renderRrgPosition(data) {
            const card = document.getElementById('rrgPositionCard');
            if (!card) return;

            const quadrantBadge = (q) => q ? `<span class="rrg-quadrant ${q}">${q}</span>` : '—';
            const fmtVal = (v) => v != null ? v.toFixed(2) : '—';
            const fmtRet = (v) => v != null ? `${v >= 0 ? '+' : ''}${v.toFixed(1)}%` : '—';

            const sr = data.sector_rrg;
            const ir = data.industry_rrg;

            card.innerHTML = `
                <div class="rrg-card">
                    <div class="rrg-card-title">RRG Position</div>
                    <div class="rrg-row">
                        <div class="rrg-block">
                            <div class="rrg-block-label">Sector</div>
                            <div class="rrg-block-name" title="${data.sector}">${data.sector}</div>
                            ${sr ? `
                                ${quadrantBadge(sr.quadrant)}
                                <div class="rrg-stat">RS-Ratio: <span>${fmtVal(sr.rs_ratio)}</span></div>
                                <div class="rrg-stat">RS-Mom: <span>${fmtVal(sr.rs_momentum)}</span></div>
                                <div class="rrg-stat">13W: <span>${fmtRet(sr.return_13w)}</span> &nbsp; 4W: <span>${fmtRet(sr.return_4w)}</span></div>
                            ` : '<div class="rrg-stat" style="color:#64748b;">No data yet</div>'}
                        </div>
                        <div class="rrg-block">
                            <div class="rrg-block-label">Industry</div>
                            <div class="rrg-block-name" title="${data.industry}">${data.industry}</div>
                            ${ir ? `
                                ${quadrantBadge(ir.quadrant_market)}
                                <div class="rrg-stat">RS-Ratio: <span>${fmtVal(ir.rs_ratio_market)}</span></div>
                                <div class="rrg-stat">RS-Mom: <span>${fmtVal(ir.rs_momentum_market)}</span></div>
                                <div class="rrg-stat">13W: <span>${fmtRet(ir.return_13w)}</span> &nbsp; 4W: <span>${fmtRet(ir.return_4w)}</span></div>
                            ` : '<div class="rrg-stat" style="color:#64748b;">No data yet</div>'}
                        </div>
                    </div>
                    <a href="rotation.html" class="rrg-link">View Sector Rotation →</a>
                </div>`;
            card.style.display = '';
        }
```

- [ ] **Step 4: Call `loadRrgPosition` from the page init**

Find in `stock.html` where `loadStockData(ticker)` is called (around line 335):
```js
            loadStockData(ticker);
```

Replace with:
```js
            loadStockData(ticker);
            loadRrgPosition(ticker);
```

- [ ] **Step 5: Verify in browser**

Open `http://localhost:8087/stock.html?ticker=XOM`:
1. After price chart, a "RRG Position" card appears
2. Shows Energy sector quadrant + RS values
3. Shows Oil, Gas & Consumable Fuels industry quadrant + RS values
4. "View Sector Rotation →" link navigates to rotation.html
5. For a ticker with no cached industry data, the industry block shows "No data yet" without crashing

- [ ] **Step 6: Commit**

```bash
git add stock.html
git commit -m "feat: RRG position card on stock.html"
```

---

## Task 3: Final verification and push

- [ ] **Step 1: Run all tests**

```bash
venv/bin/python -m pytest tests/ -v -k "not generates_cache"
```
Expected: all pass

- [ ] **Step 2: Smoke-test the endpoint**

```bash
curl -s "http://localhost:8000/api/stock-rrg-position?ticker=AAPL" | python3 -m json.tool | head -20
```
Expected: JSON with `sector`, `industry`, `sector_rrg`, `industry_rrg` fields (or 202 if caches not yet warm)

- [ ] **Step 3: Push**

```bash
git push
```
