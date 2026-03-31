# Home Page GICS Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the existing Bootstrap-based home page with a shadcn/ui-inspired zinc-dark dashboard showing 11 GICS sectors, date-range-driven performance, top movers, and sub-industry drill-down.

**Architecture:** A new `/api/gics-overview?period=1D` FastAPI endpoint computes per-industry % change and weighted PE from local PostgreSQL. The frontend (`index.html`) maps DB industry names to 11 GICS sectors via a JS config object, aggregates stats at the sector level, and renders everything with Tailwind CDN — no build step.

**Tech Stack:** FastAPI + SQLAlchemy (existing), PostgreSQL 17 (local), Tailwind CSS CDN, Vanilla JS, Inter font (Google Fonts)

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `api.py` | Modify | Add `PERIOD_MAP` constant and `/api/gics-overview` endpoint |
| `tests/test_gics_overview.py` | Create | Integration tests for the new endpoint via FastAPI TestClient |
| `index.html` | Replace | Full redesign: navbar, period selector, movers bar, sector grid, drill-down panel |

---

## Task 1: Add `/api/gics-overview` endpoint

**Files:**
- Modify: `api.py` (add after existing imports + before `@app.get("/")`)
- Create: `tests/test_gics_overview.py`

- [ ] **Step 1: Create tests directory and write failing test**

```bash
mkdir -p /home/ubuntu/stock-market-dashboard/tests
touch /home/ubuntu/stock-market-dashboard/tests/__init__.py
```

Create `/home/ubuntu/stock-market-dashboard/tests/test_gics_overview.py`:

```python
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi.testclient import TestClient
from api import app

client = TestClient(app)


def test_gics_overview_default_period():
    response = client.get("/api/gics-overview")
    assert response.status_code == 200
    data = response.json()
    assert "items" in data
    assert isinstance(data["items"], list)


def test_gics_overview_returns_industry_fields():
    response = client.get("/api/gics-overview?period=1W")
    assert response.status_code == 200
    items = response.json()["items"]
    if items:
        item = items[0]
        assert "industry" in item
        assert "pct_change" in item
        assert "avg_pe" in item
        assert "stock_count" in item


def test_gics_overview_invalid_period_returns_400():
    response = client.get("/api/gics-overview?period=INVALID")
    assert response.status_code == 400


def test_gics_overview_all_valid_periods():
    for period in ["1D", "1W", "1M", "3M", "6M", "1Y", "5Y"]:
        response = client.get(f"/api/gics-overview?period={period}")
        assert response.status_code == 200, f"Failed for period={period}"
        assert "items" in response.json()
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /home/ubuntu/stock-market-dashboard
pip install pytest httpx -q
python -m pytest tests/test_gics_overview.py -v 2>&1 | head -30
```

Expected: FAIL — `404 Not Found` on `/api/gics-overview`.

- [ ] **Step 3: Add PERIOD_MAP and endpoint to `api.py`**

Add `PERIOD_MAP` right after the existing imports block (after `load_dotenv()`, before `app = FastAPI(...)`):

```python
PERIOD_MAP = {
    "1D": 1,
    "1W": 7,
    "1M": 30,
    "3M": 90,
    "6M": 180,
    "1Y": 365,
    "5Y": 1825,
}
```

Add the endpoint at the end of `api.py`, just before `if __name__ == "__main__":`:

```python
@app.get("/api/gics-overview")
def get_gics_overview(period: str = "1D"):
    """
    Returns per-industry performance and weighted PE for the selected period.
    Period is defined as: current price vs closest available price N days ago.
    Response: { "period": str, "items": [{ "industry", "pct_change", "avg_pe", "stock_count" }] }
    """
    if period not in PERIOD_MAP:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid period '{period}'. Must be one of: {list(PERIOD_MAP.keys())}"
        )

    days = PERIOD_MAP[period]

    try:
        with db.engine.connect() as conn:
            query = text("""
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
                    WHERE p.date <= end_date.d - INTERVAL '1 day' * :days
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
            """, {"days": days})

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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /home/ubuntu/stock-market-dashboard
python -m pytest tests/test_gics_overview.py -v
```

Expected output:
```
tests/test_gics_overview.py::test_gics_overview_default_period PASSED
tests/test_gics_overview.py::test_gics_overview_returns_industry_fields PASSED
tests/test_gics_overview.py::test_gics_overview_invalid_period_returns_400 PASSED
tests/test_gics_overview.py::test_gics_overview_all_valid_periods PASSED

4 passed in Xs
```

- [ ] **Step 5: Restart service and smoke-test via curl**

```bash
sudo systemctl restart stock-api
sleep 3
curl -s "http://localhost:8000/api/gics-overview?period=1D" | python3 -c "import sys,json; d=json.load(sys.stdin); print('items:', len(d['items'])); print('first:', d['items'][0] if d['items'] else 'empty')"
```

Expected: prints item count (100–145) and first item with `industry`, `pct_change`, `avg_pe`, `stock_count`.

- [ ] **Step 6: Commit**

```bash
cd /home/ubuntu/stock-market-dashboard
git add api.py tests/
git commit -m "feat: add /api/gics-overview endpoint with period-based industry performance"
```

---

## Task 2: Build new `index.html`

**Files:**
- Replace: `index.html`

> **Note on GICS industry name matching:** The `INDUSTRY_TO_GICS` map below uses exact DB industry names (yfinance format, em-dashes `—`). After first load, open browser console — any warning `[GICS] Unmatched:` lists DB names not in the map; add them to the correct sector in `GICS_SECTORS`.

- [ ] **Step 1: Replace `index.html` with the complete new file**

Write the following as `/home/ubuntu/stock-market-dashboard/index.html`:

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Market Dashboard</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet" />
  <style>
    body { font-family: 'Inter', sans-serif; }
    .drill-panel { transition: opacity 0.2s ease; }
  </style>
</head>
<body class="bg-[#09090b] text-[#fafafa] min-h-screen">

  <!-- Navbar -->
  <nav class="border-b border-zinc-800 bg-[#09090b] sticky top-0 z-20">
    <div class="max-w-7xl mx-auto px-4 py-3 flex items-center justify-between">
      <span class="font-semibold text-sm tracking-tight">Market Dashboard</span>
      <span id="last-updated" class="text-zinc-600 text-xs"></span>
    </div>
  </nav>

  <div class="max-w-7xl mx-auto px-4 py-5 space-y-4">

    <!-- Period Selector -->
    <div id="period-selector" class="flex items-center gap-1.5 flex-wrap"></div>

    <!-- Top Movers Bar -->
    <div class="flex items-center gap-3 overflow-x-auto bg-[#18181b] border border-zinc-800 rounded-lg px-4 py-2.5 min-h-[40px]">
      <span class="text-zinc-600 text-xs uppercase tracking-wider shrink-0 font-medium">Movers</span>
      <div id="movers-bar" class="flex items-center gap-4 overflow-x-auto"></div>
    </div>

    <!-- Sector Grid -->
    <div id="sector-grid" class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-3"></div>

    <!-- Drill-down Panel -->
    <div id="drill-panel" class="drill-panel hidden"></div>

  </div>

  <script>
  // ─── GICS Sector Mapping ──────────────────────────────────────────────────
  const GICS_SECTORS = {
    "Information Technology": [
      "Software—Application", "Software—Infrastructure", "Semiconductors",
      "Semiconductor Equipment & Materials", "IT Services", "Computer Hardware",
      "Electronic Components", "Electronics & Computer Distribution",
      "Information Technology Services", "Scientific & Technical Instruments",
      "Technology Distributors", "Consumer Electronics"
    ],
    "Health Care": [
      "Biotechnology", "Drug Manufacturers—General",
      "Drug Manufacturers—Specialty & Generic", "Health Care Plans",
      "Medical Devices", "Medical Instruments & Supplies",
      "Diagnostics & Research", "Medical Care Facilities",
      "Pharmaceutical Retailers", "Health Information Services"
    ],
    "Financials": [
      "Banks—Diversified", "Banks—Regional", "Insurance—Diversified",
      "Insurance—Life", "Insurance—Property & Casualty",
      "Asset Management", "Capital Markets",
      "Financial Data & Stock Exchanges", "Credit Services",
      "Mortgage Finance", "Insurance Brokers", "Financial Conglomerates"
    ],
    "Consumer Discretionary": [
      "Auto Manufacturers", "Auto Parts", "Auto & Truck Dealerships",
      "Restaurants", "Apparel Retail", "Apparel Manufacturing",
      "Footwear & Accessories", "Luxury Goods", "Personal Services",
      "Specialty Retail", "Home Improvement Retail", "Department Stores",
      "Discount Stores", "Internet Retail", "Resorts & Casinos",
      "Travel Services", "Lodging", "Leisure", "Publishing", "Gambling"
    ],
    "Communication Services": [
      "Telecom Services", "Internet Content & Information",
      "Electronic Gaming & Multimedia", "Entertainment—Diversified",
      "Broadcasting", "Advertising Agencies"
    ],
    "Industrials": [
      "Aerospace & Defense", "Airlines", "Air Freight & Logistics",
      "Railroads", "Trucking", "Marine Shipping",
      "Farm & Heavy Construction Machinery", "Industrial Distribution",
      "Specialty Industrial Machinery", "Tools & Accessories",
      "Electrical Equipment & Parts", "Engineering & Construction",
      "Infrastructure Operations", "Waste Management",
      "Staffing & Employment Services", "Consulting Services",
      "Security & Protection Services", "Building Products & Equipment",
      "Rental & Leasing Services"
    ],
    "Energy": [
      "Oil & Gas E&P", "Oil & Gas Integrated", "Oil & Gas Midstream",
      "Oil & Gas Refining & Marketing", "Oil & Gas Drilling",
      "Oil & Gas Equipment & Services", "Thermal Coal", "Uranium"
    ],
    "Materials": [
      "Specialty Chemicals", "Agricultural Inputs", "Chemicals",
      "Coking Coal", "Gold", "Silver", "Copper",
      "Other Precious Metals & Mining", "Aluminum", "Steel",
      "Other Industrial Metals & Mining", "Paper & Paper Products",
      "Lumber & Wood Production", "Building Materials",
      "Packaging & Containers"
    ],
    "Utilities": [
      "Utilities—Regulated Electric", "Utilities—Regulated Gas",
      "Utilities—Regulated Water", "Utilities—Diversified",
      "Utilities—Independent Power Producers", "Utilities—Renewable"
    ],
    "Real Estate": [
      "REIT—Diversified", "REIT—Industrial", "REIT—Office",
      "REIT—Retail", "REIT—Residential", "REIT—Healthcare Facilities",
      "REIT—Hotel & Motel", "REIT—Specialty", "REIT—Mortgage",
      "Real Estate Services", "Real Estate—Diversified"
    ],
    "Consumer Staples": [
      "Beverages—Brewers", "Beverages—Non-Alcoholic",
      "Beverages—Wineries & Distilleries", "Confectioners",
      "Farm Products", "Food Distribution", "Grocery Stores",
      "Household & Personal Products", "Packaged Foods", "Tobacco"
    ]
  };

  // Build reverse lookup: industry name → GICS sector name
  const INDUSTRY_TO_GICS = {};
  for (const [sector, industries] of Object.entries(GICS_SECTORS)) {
    for (const ind of industries) {
      INDUSTRY_TO_GICS[ind] = sector;
    }
  }

  // ─── State ────────────────────────────────────────────────────────────────
  const state = {
    period: '1D',
    selectedSector: null,
    rawItems: [],
    sectorData: [],
  };

  // ─── Data ─────────────────────────────────────────────────────────────────
  async function loadData() {
    showLoading();
    try {
      const res = await fetch(`/api/gics-overview?period=${state.period}`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      state.rawItems = json.items || [];

      // Debug: log unmatched DB industry names
      const unmatched = state.rawItems.filter(i => !INDUSTRY_TO_GICS[i.industry]);
      if (unmatched.length) {
        console.warn('[GICS] Unmatched industries (add to GICS_SECTORS map):', unmatched.map(i => i.industry));
      }

      state.sectorData = buildSectorData(state.rawItems);
      render();

      document.getElementById('last-updated').textContent =
        'Updated ' + new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    } catch (e) {
      showError();
    }
  }

  function buildSectorData(items) {
    const sectorMap = {};
    for (const sectorName of Object.keys(GICS_SECTORS)) {
      sectorMap[sectorName] = { name: sectorName, changes: [], pes: [], stockCount: 0, subIndustries: [] };
    }
    for (const item of items) {
      const sectorName = INDUSTRY_TO_GICS[item.industry];
      if (!sectorName || !sectorMap[sectorName]) continue;
      const s = sectorMap[sectorName];
      if (item.pct_change !== null) s.changes.push(item.pct_change);
      if (item.avg_pe !== null && item.avg_pe > 0 && item.avg_pe < 200) s.pes.push(item.avg_pe);
      s.stockCount += item.stock_count;
      s.subIndustries.push(item);
    }
    return Object.values(sectorMap).map(s => ({
      name: s.name,
      pct_change: s.changes.length ? s.changes.reduce((a, b) => a + b, 0) / s.changes.length : null,
      avg_pe: s.pes.length ? s.pes.reduce((a, b) => a + b, 0) / s.pes.length : null,
      stock_count: s.stockCount,
      sub_industries: [...s.subIndustries].sort((a, b) => (b.pct_change ?? -999) - (a.pct_change ?? -999)),
    }));
  }

  // ─── Render ───────────────────────────────────────────────────────────────
  function render() {
    renderMovers();
    renderGrid();
    if (state.selectedSector) renderDrillDown(state.selectedSector);
    else document.getElementById('drill-panel').classList.add('hidden');
  }

  function renderMovers() {
    const valid = state.sectorData.filter(s => s.pct_change !== null);
    const sorted = [...valid].sort((a, b) => b.pct_change - a.pct_change);
    const gainers = sorted.slice(0, 3);
    const losers = [...sorted].reverse().slice(0, 3);
    const bar = document.getElementById('movers-bar');
    if (!gainers.length && !losers.length) {
      bar.innerHTML = '<span class="text-zinc-600 text-xs">No data</span>';
      return;
    }
    const fmt = (s) => {
      const sign = s.pct_change >= 0 ? '+' : '';
      return `${sign}${s.pct_change.toFixed(2)}%`;
    };
    bar.innerHTML = [
      ...gainers.map(s => `<span class="text-green-400 text-xs whitespace-nowrap font-medium">▲ ${s.name} ${fmt(s)}</span>`),
      '<span class="text-zinc-700 text-xs shrink-0">|</span>',
      ...losers.map(s => `<span class="text-red-400 text-xs whitespace-nowrap font-medium">▼ ${s.name} ${fmt(s)}</span>`),
    ].join('<span class="text-zinc-700 mx-1 text-xs">·</span>');
  }

  function renderGrid() {
    const grid = document.getElementById('sector-grid');
    grid.innerHTML = state.sectorData.map(sector => {
      const change = sector.pct_change;
      const isPos = change !== null && change >= 0;
      const changeColor = change === null ? 'text-zinc-500' : (isPos ? 'text-green-400' : 'text-red-400');
      const sign = change !== null && isPos ? '+' : '';
      const changeText = change === null ? '—' : `${sign}${change.toFixed(2)}%`;
      const isSelected = state.selectedSector === sector.name;
      const borderClass = isSelected
        ? 'border-zinc-500 bg-zinc-800'
        : 'border-zinc-800 bg-[#18181b] hover:border-zinc-700 hover:bg-zinc-900';

      return `
        <div class="rounded-lg p-4 border cursor-pointer transition-colors ${borderClass}"
             onclick="selectSector('${sector.name.replace(/'/g, "\\'")}')">
          <div class="text-zinc-400 text-xs mb-2 leading-snug">${sector.name}</div>
          <div class="${changeColor} text-xl font-semibold leading-none mb-2">${changeText}</div>
          <div class="flex items-center gap-2">
            <span class="text-zinc-600 text-xs">${sector.avg_pe !== null ? 'PE ' + sector.avg_pe.toFixed(1) + 'x' : '—'}</span>
            <span class="text-zinc-800 text-xs">·</span>
            <span class="text-zinc-600 text-xs">${sector.stock_count} stocks</span>
          </div>
        </div>`;
    }).join('');
  }

  function selectSector(sectorName) {
    if (state.selectedSector === sectorName) {
      state.selectedSector = null;
      document.getElementById('drill-panel').classList.add('hidden');
      renderGrid();
    } else {
      state.selectedSector = sectorName;
      renderGrid();
      renderDrillDown(sectorName);
    }
  }

  function renderDrillDown(sectorName) {
    const sector = state.sectorData.find(s => s.name === sectorName);
    if (!sector) return;
    const panel = document.getElementById('drill-panel');
    panel.classList.remove('hidden');

    const rows = sector.sub_industries.map(ind => {
      const change = ind.pct_change;
      const isPos = change !== null && change >= 0;
      const cc = change === null ? 'text-zinc-500' : (isPos ? 'text-green-400' : 'text-red-400');
      const sign = change !== null && isPos ? '+' : '';
      const ct = change === null ? '—' : `${sign}${change.toFixed(2)}%`;
      return `
        <tr class="border-b border-zinc-800 last:border-0">
          <td class="py-2.5 pr-4 text-zinc-200 text-sm">${ind.industry}</td>
          <td class="py-2.5 pr-4 text-zinc-500 text-sm text-right">${ind.stock_count}</td>
          <td class="py-2.5 pr-4 text-zinc-500 text-sm text-right">${ind.avg_pe !== null ? ind.avg_pe.toFixed(1) + 'x' : '—'}</td>
          <td class="py-2.5 text-right text-sm font-medium ${cc}">${ct}</td>
        </tr>`;
    }).join('');

    const emptyMsg = rows.length === 0
      ? '<tr><td colspan="4" class="py-6 text-center text-zinc-600 text-sm">No sub-industry data for this period</td></tr>'
      : rows;

    panel.innerHTML = `
      <div class="bg-[#18181b] border border-zinc-800 rounded-lg p-5">
        <div class="flex items-center justify-between mb-4">
          <h3 class="text-sm font-medium">
            ${sectorName}
            <span class="text-zinc-600 font-normal ml-1">→ Sub-industries</span>
          </h3>
          <button onclick="selectSector('${sectorName.replace(/'/g, "\\'")}')"
                  class="text-zinc-600 hover:text-zinc-300 text-xs transition-colors">✕ Close</button>
        </div>
        <div class="overflow-x-auto">
          <table class="w-full min-w-[400px]">
            <thead>
              <tr class="border-b border-zinc-800">
                <th class="pb-2 text-left text-zinc-600 text-xs font-medium">Sub-industry</th>
                <th class="pb-2 text-right text-zinc-600 text-xs font-medium">Stocks</th>
                <th class="pb-2 text-right text-zinc-600 text-xs font-medium pr-4">Avg PE</th>
                <th class="pb-2 text-right text-zinc-600 text-xs font-medium">Change</th>
              </tr>
            </thead>
            <tbody>${emptyMsg}</tbody>
          </table>
        </div>
      </div>`;
  }

  // ─── Loading & Error States ───────────────────────────────────────────────
  function showLoading() {
    document.getElementById('sector-grid').innerHTML =
      Array(11).fill('').map(() => `
        <div class="bg-[#18181b] border border-zinc-800 rounded-lg p-4 animate-pulse">
          <div class="h-2.5 bg-zinc-800 rounded w-3/4 mb-3"></div>
          <div class="h-5 bg-zinc-800 rounded w-1/3 mb-3"></div>
          <div class="h-2 bg-zinc-800 rounded w-1/2"></div>
        </div>`).join('');
    document.getElementById('movers-bar').innerHTML =
      '<span class="text-zinc-700 text-xs animate-pulse">Loading market data…</span>';
    document.getElementById('drill-panel').classList.add('hidden');
    state.selectedSector = null;
  }

  function showError() {
    document.getElementById('sector-grid').innerHTML = `
      <div class="col-span-4 py-16 text-center">
        <p class="text-zinc-500 text-sm mb-2">Failed to load market data.</p>
        <button onclick="loadData()"
                class="text-zinc-400 hover:text-zinc-200 text-sm underline underline-offset-2 transition-colors">
          Retry
        </button>
      </div>`;
    document.getElementById('movers-bar').innerHTML =
      '<span class="text-red-400 text-xs">Error loading data</span>';
  }

  // ─── Period Selector ──────────────────────────────────────────────────────
  function renderPeriodSelector() {
    const periods = ['1D', '1W', '1M', '3M', '6M', '1Y', '5Y'];
    document.getElementById('period-selector').innerHTML = periods.map(p => `
      <button id="btn-${p}" onclick="setPeriod('${p}')"
              class="px-3 py-1.5 text-xs rounded border transition-colors font-medium
                     ${p === state.period
                       ? 'bg-zinc-800 border-zinc-600 text-zinc-100'
                       : 'bg-transparent border-zinc-800 text-zinc-500 hover:border-zinc-700 hover:text-zinc-300'}">
        ${p}
      </button>`).join('');
  }

  function setPeriod(period) {
    state.period = period;
    state.selectedSector = null;
    renderPeriodSelector();
    loadData();
  }

  // ─── Boot ─────────────────────────────────────────────────────────────────
  renderPeriodSelector();
  loadData();
  </script>
</body>
</html>
```

- [ ] **Step 2: Verify the page loads via curl**

```bash
curl -s -o /dev/null -w "%{http_code}" http://localhost:8087/
```

Expected: `200`

- [ ] **Step 3: Manual browser verification**

Open `http://92.5.115.127:8087/` in a browser and confirm:
1. Page background is near-black (`#09090b`), 11 sector cards visible
2. Period buttons `1D 1W 1M 3M 6M 1Y 5Y` appear at top, `1D` highlighted
3. Top Movers bar shows gainers (green ▲) and losers (red ▼)
4. Clicking a sector card highlights it and shows drill-down table below the grid
5. Clicking the same card again (or ✕ Close) collapses the panel
6. Clicking `1W` refetches data and re-renders all widgets
7. Open browser console — check for `[GICS] Unmatched:` warnings; if any appear, add those industry names to the correct sector in the `GICS_SECTORS` map in `index.html` and repeat until no warnings

- [ ] **Step 4: Fix any unmatched industries found in Step 3**

If `[GICS] Unmatched:` warnings appear in the console:

```bash
# Query DB to see all industry names
psql -U stock_user -d stock_db -c "SELECT name FROM industries ORDER BY name;" -h localhost
```

For each unmatched name, add it to the correct sector array in `GICS_SECTORS` inside `index.html`, then reload the browser to confirm the warning disappears.

- [ ] **Step 5: Commit**

```bash
cd /home/ubuntu/stock-market-dashboard
git add index.html
git commit -m "feat: redesign home page with GICS 11-sector grid and period selector"
```

---

## Self-Review

**Spec coverage check:**
- ✅ Navbar + timestamp → in `index.html` (`#last-updated`)
- ✅ Date range `1D · 1W · 1M · 3M · 6M · 1Y · 5Y`, default `1D` → `renderPeriodSelector()` + `state.period = '1D'`
- ✅ Top movers bar (3 gainers + 3 losers), updates on period change → `renderMovers()` called in `render()`
- ✅ 11 GICS sector grid, % change + PE + stock count → `renderGrid()` using `buildSectorData()`
- ✅ Drill-down: click sector → sub-industry table (name, stocks, PE, change) → `renderDrillDown()`
- ✅ Click same sector again → collapses → `selectSector()` toggle logic
- ✅ Shadcn zinc dark palette → all colors use `#09090b`, `#18181b`, `#27272a`, etc.
- ✅ API endpoint `/api/gics-overview?period=1D` → Task 1
- ✅ Period fallback (1D uses last available trading day) → SQL `WHERE date = MAX(date)` handles weekends/holidays
- ✅ GICS mapping in frontend JS, no DB schema changes → `GICS_SECTORS` + `INDUSTRY_TO_GICS`
- ✅ Error state + retry → `showError()` with Retry button
- ✅ Loading skeleton → `showLoading()` with `animate-pulse`
- ✅ Integration tests for API → `tests/test_gics_overview.py`
