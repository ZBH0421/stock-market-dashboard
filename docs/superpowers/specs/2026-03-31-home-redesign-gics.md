# Home Page Redesign — GICS 11-Sector Dashboard

## Overview

Redesign the stock dashboard home page (`index.html`) with a shadcn/ui-inspired zinc aesthetic. The page shows all 11 GICS sectors, their performance over a user-selected period, top movers, and a drill-down into sub-industries.

**Tech stack:** Pure HTML + Tailwind CSS (CDN), Vanilla JS, ApexCharts (already in project), existing FastAPI backend.

---

## 1. Visual Design

**Color palette (shadcn zinc dark):**
- Background: `#09090b`
- Card background: `#18181b`
- Card border: `#27272a`
- Muted text: `#71717a`
- Body text: `#a1a1aa`
- Primary text: `#fafafa`
- Gain green: `#22c55e`
- Loss red: `#ef4444`

**Typography:** Inter font (Google Fonts CDN), clean and minimal.

**Layout:**
```
┌─────────────────────────────────────────┐
│  Navbar: "Market Dashboard"  + timestamp │
├─────────────────────────────────────────┤
│  Date range: 1D · 1W · 1M · 3M · 6M · 1Y · 5Y │
├─────────────────────────────────────────┤
│  Top Movers bar (3 gainers | 3 losers)  │
├─────────────────────────────────────────┤
│  [IT]  [Health] [Finance] [Consumer D]  │
│  [Comm][Indust] [Energy]  [Materials]   │
│  [Utilities]   [Real Est] [Consumer S]  │
├─────────────────────────────────────────┤
│  Drill-down panel (hidden until click)  │
│  Sector: Info Technology                │
│  ┌──────────────┬──────┬───────┐        │
│  │ Sub-industry │  PE  │ Chg%  │        │
│  └──────────────┴──────┴───────┘        │
└─────────────────────────────────────────┘
```

---

## 2. GICS Sector Mapping

The existing DB has 145 granular industries stored in the `industries` table. These must be mapped to the 11 standard GICS sectors via a frontend JavaScript config object — no DB schema changes needed.

**Mapping (industry name → GICS sector):**
```js
const GICS_SECTORS = {
  "Information Technology": [
    "Software—Application", "Software—Infrastructure", "Semiconductors",
    "Semiconductor Equipment & Materials", "IT Services",
    "Computer Hardware", "Electronic Components", "Electronics & Computer Distribution",
    "Information Technology Services", "Scientific & Technical Instruments",
    "Technology Distributors", "Consumer Electronics"
  ],
  "Health Care": [
    "Biotechnology", "Drug Manufacturers—General", "Drug Manufacturers—Specialty & Generic",
    "Health Care Plans", "Medical Devices", "Medical Instruments & Supplies",
    "Diagnostics & Research", "Medical Care Facilities", "Pharmaceutical Retailers",
    "Health Information Services"
  ],
  "Financials": [
    "Banks—Diversified", "Banks—Regional", "Insurance—Diversified", "Insurance—Life",
    "Insurance—Property & Casualty", "Asset Management", "Capital Markets",
    "Financial Data & Stock Exchanges", "Credit Services", "Mortgage Finance",
    "Insurance Brokers", "Financial Conglomerates"
  ],
  "Consumer Discretionary": [
    "Auto Manufacturers", "Auto Parts", "Auto & Truck Dealerships",
    "Restaurants", "Apparel Retail", "Apparel Manufacturing",
    "Footwear & Accessories", "Luxury Goods", "Personal Services",
    "Specialty Retail", "Home Improvement Retail", "Department Stores",
    "Discount Stores", "Internet Retail", "Resorts & Casinos",
    "Travel Services", "Lodging", "Airlines", "Broadcasting",
    "Entertainment", "Leisure", "Publishing", "Gambling"
  ],
  "Communication Services": [
    "Telecom Services", "Internet Content & Information",
    "Electronic Gaming & Multimedia", "Entertainment—Diversified",
    "Broadcasting", "Advertising Agencies"
  ],
  "Industrials": [
    "Aerospace & Defense", "Airlines", "Air Freight & Logistics",
    "Railroads", "Trucking", "Marine Shipping", "Farm & Heavy Construction Machinery",
    "Industrial Distribution", "Specialty Industrial Machinery",
    "Tools & Accessories", "Electrical Equipment & Parts",
    "Engineering & Construction", "Infrastructure Operations",
    "Waste Management", "Staffing & Employment Services",
    "Consulting Services", "Security & Protection Services",
    "Building Products & Equipment", "Rental & Leasing Services"
  ],
  "Energy": [
    "Oil & Gas E&P", "Oil & Gas Integrated", "Oil & Gas Midstream",
    "Oil & Gas Refining & Marketing", "Oil & Gas Drilling",
    "Oil & Gas Equipment & Services", "Thermal Coal", "Uranium"
  ],
  "Materials": [
    "Specialty Chemicals", "Agricultural Inputs", "Chemicals",
    "Coking Coal", "Gold", "Silver", "Copper", "Other Precious Metals & Mining",
    "Aluminum", "Steel", "Other Industrial Metals & Mining",
    "Paper & Paper Products", "Lumber & Wood Production", "Building Materials",
    "Packaging & Containers"
  ],
  "Utilities": [
    "Utilities—Regulated Electric", "Utilities—Regulated Gas",
    "Utilities—Regulated Water", "Utilities—Diversified",
    "Utilities—Independent Power Producers", "Utilities—Renewable"
  ],
  "Real Estate": [
    "REIT—Diversified", "REIT—Industrial", "REIT—Office", "REIT—Retail",
    "REIT—Residential", "REIT—Healthcare Facilities", "REIT—Hotel & Motel",
    "REIT—Specialty", "REIT—Mortgage", "Real Estate Services",
    "Real Estate—Diversified"
  ],
  "Consumer Staples": [
    "Beverages—Brewers", "Beverages—Non-Alcoholic", "Beverages—Wineries & Distilleries",
    "Confectioners", "Farm Products", "Food Distribution",
    "Grocery Stores", "Household & Personal Products",
    "Packaged Foods", "Tobacco"
  ]
}
```

The frontend uses this to group sub-industries returned from the API.

---

## 3. New API Endpoint

**`GET /api/gics-overview?period=1D`**

Returns aggregate performance per industry (sub-industry) for the selected period. The frontend aggregates into GICS sectors using the JS config.

**Period → lookback mapping (server-side):**
| period | INTERVAL |
|--------|----------|
| 1D     | 1 day    |
| 1W     | 7 days   |
| 1M     | 30 days  |
| 3M     | 90 days  |
| 6M     | 180 days |
| 1Y     | 365 days |
| 5Y     | 1825 days|

**Response shape:**
```json
[
  {
    "industry": "Software—Application",
    "pct_change": 1.82,
    "avg_pe": 31.4,
    "stock_count": 47
  },
  ...
]
```

**SQL logic:**
- `pct_change`: compare avg close of last N days vs avg close of the period before that (equal-length window), weighted by market cap
- `avg_pe`: `SUM(close / trailing_eps * market_cap) / SUM(market_cap)` where `trailing_eps > 0` and PE between 0 and 200
- `stock_count`: distinct tickers in that industry during the period

**Fallback for 1D:** If today has no data yet (market closed or weekend), use last available trading day vs the one before.

---

## 4. Frontend Behavior

### Date Range Selector
- 7 buttons: `1D · 1W · 1M · 3M · 6M · 1Y · 5Y`
- Default active: `1D`
- On click: fetch new data, re-render all widgets
- Active button: white text, `#27272a` background, border highlight

### Top Movers Bar
- Shows top 3 gainers and top 3 losers by % change (sector-level, computed from sub-industry aggregation)
- Format: `▲ Health Care +2.4%` / `▼ Energy −1.2%`
- Scrolls horizontally on mobile

### Sector Grid
- 11 GICS sector cards in a 4-column grid (wraps responsively)
- Each card shows:
  - Sector name
  - `% change` (colored green/red)
  - `PE: XX.Xx` (muted, hidden if unavailable)
  - `N stocks` (muted)
- Clicking a card highlights it and opens the drill-down panel
- Clicking the same card again collapses the panel

### Drill-Down Panel
- Appears below the grid (full width) when a sector is selected
- Header: `Sector Name → Sub-industries`
- Table columns: `Sub-industry | Stocks | PE | Change`
- Sorted by `% change` descending
- "← Back" button / click outside card closes it
- Smooth expand/collapse animation (CSS max-height transition)

---

## 5. File Changes

| File | Action |
|------|--------|
| `index.html` | Replace entirely with new design |
| `api.py` | Add `/api/gics-overview` endpoint |
| `config.js` | No change needed (API_BASE_URL already relative) |

No new files. No build step. Tailwind loaded via CDN.

---

## 6. Error Handling

- If API returns empty array: show "No data available" state in grid
- If individual sector has no PE data: display "—" instead of a number
- Loading state: skeleton pulse animation on cards while fetching
- Network error: toast/banner message at top

---

## 7. Out of Scope

- Mobile-first redesign (responsive but not mobile-optimized)
- User authentication
- Watchlists or favorites
- Historical PE trajectory chart (already exists at `pe_history.html`)
- Real-time price streaming
