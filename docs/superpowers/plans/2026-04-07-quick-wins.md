# Quick Wins Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Three small improvements: pre-warm industry cache in daily_update.py, add SPY/QQQ to the `/api/sector-etf-prices` response, and add SPY/QQQ checkbox overlay to the ETF chart in rotation.html.

**Architecture:** `daily_update.py` calls `generate_industry_rotation_history.py` for all 11 sectors after the daily stock update. `api.py` adds a `benchmarks` key to the existing `/api/sector-etf-prices` response. `rotation.html` reads that key and toggles SPY/QQQ lines via checkboxes.

**Tech Stack:** Python subprocess, yfinance, ApexCharts line chart, Bootstrap 5 dark.

---

## Codebase Context

**`daily_update.py`**: `DailyUpdater.run()` at line 46 fetches daily prices for all tickers. Add warm-up at the end of `run()`, after the summary print block (line 130).

**`api.py`**:
- `SECTOR_ETFS` dict at line 1171 — 11 sector ETFs
- `get_sector_etf_prices(period)` at line 1185 — downloads prices, caches daily, returns `{ period, sectors: { sector_name: { etf, dates, values } } }`
- Current cache key: `/tmp/sector_etf_prices_{period}_{today}.json`

**`rotation.html`**:
- `etfData` global — set in `loadEtfChart()`, shape `{ period, sectors: {...} }`
- `renderEtfChart()` at line 835 — builds series from `etfData.sectors`, calls `etfChart.updateOptions` or creates a new ApexCharts instance
- ETF card header at line 195-203 — has range buttons, need to add SPY/QQQ checkboxes here

**`generate_industry_rotation_history.py`**: CLI at bottom — `python generate_industry_rotation_history.py --sector "Energy" --force`

**`ALL_SECTORS`** from `generate_rotation_history.py`: `['Communication Services', 'Consumer Discretionary', 'Consumer Staples', 'Energy', 'Financials', 'Health Care', 'Industrials', 'Information Technology', 'Materials', 'Real Estate', 'Utilities']`

---

## File Map

| File | Action |
|------|--------|
| `daily_update.py` | Modify — add warm-up loop at end of `run()` |
| `api.py` | Modify — add `benchmarks` to `get_sector_etf_prices` response, invalidate old cache format |
| `rotation.html` | Modify — SPY/QQQ checkboxes in ETF card header, `renderEtfChart` handles benchmarks |
| `tests/test_quick_wins.py` | Create |

---

## Task 1: Industry warm-up in `daily_update.py`

**Files:**
- Modify: `daily_update.py:130-134`
- Test: `tests/test_quick_wins.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_quick_wins.py`:

```python
import subprocess
import sys
import unittest
from unittest.mock import patch, MagicMock, call
import importlib


def _load_updater():
    import daily_update
    importlib.reload(daily_update)
    return daily_update


def test_warmup_called_for_all_sectors():
    """After run(), generate_industry_rotation_history.py is called for all 11 sectors."""
    du = _load_updater()

    updater = du.DailyUpdater.__new__(du.DailyUpdater)
    updater.db = MagicMock()
    updater.fetcher = MagicMock()
    updater.db.engine.connect.return_value.__enter__ = MagicMock(return_value=MagicMock(
        execute=MagicMock(return_value=MagicMock(fetchall=MagicMock(return_value=[]))),
        scalar=MagicMock(return_value=None),
    ))
    updater.db.engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    with patch("subprocess.run") as mock_run:
        updater._warmup_industry_cache()
        assert mock_run.call_count == 11
        called_sectors = [c.args[0][3] for c in mock_run.call_args_list]
        assert "Energy" in called_sectors
        assert "Information Technology" in called_sectors
        assert "Utilities" in called_sectors
```

- [ ] **Step 2: Run test to verify it fails**

```bash
venv/bin/python -m pytest tests/test_quick_wins.py::test_warmup_called_for_all_sectors -v
```
Expected: FAIL with `AttributeError: type object 'DailyUpdater' has no attribute '_warmup_industry_cache'`

- [ ] **Step 3: Implement `_warmup_industry_cache` and call it from `run()`**

In `daily_update.py`, add this import at the top (after existing imports):

```python
import subprocess
import sys
from pathlib import Path
```

Add `_warmup_industry_cache` method to `DailyUpdater` class, just before `run()`:

```python
    def _warmup_industry_cache(self):
        """Pre-generate industry rotation history for all 11 sectors."""
        from generate_rotation_history import ALL_SECTORS
        script = Path(__file__).parent / "generate_industry_rotation_history.py"
        print("\n--- Warming up industry rotation cache ---")
        for sector in ALL_SECTORS:
            print(f"  Generating {sector}…")
            subprocess.run(
                [sys.executable, str(script), "--sector", sector, "--force"],
                check=False,
            )
        print("--- Warm-up complete ---")
```

At the end of `run()`, after the summary print block (after `print(f"Errors:    {error_count}")`), add:

```python
        self._warmup_industry_cache()
```

- [ ] **Step 4: Run test to verify it passes**

```bash
venv/bin/python -m pytest tests/test_quick_wins.py::test_warmup_called_for_all_sectors -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add daily_update.py tests/test_quick_wins.py
git commit -m "feat: pre-warm industry rotation cache in daily_update.py"
```

---

## Task 2: Add SPY/QQQ to `/api/sector-etf-prices`

**Files:**
- Modify: `api.py` (around line 1171)
- Test: `tests/test_quick_wins.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_quick_wins.py`:

```python
import datetime
import json
from pathlib import Path
from fastapi.testclient import TestClient
from api import app

client = TestClient(app)


def _clear_etf_cache():
    today = datetime.date.today().isoformat()
    for period in ["3M", "6M", "1Y", "3Y", "ALL"]:
        p = Path(f"/tmp/sector_etf_prices_{period}_{today}.json")
        p.unlink(missing_ok=True)


def _write_fake_etf_cache(period="1Y"):
    today = datetime.date.today().isoformat()
    data = {
        "period": period,
        "sectors": {
            "Energy": {"etf": "XLE", "dates": ["2024-01-02"], "values": [0.0]},
        },
        "benchmarks": {
            "SPY": {"dates": ["2024-01-02"], "values": [0.0]},
            "QQQ": {"dates": ["2024-01-02"], "values": [0.0]},
        },
    }
    Path(f"/tmp/sector_etf_prices_{period}_{today}.json").write_text(json.dumps(data))
    return data


def test_etf_endpoint_has_benchmarks_key():
    _clear_etf_cache()
    _write_fake_etf_cache("1Y")
    res = client.get("/api/sector-etf-prices?period=1Y")
    assert res.status_code == 200
    data = res.json()
    assert "benchmarks" in data
    assert "SPY" in data["benchmarks"]
    assert "QQQ" in data["benchmarks"]
    _clear_etf_cache()


def test_etf_benchmark_has_dates_and_values():
    _clear_etf_cache()
    _write_fake_etf_cache("1Y")
    res = client.get("/api/sector-etf-prices?period=1Y")
    data = res.json()
    spy = data["benchmarks"]["SPY"]
    assert "dates" in spy
    assert "values" in spy
    assert isinstance(spy["dates"], list)
    assert isinstance(spy["values"], list)
    _clear_etf_cache()
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
venv/bin/python -m pytest tests/test_quick_wins.py::test_etf_endpoint_has_benchmarks_key tests/test_quick_wins.py::test_etf_benchmark_has_dates_and_values -v
```
Expected: FAIL (no `benchmarks` key in response)

- [ ] **Step 3: Add `BENCHMARK_ETFS` and update `get_sector_etf_prices`**

In `api.py`, find `SECTOR_ETFS = {` (line ~1171). Add immediately after the closing `}`:

```python
BENCHMARK_ETFS = {
    "SPY": "SPY",
    "QQQ": "QQQ",
}
```

In `get_sector_etf_prices`, find:
```python
    tickers = list(SECTOR_ETFS.values())
```
Replace with:
```python
    tickers = list(SECTOR_ETFS.values()) + list(BENCHMARK_ETFS.values())
```

Find the result assembly block:
```python
    result = {"period": period, "sectors": {}}
    for sector, etf in SECTOR_ETFS.items():
        if etf not in closes.columns:
            continue
        series = closes[etf].dropna()
        if series.empty:
            continue
        base = series.iloc[0]
        pct = ((series - base) / base * 100).round(2)
        result["sectors"][sector] = {
            "etf": etf,
            "dates": [d.strftime("%Y-%m-%d") for d in pct.index],
            "values": pct.tolist(),
        }

    cache.write_text(_json.dumps(result))
    return result
```

Replace with:
```python
    result = {"period": period, "sectors": {}, "benchmarks": {}}
    for sector, etf in SECTOR_ETFS.items():
        if etf not in closes.columns:
            continue
        series = closes[etf].dropna()
        if series.empty:
            continue
        base = series.iloc[0]
        pct = ((series - base) / base * 100).round(2)
        result["sectors"][sector] = {
            "etf": etf,
            "dates": [d.strftime("%Y-%m-%d") for d in pct.index],
            "values": pct.tolist(),
        }

    for name, etf in BENCHMARK_ETFS.items():
        if etf not in closes.columns:
            continue
        series = closes[etf].dropna()
        if series.empty:
            continue
        base = series.iloc[0]
        pct = ((series - base) / base * 100).round(2)
        result["benchmarks"][name] = {
            "dates": [d.strftime("%Y-%m-%d") for d in pct.index],
            "values": pct.tolist(),
        }

    cache.write_text(_json.dumps(result))
    return result
```

Also delete today's cached ETF files so the new format takes effect:
```bash
today=$(date +%Y-%m-%d); rm -f /tmp/sector_etf_prices_*_${today}.json
```

- [ ] **Step 4: Restart API and run tests**

```bash
sudo systemctl restart stock-api && sleep 2
venv/bin/python -m pytest tests/test_quick_wins.py::test_etf_endpoint_has_benchmarks_key tests/test_quick_wins.py::test_etf_benchmark_has_dates_and_values -v
```
Expected: both PASS

- [ ] **Step 5: Commit**

```bash
git add api.py tests/test_quick_wins.py
git commit -m "feat: add SPY/QQQ benchmarks to /api/sector-etf-prices response"
```

---

## Task 3: SPY/QQQ checkbox overlay in ETF chart

**Files:**
- Modify: `rotation.html`

- [ ] **Step 1: Add CSS for benchmark checkboxes**

In `rotation.html`, find the CSS block. After `.benchmark-btn.active { background: #1d4ed8; color: #fff; }` and before `</style>`, add:

```css
/* Benchmark overlay checkboxes */
.bm-check { display: inline-flex; align-items: center; gap: 5px; font-size: 11px; color: #94a3b8; cursor: pointer; padding: 3px 8px; border: 1px solid #334155; border-radius: 6px; background: transparent; user-select: none; }
.bm-check input { accent-color: #94a3b8; cursor: pointer; }
.bm-check.spy input { accent-color: #f1f5f9; }
.bm-check.qqq input { accent-color: #fb923c; }
.bm-check.checked { border-color: #475569; color: #e2e8f0; }
```

- [ ] **Step 2: Add checkboxes to ETF card header**

Find in `rotation.html`:
```html
                <div class="d-flex gap-1" id="etfRangeBtns">
                    <button class="range-btn" data-etf-period="3M">3M</button>
                    <button class="range-btn active" data-etf-period="1Y">1Y</button>
                    <button class="range-btn" data-etf-period="3Y">3Y</button>
                    <button class="range-btn" data-etf-period="ALL">全部</button>
                </div>
```

Replace with:
```html
                <div class="d-flex align-items-center gap-2 flex-wrap">
                    <div class="d-flex gap-1" id="etfRangeBtns">
                        <button class="range-btn" data-etf-period="3M">3M</button>
                        <button class="range-btn active" data-etf-period="1Y">1Y</button>
                        <button class="range-btn" data-etf-period="3Y">3Y</button>
                        <button class="range-btn" data-etf-period="ALL">全部</button>
                    </div>
                    <label class="bm-check spy" id="spyCheck">
                        <input type="checkbox" id="spyCheckbox"> SPY
                    </label>
                    <label class="bm-check qqq" id="qqqCheck">
                        <input type="checkbox" id="qqqCheckbox"> QQQ
                    </label>
                </div>
```

- [ ] **Step 3: Update `renderEtfChart` to include benchmarks when checked**

Find `function renderEtfChart() {` in `rotation.html`. Replace the entire function with:

```js
    function renderEtfChart() {
        if (!etfData) return;
        const series = Object.entries(etfData.sectors)
            .filter(([sector]) => visibleSectors.has(sector))
            .map(([sector, d]) => ({
                name: sector,
                data: d.dates.map((dt, i) => ({ x: new Date(dt).getTime(), y: d.values[i] })),
            }));

        const colors = Object.entries(etfData.sectors)
            .filter(([sector]) => visibleSectors.has(sector))
            .map(([sector]) => SECTOR_COLORS[sector] || '#94a3b8');

        // Add benchmark overlays if checked
        const spyChecked = document.getElementById('spyCheckbox')?.checked;
        const qqqChecked = document.getElementById('qqqCheckbox')?.checked;
        const bmColors = [];

        if (spyChecked && etfData.benchmarks?.SPY) {
            const d = etfData.benchmarks.SPY;
            series.push({ name: 'SPY', data: d.dates.map((dt, i) => ({ x: new Date(dt).getTime(), y: d.values[i] })) });
            bmColors.push('#f1f5f9');
        }
        if (qqqChecked && etfData.benchmarks?.QQQ) {
            const d = etfData.benchmarks.QQQ;
            series.push({ name: 'QQQ', data: d.dates.map((dt, i) => ({ x: new Date(dt).getTime(), y: d.values[i] })) });
            bmColors.push('#fb923c');
        }

        const allColors = [...colors, ...bmColors];

        if (etfChart) {
            etfChart.updateOptions({ series, colors: allColors }, true, false);
            return;
        }

        etfChart = new ApexCharts(document.getElementById('etfChart'), {
            chart: {
                type: 'line', height: 280, background: '#1e293b',
                toolbar: { show: false }, animations: { enabled: false },
                zoom: { enabled: false },
            },
            series,
            colors: allColors,
            stroke: {
                width: series.map(s => (s.name === 'SPY' || s.name === 'QQQ') ? 2 : 2),
                curve: 'smooth',
                dashArray: series.map(s => (s.name === 'SPY' || s.name === 'QQQ') ? 4 : 0),
            },
            xaxis: { type: 'datetime', labels: { style: { colors: '#64748b' }, datetimeUTC: false } },
            yaxis: {
                labels: {
                    style: { colors: '#64748b' },
                    formatter: v => `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`,
                },
            },
            tooltip: {
                theme: 'dark',
                y: { formatter: v => `${v >= 0 ? '+' : ''}${v.toFixed(2)}%` },
                x: { format: 'yyyy-MM-dd' },
            },
            legend: { show: false },
            grid: { borderColor: '#334155' },
            annotations: { yaxis: [{ y: 0, borderColor: '#475569', strokeDashArray: 4 }] },
            theme: { mode: 'dark' },
        });
        etfChart.render();
        document.getElementById('etfChartCard').style.display = '';
    }
```

- [ ] **Step 4: Wire checkbox change events**

Find in `rotation.html` (near the bottom of the script, before `load();`):
```js
    document.getElementById('etfRangeBtns').addEventListener('click', e => {
```

Add before that line:

```js
    document.getElementById('spyCheckbox').addEventListener('change', function() {
        document.getElementById('spyCheck').classList.toggle('checked', this.checked);
        if (etfChart) { etfChart.destroy(); etfChart = null; }
        renderEtfChart();
    });
    document.getElementById('qqqCheckbox').addEventListener('change', function() {
        document.getElementById('qqqCheck').classList.toggle('checked', this.checked);
        if (etfChart) { etfChart.destroy(); etfChart = null; }
        renderEtfChart();
    });

```

- [ ] **Step 5: Verify in browser**

Open `http://localhost:8087/rotation.html`:
1. ETF card header shows `SPY ☐` and `QQQ ☐` next to range buttons
2. Check SPY → white dashed line appears on ETF chart
3. Check QQQ → orange dashed line appears
4. Uncheck → line disappears
5. No console errors

- [ ] **Step 6: Commit**

```bash
git add rotation.html
git commit -m "feat: SPY/QQQ checkbox overlay on ETF performance chart"
```

---

## Task 4: Final verification and push

- [ ] **Step 1: Run all tests**

```bash
venv/bin/python -m pytest tests/ -v -k "not generates_cache"
```
Expected: all tests pass

- [ ] **Step 2: Restart API and smoke-test**

```bash
sudo systemctl restart stock-api && sleep 2
curl -s "http://localhost:8000/api/sector-etf-prices?period=1Y" | python3 -c "import sys,json; d=json.load(sys.stdin); print('benchmarks:', list(d.get('benchmarks',{}).keys()))"
```
Expected: `benchmarks: ['SPY', 'QQQ']`

- [ ] **Step 3: Push**

```bash
git push
```
