# Bottom Signal Alert Banner — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Show a three-state alert banner below the navbar on `index.html` and `rotation.html` indicating whether a market bottom signal is currently active, so the user immediately sees buying-opportunity alerts when opening the dashboard.

**Architecture:** A new `signal_checker.py` module computes the signal level from the latest RRG snapshot + live VIX. A new `GET /api/signal-status` endpoint in `api.py` calls it and returns JSON. Both frontend pages fetch this endpoint on load and render a colour-coded banner (amber = strong, blue = general, dark = none).

**Tech Stack:** Python/FastAPI (backend), vanilla JS + inline CSS (frontend), yfinance (VIX fetch), pytest + FastAPI TestClient (tests).

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `signal_checker.py` | **Create** | Reads latest RRG snapshot, fetches VIX, returns signal level dict |
| `api.py` | **Modify** | Add `GET /api/signal-status` route |
| `tests/test_signal_checker.py` | **Create** | Unit tests for signal_checker + API endpoint |
| `index.html` | **Modify** | Add banner HTML + fetch script below `<nav>` |
| `rotation.html` | **Modify** | Same banner HTML + fetch script below `<nav>` |

---

### Task 1: `signal_checker.py` — signal computation module

**Files:**
- Create: `signal_checker.py`
- Test: `tests/test_signal_checker.py`

**Context:**  
The signal logic is based on data from `generate_rotation_history.py`'s `compute_snapshots()` which returns a list of dicts like:
```python
{
  "date": "2026-04-07",
  "week_index": 301,
  "sectors": [
    {"sector": "Energy", "rs_ratio": 105.0, "rs_momentum": 102.0, "quadrant": "Leading", ...},
    ...
  ]
}
```
The latest snapshot is `snapshots[-1]`. Quadrant values are: `"Leading"`, `"Weakening"`, `"Lagging"`, `"Improving"`.

VIX is fetched via yfinance: `yf.download('^VIX', period='5d', progress=False)['Close'].iloc[-1]`.

**Signal levels:**
- `"strong"`: Lagging ≤ 2 AND Improving ≥ 2 AND VIX > 20
- `"general"`: Lagging ≤ 3 AND Improving ≥ 2 AND key_momentum_signals ≥ 2 (but NOT strong)
- `"none"`: everything else

Key sectors for momentum: `"Real Estate"`, `"Materials"`, `"Industrials"`, `"Financials"`. A sector counts as a momentum signal if its `rs_momentum > 100`.

If VIX fetch fails, log a warning and set `vix=None` — in this case the level can never be `"strong"` (requires confirmed VIX > 20).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_signal_checker.py`:

```python
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pytest
from unittest.mock import patch
from signal_checker import compute_signal


def _snap(sectors):
    """Build a minimal snapshot dict."""
    return {"date": "2026-04-07", "week_index": 0, "sectors": sectors}


def _sector(name, quadrant, rs_momentum=105.0):
    return {"sector": name, "rs_ratio": 100.0, "rs_momentum": rs_momentum,
            "quadrant": quadrant, "return_13w": 0.0, "return_4w": 0.0}


ALL_11 = [
    _sector("Communication Services", "Leading"),
    _sector("Consumer Discretionary", "Leading"),
    _sector("Consumer Staples", "Leading"),
    _sector("Energy", "Leading"),
    _sector("Financials", "Improving"),
    _sector("Health Care", "Leading"),
    _sector("Industrials", "Improving"),
    _sector("Information Technology", "Leading"),
    _sector("Materials", "Improving"),
    _sector("Real Estate", "Lagging", rs_momentum=98.0),
    _sector("Utilities", "Lagging"),
]


def test_strong_signal_when_vix_high_and_lagging_low():
    # Lagging=2 (Real Estate + Utilities), Improving=3 (Fin/Ind/Mat), VIX=25
    snapshot = _snap(ALL_11)
    with patch("signal_checker._fetch_vix", return_value=25.0):
        result = compute_signal([snapshot])
    assert result["level"] == "strong"
    assert result["lagging"] == 2
    assert result["improving"] == 3
    assert result["vix"] == 25.0
    assert result["key_momentum_signals"] == 2  # Industrials + Materials (Real Estate mom<100)
    assert "updated_at" in result
    assert "message" in result


def test_general_signal_when_vix_low():
    # Same sectors but VIX=15 → can't be strong, check general
    snapshot = _snap(ALL_11)
    with patch("signal_checker._fetch_vix", return_value=15.0):
        result = compute_signal([snapshot])
    assert result["level"] == "general"


def test_no_signal_when_lagging_high():
    sectors = [
        _sector("Communication Services", "Lagging"),
        _sector("Consumer Discretionary", "Lagging"),
        _sector("Consumer Staples", "Lagging"),
        _sector("Energy", "Lagging"),
        _sector("Financials", "Improving"),
        _sector("Health Care", "Leading"),
        _sector("Industrials", "Improving"),
        _sector("Information Technology", "Leading"),
        _sector("Materials", "Improving"),
        _sector("Real Estate", "Lagging"),
        _sector("Utilities", "Lagging"),
    ]
    with patch("signal_checker._fetch_vix", return_value=30.0):
        result = compute_signal([_snap(sectors)])
    assert result["level"] == "none"


def test_strong_requires_vix_above_20():
    # Lagging=2 but VIX=19.9 → not strong
    snapshot = _snap(ALL_11)
    with patch("signal_checker._fetch_vix", return_value=19.9):
        result = compute_signal([snapshot])
    assert result["level"] != "strong"


def test_vix_fetch_failure_falls_back_to_general_or_none():
    # VIX fetch throws → vix=None, never strong
    snapshot = _snap(ALL_11)
    with patch("signal_checker._fetch_vix", side_effect=Exception("network error")):
        result = compute_signal([snapshot])
    assert result["level"] != "strong"
    assert result["vix"] is None


def test_empty_snapshots_returns_none():
    result = compute_signal([])
    assert result["level"] == "none"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /home/ubuntu/stock-market-dashboard
venv/bin/python -m pytest tests/test_signal_checker.py -v 2>&1 | head -20
```

Expected: `ModuleNotFoundError: No module named 'signal_checker'`

- [ ] **Step 3: Implement `signal_checker.py`**

Create `/home/ubuntu/stock-market-dashboard/signal_checker.py`:

```python
"""
Computes RRG-based market bottom signal level from the latest weekly snapshot + VIX.

Levels:
  "strong"  — Lagging<=2, Improving>=2, VIX>20       (historical 7/7 success)
  "general" — Lagging<=3, Improving>=2, key_mom>=2   (historical ~85% success)
  "none"    — all other states
"""
import datetime
import yfinance as yf

_KEY_SECTORS = {"Real Estate", "Materials", "Industrials", "Financials"}


def _fetch_vix() -> float:
    """Fetch latest VIX close. Raises on failure."""
    data = yf.download("^VIX", period="5d", progress=False)
    return float(data["Close"].iloc[-1])


def compute_signal(snapshots: list[dict]) -> dict:
    """
    Given a list of snapshot dicts (from compute_snapshots()), return a signal dict.

    Args:
        snapshots: list of dicts with keys 'date', 'week_index', 'sectors'.
                   Each sector dict has 'sector', 'quadrant', 'rs_momentum'.

    Returns:
        {
          "level": "strong" | "general" | "none",
          "lagging": int,
          "improving": int,
          "vix": float | None,
          "key_momentum_signals": int,
          "message": str,
          "updated_at": str,  # ISO date of latest snapshot
        }
    """
    if not snapshots:
        return _result("none", 0, 0, None, 0, "目前無底部信號", "N/A")

    latest = snapshots[-1]
    sectors = latest.get("sectors", [])
    updated_at = latest.get("date", "N/A")

    lagging = sum(1 for s in sectors if s["quadrant"] == "Lagging")
    improving = sum(1 for s in sectors if s["quadrant"] == "Improving")
    key_mom = sum(
        1 for s in sectors
        if s["sector"] in _KEY_SECTORS and s["rs_momentum"] > 100
    )

    vix = None
    try:
        vix = _fetch_vix()
    except Exception as e:
        print(f"[signal_checker] WARNING: VIX fetch failed: {e}")

    # Determine level
    is_strong = (
        lagging <= 2
        and improving >= 2
        and vix is not None
        and vix > 20
    )
    is_general = (
        not is_strong
        and lagging <= 3
        and improving >= 2
        and key_mom >= 2
    )

    if is_strong:
        vix_str = f"{vix:.1f}"
        msg = f"底部強信號：Lagging={lagging}｜VIX={vix_str}｜歷史勝率 100%（7/7）"
        level = "strong"
    elif is_general:
        vix_str = f"{vix:.1f}" if vix is not None else "N/A"
        msg = f"底部信號：Lagging={lagging}｜VIX={vix_str}｜歷史勝率 85%"
        level = "general"
    else:
        msg = "目前無底部信號"
        level = "none"

    return _result(level, lagging, improving, vix, key_mom, msg, updated_at)


def _result(level, lagging, improving, vix, key_mom, message, updated_at):
    return {
        "level": level,
        "lagging": lagging,
        "improving": improving,
        "vix": round(vix, 2) if vix is not None else None,
        "key_momentum_signals": key_mom,
        "message": message,
        "updated_at": updated_at,
    }
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /home/ubuntu/stock-market-dashboard
venv/bin/python -m pytest tests/test_signal_checker.py -v
```

Expected: `6 passed`

- [ ] **Step 5: Commit**

```bash
git add signal_checker.py tests/test_signal_checker.py
git commit -m "feat: add signal_checker module for RRG bottom signal detection"
```

---

### Task 2: `GET /api/signal-status` endpoint

**Files:**
- Modify: `api.py` (add import + new endpoint after line 1168, before `SECTOR_ETFS`)
- Test: `tests/test_signal_checker.py` (append API tests)

**Context:**  
`api.py` uses FastAPI. Pattern for new endpoints: `@app.get("/api/...")`. The `compute_snapshots()` function is in `generate_rotation_history.py` and is already imported indirectly — add a direct import. The endpoint calls `compute_snapshots()` to get snapshots, passes them to `compute_signal()`, returns the dict directly (FastAPI auto-serialises).

- [ ] **Step 1: Write the failing API tests**

Append to `tests/test_signal_checker.py`:

```python
from fastapi.testclient import TestClient
from api import app

api_client = TestClient(app)


def test_api_signal_status_returns_200():
    with patch("signal_checker._fetch_vix", return_value=15.0):
        res = api_client.get("/api/signal-status")
    assert res.status_code == 200
    data = res.json()
    assert "level" in data
    assert data["level"] in ("strong", "general", "none")
    assert "lagging" in data
    assert "improving" in data
    assert "vix" in data
    assert "message" in data
    assert "updated_at" in data


def test_api_signal_status_level_is_valid_string():
    with patch("signal_checker._fetch_vix", return_value=22.0):
        res = api_client.get("/api/signal-status")
    assert res.status_code == 200
    assert res.json()["level"] in ("strong", "general", "none")
```

- [ ] **Step 2: Run to verify they fail**

```bash
cd /home/ubuntu/stock-market-dashboard
venv/bin/python -m pytest tests/test_signal_checker.py::test_api_signal_status_returns_200 -v
```

Expected: `FAILED` — `404 Not Found`

- [ ] **Step 3: Add import and endpoint to `api.py`**

At the top of `api.py`, after the existing imports (around line 11), add:

```python
from signal_checker import compute_signal
from generate_rotation_history import compute_snapshots as _compute_rrg_snapshots
```

After line 1168 (end of `get_sector_rotation_history`), before the `SECTOR_ETFS = {` line, add:

```python
@app.get("/api/signal-status")
def get_signal_status():
    """
    Returns the current RRG-based market bottom signal level.
    Levels: 'strong' (VIX>20 + Lagging<=2), 'general' (broader condition), 'none'.
    """
    try:
        snapshots = _compute_rrg_snapshots()
        return compute_signal(snapshots)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Signal computation failed: {e}")
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /home/ubuntu/stock-market-dashboard
venv/bin/python -m pytest tests/test_signal_checker.py -v
```

Expected: `8 passed`

- [ ] **Step 5: Smoke-test the live endpoint**

```bash
sudo systemctl restart stock-api && sleep 3
curl -s http://localhost:8000/api/signal-status | python3 -m json.tool
```

Expected: JSON with `"level"`, `"lagging"`, `"improving"`, `"vix"`, `"message"`, `"updated_at"`.

- [ ] **Step 6: Commit**

```bash
git add api.py tests/test_signal_checker.py
git commit -m "feat: add /api/signal-status endpoint"
```

---

### Task 3: Banner on `index.html`

**Files:**
- Modify: `index.html` (insert banner div + script after `</nav>`)

**Context:**  
`index.html` line 17 is the `<nav>` element; line 28 is `</nav>`. The banner sits as a sibling div immediately after `</nav>`, before the `<div class="max-w-7xl mx-auto px-4 py-5 space-y-4">` content wrapper. The page uses Tailwind CSS + inline styles (dark theme, `bg-[#09090b]`).

Banner starts hidden (`display:none`) and fades in after the fetch resolves — prevents layout flash.

- [ ] **Step 1: Insert banner HTML + script into `index.html`**

After line 28 (`</nav>`), insert:

```html
  <!-- Bottom Signal Banner -->
  <div id="signal-banner" style="display:none;padding:8px 16px;text-align:center;font-size:0.85rem;border-bottom:1px solid transparent;transition:opacity 0.3s ease;opacity:0;">
    <span id="signal-banner-text"></span>
  </div>
  <script>
    (function() {
      var STYLES = {
        strong:  { bg: '#92400e', border: '#b45309', color: '#fde68a' },
        general: { bg: '#1e3a5f', border: '#2563eb', color: '#93c5fd' },
        none:    { bg: '#0f172a', border: '#1e293b', color: '#475569' },
      };
      fetch('/api/signal-status')
        .then(function(r) { return r.json(); })
        .then(function(d) {
          var banner = document.getElementById('signal-banner');
          var text   = document.getElementById('signal-banner-text');
          var s = STYLES[d.level] || STYLES.none;
          banner.style.background   = s.bg;
          banner.style.borderBottomColor = s.border;
          banner.style.color        = s.color;
          text.textContent          = d.message;
          banner.style.display      = 'block';
          setTimeout(function() { banner.style.opacity = '1'; }, 10);
        })
        .catch(function() {});
    })();
  </script>
```

- [ ] **Step 2: Verify in browser**

Open `http://localhost:8087/` and confirm:
- A coloured banner appears below the navbar
- Banner text matches current signal state (e.g. "目前無底部信號" in dark grey, or signal message in amber/blue)
- No layout flash on load

- [ ] **Step 3: Commit**

```bash
git add index.html
git commit -m "feat: add bottom signal banner to index.html"
```

---

### Task 4: Banner on `rotation.html`

**Files:**
- Modify: `rotation.html` (insert identical banner div + script after `</nav>`, line 139)

**Context:**  
`rotation.html` uses Bootstrap 5 (not Tailwind). The `<nav>` ends at line 139. The content wrapper starts at line 141 (`<div class="container-fluid py-4 px-4">`). Insert the banner between them. The banner HTML+script is identical to Task 3.

- [ ] **Step 1: Insert banner HTML + script into `rotation.html`**

After line 139 (`</nav>`), insert the identical block:

```html
  <!-- Bottom Signal Banner -->
  <div id="signal-banner" style="display:none;padding:8px 16px;text-align:center;font-size:0.85rem;border-bottom:1px solid transparent;transition:opacity 0.3s ease;opacity:0;">
    <span id="signal-banner-text"></span>
  </div>
  <script>
    (function() {
      var STYLES = {
        strong:  { bg: '#92400e', border: '#b45309', color: '#fde68a' },
        general: { bg: '#1e3a5f', border: '#2563eb', color: '#93c5fd' },
        none:    { bg: '#0f172a', border: '#1e293b', color: '#475569' },
      };
      fetch('/api/signal-status')
        .then(function(r) { return r.json(); })
        .then(function(d) {
          var banner = document.getElementById('signal-banner');
          var text   = document.getElementById('signal-banner-text');
          var s = STYLES[d.level] || STYLES.none;
          banner.style.background   = s.bg;
          banner.style.borderBottomColor = s.border;
          banner.style.color        = s.color;
          text.textContent          = d.message;
          banner.style.display      = 'block';
          setTimeout(function() { banner.style.opacity = '1'; }, 10);
        })
        .catch(function() {});
    })();
  </script>
```

- [ ] **Step 2: Verify in browser**

Open `http://localhost:8087/rotation.html` and confirm the banner renders identically.

- [ ] **Step 3: Commit**

```bash
git add rotation.html
git commit -m "feat: add bottom signal banner to rotation.html"
```
