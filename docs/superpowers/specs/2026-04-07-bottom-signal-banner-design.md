# Bottom Signal Alert Banner — Design Spec

## Goal

Show a persistent top-of-page banner on `index.html` and `rotation.html` indicating the current RRG-based market bottom signal level, so the user is immediately aware of buying opportunities when opening the dashboard.

## Architecture

Three components:

1. **`signal_checker.py`** — standalone module that computes signal level from latest RRG snapshot + VIX. No side effects, easy to test.
2. **`api.py`** — new `GET /api/signal-status` endpoint that calls `signal_checker.py` and returns JSON.
3. **`index.html` + `rotation.html`** — banner HTML injected below the navbar; JS fetches `/api/signal-status` on page load and renders the appropriate state.

## Signal Logic (`signal_checker.py`)

Reads the latest week's sector RRG snapshot (same data source as `generate_rotation_history.py`) and fetches the current VIX price via yfinance.

**Key sectors for momentum check:** Real Estate, Materials, Industrials, Financials

**Three levels:**

| Level | Condition | Historical win rate |
|-------|-----------|-------------------|
| `"strong"` | Lagging ≤ 2 AND Improving ≥ 2 AND VIX > 20 | 7/7 (100%) |
| `"general"` | Lagging ≤ 3 AND Improving ≥ 2 AND key sector momentum signals ≥ 2 (but not strong) | ~85% |
| `"none"` | All other states | — |

**Key sector momentum signal:** a key sector counts as a signal if its `rs_momentum > 100`.

**VIX fetch:** `yfinance.download('^VIX', period='5d')` — take the most recent close. Cache result in memory for the duration of the process (no file cache needed; the API is called infrequently).

## API Response (`/api/signal-status`)

```json
{
  "level": "strong",
  "lagging": 2,
  "improving": 3,
  "vix": 25.4,
  "key_momentum_signals": 2,
  "message": "底部強信號：Lagging=2, VIX=25.4",
  "updated_at": "2026-04-07"
}
```

`updated_at` is the date of the latest RRG snapshot used.

If VIX cannot be fetched, the endpoint still returns a result but uses `vix: null` and evaluates signal without the VIX condition (falls back to `"general"` or `"none"` only — never `"strong"` without confirmed VIX > 20).

## Banner UI

Placed immediately below the `<nav>` element in both pages. Fetched and rendered via a `<script>` block at page load. No flash/layout shift — banner starts hidden and fades in after fetch.

### Visual states

**Strong signal** (`level: "strong"`) — amber:
```
background: #92400e (dark amber)
border-bottom: 1px solid #b45309
text color: #fde68a
content: ⚡ 底部強信號 — Lagging {n}｜VIX {v}｜歷史勝率 100%（7/7）
```

**General signal** (`level: "general"`) — blue:
```
background: #1e3a5f (dark blue)
border-bottom: 1px solid #2563eb
text color: #93c5fd
content: 📊 底部信號 — Lagging {n}｜VIX {v}｜歷史勝率 85%
```

**No signal** (`level: "none"`) — subtle dark:
```
background: #0f172a
border-bottom: 1px solid #1e293b
text color: #475569
content: 目前無底部信號
```

All three states are always rendered (not hidden) so the user always knows the current state. Banner height: `py-2 px-4`, single line of text centered. Font size: `0.85rem`.

## File Changes

- **Create:** `signal_checker.py`
- **Modify:** `api.py` — add `/api/signal-status` route
- **Modify:** `index.html` — add banner HTML + fetch script
- **Modify:** `rotation.html` — same banner HTML + fetch script

## Testing

- Unit test `signal_checker.py` with mocked RRG snapshots covering all three signal levels
- Unit test VIX fallback (VIX fetch fails → no strong signal)
- Integration test `GET /api/signal-status` returns valid JSON with required fields
- Manual browser check: banner renders correctly on both pages in all three states
