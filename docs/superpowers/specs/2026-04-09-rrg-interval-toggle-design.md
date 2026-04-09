# RRG Interval Toggle Design

**Date:** 2026-04-09
**Scope:** Sector RRG only (`rotation.html`). Industry drill-down excluded for now.

## Summary

Add a "週 | 日" (Weekly | Daily) toggle to the RRG control bar, allowing users to switch between weekly snapshots (current, every 5 trading days) and daily snapshots (every trading day). Backend generates separate cache files for each interval.

---

## Backend

### `generate_rotation_history.py`

- Add `--interval weekly|daily` CLI argument (default: `weekly`)
- `weekly`: `step=5`, key name `week_index` (no change to existing logic)
- `daily`: `step=1`, key name `day_index` (same RS calculation, different sampling)
- Cache filenames:
  - Weekly: `/tmp/sector_rotation_history_{date}.json` (unchanged)
  - Daily: `/tmp/sector_rotation_history_daily_{date}.json` (new)
- Snapshot counts: ~302 weekly, ~1508 daily

### `/api/sector-rotation-history`

- Add query param `?interval=weekly|daily` (default: `weekly`)
- Weekly: reads existing cache, behaviour unchanged
- Daily: reads `sector_rotation_history_daily_{date}.json`; if missing, spawns `generate_rotation_history.py --interval daily --force` in background and returns 202
- No changes to existing weekly path

### `daily_update.py`

- After existing `_regenerate_sector_rrg()` (weekly), add `_regenerate_sector_rrg_daily()` that runs `generate_rotation_history.py --interval daily --force`
- Both run sequentially during the 23:00 UTC daily update

---

## Frontend (`rotation.html`)

### UI

- Add a "週 | 日" segment toggle in the ctrl-bar, styled with existing `.benchmark-toggle` / `.benchmark-btn` classes
- Position: after the range buttons, before the benchmark toggle
- Default: 週 (weekly)

### State

- New JS variable `currentInterval = 'weekly'`
- On toggle click: set `currentInterval`, re-fetch snapshots, reset `currentIdx` to latest

### Data fetch

- Weekly: `fetch('/api/sector-rotation-history')` (unchanged)
- Daily: `fetch('/api/sector-rotation-history?interval=daily')`
- Both paths use the existing `pollUntilReady` 202-polling logic

### Timeline label

- Weekly: `snap.date + ' (week N/302)'` (use `week_index`)
- Daily: `snap.date + ' (day N/1508)'` (use `day_index`)

### Range buttons (3M / 6M / 1Y / 全部)

- Weekly: slice by weeks count (existing `data-weeks` attribute, unchanged)
- Daily: translate to trading days — 3M→65, 6M→130, 1Y→260, 全部→0
- On interval switch, re-apply the currently active range button with the correct unit

### Smoothing

- No change. Both modes use the existing 3-point moving average in `smoothSnapshots()`.

---

## What Does NOT Change

- Industry drill-down (not in scope)
- Weekly cache generation timing and filename
- Signal status endpoint (`/api/signal-status` reads weekly cache only)
- Smoothing window size

---

## Testing

- Weekly mode: verify existing behaviour unchanged after refactor
- Daily mode: first load triggers 202 → polling → renders ~1508 snapshots
- Range buttons: 1Y in daily mode shows ~260 snapshots
- Timeline label: correct mode label shown
- daily_update: both weekly and daily caches regenerated on next 23:00 run
