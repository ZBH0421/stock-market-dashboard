# RRG Interval Toggle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a "週 | 日" toggle to the RRG control bar so users can switch between weekly (every 5 trading days, ~302 snapshots) and daily (every trading day, ~1508 snapshots) intervals.

**Architecture:** Backend generates two separate cache files via a new `--interval` CLI flag in `generate_rotation_history.py`. The existing `/api/sector-rotation-history` endpoint gains an `?interval=` query param that routes to the correct cache. Frontend adds a segment toggle that re-fetches data and re-applies the active time range in the correct unit.

**Tech Stack:** Python/FastAPI (backend), vanilla JS (frontend), pytest (tests)

---

## File Map

| File | Change |
|------|--------|
| `generate_rotation_history.py` | Add `--interval weekly\|daily` arg; parameterise `step` and index key |
| `api.py` | Add `?interval=weekly\|daily` query param to `get_sector_rotation_history` |
| `daily_update.py` | Add `_regenerate_sector_rrg_daily()` called after existing weekly regeneration |
| `rotation.html` | Add interval toggle UI; update fetch, renderFrame label, setTimeRange, exitDrill |
| `tests/test_rotation_history.py` | Add tests for daily cache path and `?interval=daily` endpoint |

---

## Task 1: Parameterise `generate_rotation_history.py` for daily interval

**Files:**
- Modify: `generate_rotation_history.py`

- [ ] **Step 1: Update `compute_snapshots()` to accept a `step` parameter**

In `generate_rotation_history.py`, change the function signature and loop. The index key becomes `week_index` when step=5 and `day_index` when step=1:

```python
def compute_snapshots(step: int = 5) -> list[dict]:
    """Load all price data and compute RS-Ratio / RS-Momentum for every `step` trading days."""
    # ... existing DB query and DataFrame setup unchanged ...

    snapshots = []
    point_index = 0
    index_key = "week_index" if step == 5 else "day_index"
    for t_idx in range(65, len(dates), step):
        # ... existing per-snapshot computation unchanged ...

        snapshots.append({
            "date": date_t.strftime("%Y-%m-%d"),
            index_key: point_index,
            "sectors": sectors_out,
        })
        point_index += 1

    return snapshots
```

- [ ] **Step 2: Update `generate()` to accept `interval` and write to the correct cache file**

```python
def generate(force: bool = False, interval: str = "weekly") -> None:
    today = datetime.date.today().isoformat()
    suffix = "_daily" if interval == "daily" else ""
    cache = Path(f"/tmp/sector_rotation_history{suffix}_{today}.json")
    lock  = Path(f"/tmp/sector_rotation_history{suffix}_{today}.lock")

    if cache.exists() and not force:
        return

    if lock.exists():
        for _ in range(24):
            time.sleep(5)
            if cache.exists():
                return
        lock.unlink(missing_ok=True)

    step = 1 if interval == "daily" else 5
    lock.write_text(str(os.getpid()))
    try:
        snapshots = compute_snapshots(step=step)
        result = {
            "generated_at": today,
            "interval": interval,
            "total_snapshots": len(snapshots),
            "snapshots": snapshots,
        }
        _atomic_write(cache, result)
    except Exception:
        traceback.print_exc()
    finally:
        lock.unlink(missing_ok=True)
```

- [ ] **Step 3: Update `__main__` block to read `--interval` flag**

```python
if __name__ == "__main__":
    force    = "--force" in sys.argv
    interval = "daily" if "--interval" in sys.argv and sys.argv[sys.argv.index("--interval") + 1] == "daily" else "weekly"
    print(f"Generating rotation history (force={force}, interval={interval})...")
    t0 = time.time()
    generate(force=force, interval=interval)
    print(f"Done in {time.time() - t0:.1f}s")
```

- [ ] **Step 4: Verify weekly cache still generates correctly**

```bash
cd /home/ubuntu/stock-market-dashboard
python generate_rotation_history.py --force
python -c "
import json; from pathlib import Path; import datetime
d = json.loads(Path(f'/tmp/sector_rotation_history_{datetime.date.today().isoformat()}.json').read_text())
print('interval:', d.get('interval', 'weekly'))
print('total_snapshots:', d['total_snapshots'])
print('first key check:', list(d['snapshots'][0].keys()))
"
```

Expected: `interval: weekly`, `total_snapshots: ~302`, keys include `week_index`.

- [ ] **Step 5: Verify daily cache generates correctly (this takes ~70s)**

```bash
python generate_rotation_history.py --force --interval daily
python -c "
import json; from pathlib import Path; import datetime
d = json.loads(Path(f'/tmp/sector_rotation_history_daily_{datetime.date.today().isoformat()}.json').read_text())
print('interval:', d['interval'])
print('total_snapshots:', d['total_snapshots'])
print('first key check:', list(d['snapshots'][0].keys()))
"
```

Expected: `interval: daily`, `total_snapshots: ~1508`, keys include `day_index`.

- [ ] **Step 6: Commit**

```bash
git add generate_rotation_history.py
git commit -m "feat: add --interval weekly|daily flag to generate_rotation_history"
```

---

## Task 2: Update API endpoint to support `?interval=daily`

**Files:**
- Modify: `api.py:1141-1170`

- [ ] **Step 1: Write the failing tests first**

In `tests/test_rotation_history.py`, add:

```python
def _write_fake_daily_cache(snapshots: list) -> Path:
    today = datetime.date.today().isoformat()
    cache = Path(f"/tmp/sector_rotation_history_daily_{today}.json")
    cache.write_text(json.dumps({
        "generated_at": today,
        "interval": "daily",
        "total_snapshots": len(snapshots),
        "snapshots": snapshots,
    }))
    return cache


def _clear_daily_cache():
    today = datetime.date.today().isoformat()
    Path(f"/tmp/sector_rotation_history_daily_{today}.json").unlink(missing_ok=True)
    Path(f"/tmp/sector_rotation_history_daily_{today}.lock").unlink(missing_ok=True)


def test_daily_returns_200_when_cache_exists():
    fake = {
        "date": "2025-01-01",
        "day_index": 0,
        "sectors": [{"sector": "Energy", "rs_ratio": 105.0, "rs_momentum": 102.0,
                      "quadrant": "Leading", "return_13w": 5.0, "return_4w": 2.0}],
    }
    _write_fake_daily_cache([fake])
    try:
        res = client.get("/api/sector-rotation-history?interval=daily")
        assert res.status_code == 200
        data = res.json()
        assert data["interval"] == "daily"
        assert data["total_snapshots"] == 1
        assert data["snapshots"][0]["day_index"] == 0
    finally:
        _clear_daily_cache()


def test_daily_returns_202_when_no_cache():
    _clear_daily_cache()
    res = client.get("/api/sector-rotation-history?interval=daily")
    assert res.status_code == 202
    assert res.json()["status"] == "generating"


def test_weekly_unaffected_by_interval_param():
    fake = {
        "date": "2025-01-01",
        "week_index": 0,
        "sectors": [{"sector": "Energy", "rs_ratio": 105.0, "rs_momentum": 102.0,
                      "quadrant": "Leading", "return_13w": 5.0, "return_4w": 2.0}],
    }
    _write_fake_cache([fake])
    try:
        res = client.get("/api/sector-rotation-history?interval=weekly")
        assert res.status_code == 200
        assert res.json()["snapshots"][0]["week_index"] == 0
    finally:
        _clear_cache()
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
venv/bin/python -m pytest tests/test_rotation_history.py::test_daily_returns_200_when_cache_exists tests/test_rotation_history.py::test_daily_returns_202_when_no_cache tests/test_rotation_history.py::test_weekly_unaffected_by_interval_param -v
```

Expected: all 3 FAIL (endpoint doesn't accept `?interval` yet).

- [ ] **Step 3: Update `get_sector_rotation_history` in `api.py`**

Replace the existing function at line ~1141:

```python
@app.get("/api/sector-rotation-history")
def get_sector_rotation_history(interval: str = "weekly"):
    """
    Returns all weekly or daily RRG snapshots for animated playback.
    interval=weekly  → /tmp/sector_rotation_history_{date}.json  (default)
    interval=daily   → /tmp/sector_rotation_history_daily_{date}.json
    Returns 202 {status: 'generating'} while computing.
    """
    import datetime, subprocess, sys
    from pathlib import Path
    from fastapi.responses import JSONResponse
    import json as _json

    if interval not in ("weekly", "daily"):
        raise HTTPException(status_code=400, detail="interval must be 'weekly' or 'daily'")

    today = datetime.date.today().isoformat()
    suffix = "_daily" if interval == "daily" else ""
    cache = Path(f"/tmp/sector_rotation_history{suffix}_{today}.json")

    if cache.exists():
        try:
            return _json.loads(cache.read_text())
        except (ValueError, OSError):
            cache.unlink(missing_ok=True)

    script = Path(__file__).parent / "generate_rotation_history.py"
    args = [sys.executable, str(script), "--force"]
    if interval == "daily":
        args += ["--interval", "daily"]
    subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return JSONResponse(status_code=202, content={
        "status": "generating",
        "message": f"Computing {interval} history snapshots, retry in 30 seconds.",
    })
```

- [ ] **Step 4: Run the tests — all 3 should pass**

```bash
venv/bin/python -m pytest tests/test_rotation_history.py -v
```

Expected: all tests PASS, including the 3 new ones.

- [ ] **Step 5: Commit**

```bash
git add api.py tests/test_rotation_history.py
git commit -m "feat: add ?interval=daily support to /api/sector-rotation-history"
```

---

## Task 3: Add `_regenerate_sector_rrg_daily()` to `daily_update.py`

**Files:**
- Modify: `daily_update.py`

- [ ] **Step 1: Add the new method after `_regenerate_sector_rrg`**

In `daily_update.py`, after line 61, add:

```python
    def _regenerate_sector_rrg_daily(self):
        """Force-regenerate today's daily sector rotation history cache from latest DB data."""
        script = Path(__file__).parent / "generate_rotation_history.py"
        print("\n--- Regenerating daily sector RRG cache ---")
        result = subprocess.run(
            [sys.executable, str(script), "--force", "--interval", "daily"],
            check=False,
        )
        if result.returncode != 0:
            print(f"  WARNING: daily sector RRG regeneration failed (exit {result.returncode})")
        else:
            print("  Daily sector RRG cache updated.")
```

- [ ] **Step 2: Call it in `run()` after the weekly regeneration**

In `daily_update.py`, the `run()` method ends with:

```python
        self._regenerate_sector_rrg()
        self._warmup_industry_cache()
        self._refresh_shiller_cape()
```

Change to:

```python
        self._regenerate_sector_rrg()
        self._regenerate_sector_rrg_daily()
        self._warmup_industry_cache()
        self._refresh_shiller_cape()
```

- [ ] **Step 3: Verify the script parses correctly**

```bash
venv/bin/python -c "import daily_update; print('OK')"
```

Expected: `OK` with no import errors.

- [ ] **Step 4: Commit**

```bash
git add daily_update.py
git commit -m "feat: regenerate daily RRG cache in daily_update"
```

---

## Task 4: Add interval toggle UI and state to `rotation.html`

**Files:**
- Modify: `rotation.html`

- [ ] **Step 1: Add CSS for the interval toggle**

The existing `.benchmark-toggle` and `.benchmark-btn` classes already provide the correct styling. No new CSS needed — the interval toggle will reuse them with a different `id`.

- [ ] **Step 2: Add the HTML toggle in ctrl-bar (before benchmark toggle)**

Find this block (around line 218):

```html
            <div class="benchmark-toggle" id="benchmarkToggle" style="display:none;">
                <button class="benchmark-btn active" data-bm="market">vs 全市場</button>
                <button class="benchmark-btn" data-bm="sector">vs Sector</button>
            </div>
```

Insert the interval toggle immediately before it:

```html
            <div class="benchmark-toggle" id="intervalToggle">
                <button class="benchmark-btn active" data-interval="weekly">週</button>
                <button class="benchmark-btn" data-interval="daily">日</button>
            </div>

            <div class="benchmark-toggle" id="benchmarkToggle" style="display:none;">
                <button class="benchmark-btn active" data-bm="market">vs 全市場</button>
                <button class="benchmark-btn" data-bm="sector">vs Sector</button>
            </div>
```

- [ ] **Step 3: Add `currentInterval` state variable**

Near the top of the `<script>` block, the existing state variables start around line 355:

```js
    let allSnapshots = [];
```

Add after it:

```js
    let currentInterval = 'weekly';
```

- [ ] **Step 4: Update `renderFrame()` to show correct label**

Find (around line 647):

```js
        document.getElementById('timelineDate').textContent = snap.date + ` (week ${snap.week_index + 1}/${allSnapshots.length})`;
```

Replace with:

```js
        const idxKey   = currentInterval === 'daily' ? 'day_index' : 'week_index';
        const unitWord = currentInterval === 'daily' ? 'day' : 'week';
        document.getElementById('timelineDate').textContent =
            snap.date + ` (${unitWord} ${(snap[idxKey] ?? 0) + 1}/${allSnapshots.length})`;
```

- [ ] **Step 5: Update `setTimeRange()` to use trading days in daily mode**

Find (around line 690):

```js
    function setTimeRange(weeks) {
        const wasPlaying = !!playTimer;
        stopPlay();
        playSnapshots = (weeks === 0 || weeks >= allSnapshots.length)
            ? allSnapshots
            : allSnapshots.slice(-weeks);
        currentIdx = playSnapshots.length - 1;
        renderFrame(currentIdx);
        if (wasPlaying) startPlay();
    }
```

Replace with:

```js
    function setTimeRange(weeks) {
        const wasPlaying = !!playTimer;
        stopPlay();
        // In daily mode, range buttons represent trading days (1Y=260, 6M=130, 3M=65)
        const count = currentInterval === 'daily' && weeks > 0 ? weeks * 5 : weeks;
        playSnapshots = (count === 0 || count >= allSnapshots.length)
            ? allSnapshots
            : allSnapshots.slice(-count);
        currentIdx = playSnapshots.length - 1;
        renderFrame(currentIdx);
        if (wasPlaying) startPlay();
    }
```

- [ ] **Step 6: Update `pollUntilReady()` to pass interval param**

Find (around line 989):

```js
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
```

Replace with:

```js
    async function pollUntilReady(interval) {
        const url = interval === 'daily'
            ? '/api/sector-rotation-history?interval=daily'
            : '/api/sector-rotation-history';
        for (let attempt = 0; attempt < 20; attempt++) {
            await new Promise(r => setTimeout(r, attempt === 0 ? 0 : 10000));
            const res = await fetch(url);
            if (res.status === 200) return res.json();
            if (res.status !== 202) throw new Error('API error ' + res.status);
            document.getElementById('loadingMsg').textContent =
                `計算中… 請稍候 (${attempt * 10}s elapsed)`;
        }
        throw new Error('Timed out waiting for history data');
    }
```

- [ ] **Step 7: Update `load()` to pass interval to `pollUntilReady`**

Find (around line 1026):

```js
    async function load() {
        try {
            const data = await pollUntilReady();
```

Replace with:

```js
    async function load() {
        try {
            const data = await pollUntilReady(currentInterval);
```

- [ ] **Step 8: Update `exitDrill()` sector reload to respect `currentInterval`**

Find (around line 892):

```js
        fetch('/api/sector-rotation-history').then(r => r.json()).then(data => {
            allSnapshots = smoothSnapshots(data.snapshots || []);
            playSnapshots = allSnapshots.length > 52 ? allSnapshots.slice(-52) : allSnapshots;
```

Replace with:

```js
        const histUrl = currentInterval === 'daily'
            ? '/api/sector-rotation-history?interval=daily'
            : '/api/sector-rotation-history';
        fetch(histUrl).then(r => r.json()).then(data => {
            allSnapshots = smoothSnapshots(data.snapshots || []);
            const defaultCount = currentInterval === 'daily' ? 260 : 52;
            playSnapshots = allSnapshots.length > defaultCount ? allSnapshots.slice(-defaultCount) : allSnapshots;
```

- [ ] **Step 9: Wire up the interval toggle in `initControls()`**

At the end of `initControls()` (around line 970, before the closing `}`), add:

```js
        // Interval toggle (週 / 日)
        document.querySelectorAll('#intervalToggle .benchmark-btn').forEach(btn => {
            btn.addEventListener('click', async () => {
                const newInterval = btn.dataset.interval;
                if (newInterval === currentInterval) return;
                currentInterval = newInterval;
                document.querySelectorAll('#intervalToggle .benchmark-btn')
                    .forEach(b => b.classList.toggle('active', b.dataset.interval === newInterval));
                stopPlay();
                try {
                    const data = await pollUntilReady(currentInterval);
                    allSnapshots = smoothSnapshots(data.snapshots || []);
                    const defaultCount = currentInterval === 'daily' ? 260 : 52;
                    playSnapshots = allSnapshots.length > defaultCount
                        ? allSnapshots.slice(-defaultCount)
                        : allSnapshots;
                    currentIdx = playSnapshots.length - 1;
                    // Re-apply active range button
                    const activeRangeBtn = document.querySelector('.range-btn.active[data-weeks]');
                    if (activeRangeBtn) setTimeRange(parseInt(activeRangeBtn.dataset.weeks));
                    else renderFrame(currentIdx);
                } catch (e) {
                    console.error('Interval switch failed:', e);
                }
            });
        });
```

- [ ] **Step 10: Restart service and verify in browser**

```bash
sudo systemctl restart stock-api
```

Open http://localhost:8087/rotation.html. Confirm:
- "週 | 日" toggle appears in the control bar
- Default is 週 (active/highlighted)
- Clicking 日 fetches `/api/sector-rotation-history?interval=daily` (check browser Network tab)
- Timeline label switches between "(week N/302)" and "(day N/1508)"
- 1Y range in daily mode shows ~260 snapshots instead of 52

- [ ] **Step 11: Commit**

```bash
git add rotation.html
git commit -m "feat: add 週/日 interval toggle to RRG control bar"
```

---

## Task 5: Run full test suite and push

- [ ] **Step 1: Run all tests**

```bash
cd /home/ubuntu/stock-market-dashboard
venv/bin/python -m pytest tests/ -v
```

Expected: all tests PASS.

- [ ] **Step 2: Push**

```bash
git push
```
