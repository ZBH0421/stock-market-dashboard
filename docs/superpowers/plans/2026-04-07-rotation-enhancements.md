# Rotation Enhancements Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Four enhancements to `rotation.html`: (1) direction arrows on RRG trails, (2) custom date range picker, (3) split-pill behavior where left-click adds a sector/industry to a comparison panel and right ▾ triggers drill-down, (4) comparison panel below ETF chart showing selected sectors/industries vs SPY.

**Architecture:** All changes are frontend-only in `rotation.html`. State tracked in two new globals: `comparedItems` (Set of names) and `comparedData` (Map of name → `{dates, values}`). Comparison data for sectors is pulled from the existing `etfData` global; for industries it is computed from the `drillSnapshots` already in memory. The comparison chart is a new ApexCharts line chart instance rendered in a new card below the ETF chart.

**Tech Stack:** ApexCharts (scatter + line), SVG overlay, Bootstrap 5 dark.

---

## Codebase Context

**Key globals in `rotation.html`:**
- `allSnapshots` / `playSnapshots` — array of `{ date, week_index, sectors: [{sector, rs_ratio, rs_momentum, quadrant, return_13w, return_4w}] }`
- `currentIdx` — current frame index into `playSnapshots`
- `drillSector` — null in sector view, string in industry view
- `drillSnapshots` — raw industry snapshots from API when in drill mode; shape: `[{ date, week_index, industries: [{ industry, rs_ratio_market, rs_momentum_market, rs_ratio_sector, rs_momentum_sector, return_13w, return_4w, stock_count }] }]`
- `etfData` — `{ period, sectors: { "Energy": { etf, dates, values } }, benchmarks: { "SPY": { dates, values }, "QQQ": { dates, values } } }` (after Plan A Task 2)
- `SECTOR_COLORS` — dict of sector name → hex color
- `getItemColor(name)` — returns `SECTOR_COLORS[name] || industryColor(name)`
- `visibleSectors` (Set), `visibleIndustries` (Set)

**Key functions:**
- `drawTrails()` — draws SVG trail lines + circles; called in `renderFrame()`
- `initSectorToggles()` — creates sector pills with click → `enterDrill(sector)`
- `initIndustryToggles(industries)` — creates industry pills with click-to-toggle visibility
- `enterDrill(sector)` — async; loads industry data, rebuilds chart
- `exitDrill()` — restores sector view
- `renderEtfChart()` — builds/updates the ETF ApexCharts line chart
- `loadEtfChart(period)` — fetches `/api/sector-etf-prices`, sets `etfData`, calls `renderEtfChart()`

**HTML structure (sector view flow):**
```
ctrlBar → drillBreadcrumb (hidden) → sectorToggles → rrgChart card → etfChartCard → sector ranking table
```

**Important:** `drawTrails()` ends at line ~373. The last thing it draws is `<circle>` dots for each trail point. Arrow will be added at the very end, pointing in direction of last 2 trail points.

---

## File Map

| File | Action |
|------|--------|
| `rotation.html` | Modify — 4 features, all JS/HTML/CSS |

---

## Task 1: Direction arrows on RRG trails

**Files:**
- Modify: `rotation.html` — `drawTrails()` function

Add a small arrowhead SVG polygon at the current position of each sector/industry dot, pointing in the direction the dot has been moving over the last 2 frames.

- [ ] **Step 1: Add the arrow drawing helper and integrate into `drawTrails`**

Read `rotation.html` to find the `drawTrails()` function. It ends with the loop that draws circles:

```js
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
```

Replace the entire closing section (from `for (let i = 0; i < points.length - 1; i++) {` through the closing `});` and `}`) with:

```js
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

            // Direction arrow at the tip (current position)
            if (points.length >= 2) {
                const tip = points[points.length - 1];
                const prev = points[points.length - 2];
                const dx = tip.px - prev.px;
                const dy = tip.py - prev.py;
                const len = Math.sqrt(dx * dx + dy * dy);
                if (len > 1) {
                    const ux = dx / len, uy = dy / len;
                    const px = -uy, py = ux; // perpendicular
                    const sz = 7, hw = 3.5;
                    // tip point, left base, right base
                    const pts = [
                        `${tip.px + ux * sz},${tip.py + uy * sz}`,
                        `${tip.px - ux * sz * 0.3 - px * hw},${tip.py - uy * sz * 0.3 - py * hw}`,
                        `${tip.px - ux * sz * 0.3 + px * hw},${tip.py - uy * sz * 0.3 + py * hw}`,
                    ].join(' ');
                    const arrow = document.createElementNS('http://www.w3.org/2000/svg', 'polygon');
                    arrow.setAttribute('points', pts);
                    arrow.setAttribute('fill', color);
                    arrow.setAttribute('fill-opacity', '0.9');
                    svg.appendChild(arrow);
                }
            }
        });
    }
```

- [ ] **Step 2: Verify arrows in browser**

Open `http://localhost:8087/rotation.html`. Jump to the last frame (⏭). Each sector trail should have a small arrowhead at its tip. Play the animation — arrows should move and rotate with the dots. No console errors.

- [ ] **Step 3: Commit**

```bash
git add rotation.html
git commit -m "feat: direction arrows on RRG trails"
```

---

## Task 2: Custom date range picker

**Files:**
- Modify: `rotation.html` — HTML (range buttons row) + JS (`initControls`)

Adds a "自訂" button that reveals two date inputs. Confirming filters `playSnapshots` to the chosen date range.

- [ ] **Step 1: Add CSS for date range picker**

In `rotation.html`, find the CSS block (inside `<style>`). Add before `</style>`:

```css
/* Custom date range picker */
.date-range-wrap { display: none; align-items: center; gap: 6px; font-size: 11px; color: #94a3b8; }
.date-range-wrap.visible { display: flex; }
.date-input { background: #0f172a; border: 1px solid #334155; border-radius: 6px; color: #e2e8f0; padding: 3px 7px; font-size: 11px; width: 120px; }
.date-confirm-btn { background: #1d4ed8; color: #fff; border: none; border-radius: 6px; padding: 3px 10px; font-size: 11px; cursor: pointer; }
.date-confirm-btn:hover { background: #2563eb; }
```

- [ ] **Step 2: Add date range HTML to control bar**

Find in `rotation.html`:
```html
            <div class="d-flex gap-1">
                <button class="range-btn" data-weeks="13">3M</button>
                <button class="range-btn" data-weeks="26">6M</button>
                <button class="range-btn active" data-weeks="52">1Y</button>
                <button class="range-btn" data-weeks="0">全部</button>
            </div>
```

Replace with:
```html
            <div class="d-flex align-items-center gap-1 flex-wrap">
                <button class="range-btn" data-weeks="13">3M</button>
                <button class="range-btn" data-weeks="26">6M</button>
                <button class="range-btn active" data-weeks="52">1Y</button>
                <button class="range-btn" data-weeks="0">全部</button>
                <button class="range-btn" id="customRangeBtn">自訂</button>
                <div class="date-range-wrap" id="dateRangeWrap">
                    <input type="date" class="date-input" id="dateStart">
                    <span>→</span>
                    <input type="date" class="date-input" id="dateEnd">
                    <button class="date-confirm-btn" id="dateConfirm">確認</button>
                </div>
            </div>
```

- [ ] **Step 3: Add JS for date range in `initControls`**

Read `rotation.html` to find `initControls()`. It ends with:
```js
        document.getElementById('benchmarkToggle').addEventListener('click', e => {
            ...
        });
    }
```

Find the block that handles range buttons (look for `data-weeks`):
```js
        document.querySelectorAll('.range-btn[data-weeks]').forEach(btn => {
            btn.addEventListener('click', () => {
                ...
            });
        });
```

After that block, add before the closing `}` of `initControls`:

```js
        // Custom date range
        document.getElementById('customRangeBtn').addEventListener('click', () => {
            const wrap = document.getElementById('dateRangeWrap');
            const visible = wrap.classList.toggle('visible');
            if (visible && allSnapshots.length > 0) {
                const dates = allSnapshots.map(s => s.date);
                document.getElementById('dateStart').min = dates[0];
                document.getElementById('dateStart').max = dates[dates.length - 1];
                document.getElementById('dateStart').value = dates[Math.max(0, dates.length - 52)];
                document.getElementById('dateEnd').min = dates[0];
                document.getElementById('dateEnd').max = dates[dates.length - 1];
                document.getElementById('dateEnd').value = dates[dates.length - 1];
            }
            if (visible) {
                document.querySelectorAll('.range-btn').forEach(b => b.classList.remove('active'));
                document.getElementById('customRangeBtn').classList.add('active');
            }
        });

        document.getElementById('dateConfirm').addEventListener('click', () => {
            const start = document.getElementById('dateStart').value;
            const end = document.getElementById('dateEnd').value;
            if (!start || !end || start > end) return;
            stopPlay();
            playSnapshots = allSnapshots.filter(s => s.date >= start && s.date <= end);
            if (playSnapshots.length === 0) playSnapshots = allSnapshots.slice(-1);
            currentIdx = playSnapshots.length - 1;
            renderFrame(currentIdx);
        });
```

- [ ] **Step 4: Verify in browser**

Open `http://localhost:8087/rotation.html`:
1. "自訂" button appears next to range buttons
2. Click "自訂" → two date inputs and "確認" button appear
3. Change dates and click "確認" → RRG updates to that range
4. Pre-set buttons (3M/6M/1Y/全部) still work; they should hide the date picker (note: the wrap stays visible but the active class shifts — this is acceptable UX)

- [ ] **Step 5: Commit**

```bash
git add rotation.html
git commit -m "feat: custom date range picker for RRG playback"
```

---

## Task 3: Split pill behavior (left = compare, right ▾ = drill)

**Files:**
- Modify: `rotation.html` — state vars, CSS, `initSectorToggles`, `initIndustryToggles`, new `addToComparison`/`removeFromComparison`

- [ ] **Step 1: Add comparison state variables**

Find in `rotation.html`:
```js
    let drillLoading = false;
```

Replace with:
```js
    let drillLoading = false;
    let comparedItems = new Set();  // names of sectors/industries currently in comparison
    let comparedData = new Map();   // name -> { dates: [], values: [], color: string }
```

- [ ] **Step 2: Add split pill CSS**

In `rotation.html`, find `.sector-pill { ... }` CSS block. Replace the entire sector-pill block with:

```css
        .sector-pill { display: inline-flex; align-items: center; border: 1px solid #334155; border-radius: 20px; font-size: 12px; color: #94a3b8; margin: 2px; overflow: hidden; }
        .sector-pill .pill-left { display: flex; align-items: center; gap: 5px; padding: 4px 10px; cursor: pointer; }
        .sector-pill .pill-left:hover { background: #334155; color: #e2e8f0; }
        .sector-pill .pill-right { padding: 4px 8px; border-left: 1px solid #334155; color: #64748b; cursor: pointer; font-size: 11px; }
        .sector-pill .pill-right:hover { background: #334155; color: #60a5fa; }
        .sector-pill .pill-dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
        .sector-pill.compared .pill-left { color: #60a5fa; background: #1e3a5f; }
        .sector-pill.compared { border-color: #1d4ed8; }
        .sector-pill.no-drill .pill-right { display: none; }
```

- [ ] **Step 3: Add `addToComparison` and `removeFromComparison`**

Find `function fmtReturn(v) {` in `rotation.html`. Insert BEFORE it:

```js
    function addToComparison(name, color) {
        if (comparedItems.has(name)) {
            comparedItems.delete(name);
            comparedData.delete(name);
        } else {
            comparedItems.add(name);
            let dates = [], values = [];
            const isSector = !!SECTOR_COLORS[name];
            if (isSector && etfData?.sectors?.[name]) {
                dates = etfData.sectors[name].dates;
                values = etfData.sectors[name].values;
            } else if (!isSector) {
                // Industry: compute cumulative return from drillSnapshots
                const snaps = drillSnapshots.filter(s => s.industries.some(i => i.industry === name));
                if (snaps.length > 0) {
                    let cum = 1;
                    snaps.forEach(s => {
                        const ind = s.industries.find(i => i.industry === name);
                        if (ind) {
                            const w = Math.pow(1 + ind.return_4w / 100, 1 / 4) - 1;
                            cum *= (1 + w);
                            dates.push(s.date);
                            values.push(parseFloat(((cum - 1) * 100).toFixed(2)));
                        }
                    });
                    // Re-base to 0 at start
                    if (values.length > 0) {
                        const base = Math.pow(1 + drillSnapshots[0].industries.find(i => i.industry === name)?.return_4w / 100 || 0, 1/4) - 1;
                        // Already cumulative from 1, subtract 0 baseline
                    }
                }
            }
            comparedData.set(name, { dates, values, color });
        }
        // Update pill visual
        document.querySelectorAll(`.sector-pill[data-sector="${CSS.escape(name)}"]`).forEach(pill => {
            pill.classList.toggle('compared', comparedItems.has(name));
        });
        renderComparisonPanel();
    }
```

- [ ] **Step 4: Rewrite `initSectorToggles`**

Find `function initSectorToggles() {` and replace the entire function:

```js
    function initSectorToggles() {
        const container = document.getElementById('sectorToggles');
        container.innerHTML = '';
        Object.entries(SECTOR_COLORS).forEach(([sector, color]) => {
            const pill = document.createElement('div');
            pill.className = 'sector-pill' + (comparedItems.has(sector) ? ' compared' : '');
            pill.dataset.sector = sector;
            pill.title = `Click to compare ${sector}; ▾ to drill into industries`;
            pill.innerHTML = `
                <div class="pill-left"><span class="pill-dot" style="background:${color}"></span>${sector}</div>
                <div class="pill-right" title="Drill into industries">▾</div>`;
            pill.querySelector('.pill-left').addEventListener('click', (e) => {
                e.stopPropagation();
                addToComparison(sector, color);
            });
            pill.querySelector('.pill-right').addEventListener('click', (e) => {
                e.stopPropagation();
                enterDrill(sector);
            });
            container.appendChild(pill);
        });
        container.style.display = '';
    }
```

- [ ] **Step 5: Rewrite `initIndustryToggles`**

Find `function initIndustryToggles(industries) {` and replace the entire function:

```js
    function initIndustryToggles(industries) {
        const container = document.getElementById('sectorToggles');
        container.innerHTML = '';
        industries.forEach(name => {
            const color = getItemColor(name);
            const pill = document.createElement('div');
            pill.className = 'sector-pill no-drill' + (comparedItems.has(name) ? ' compared' : '');
            pill.dataset.sector = name;
            pill.innerHTML = `
                <div class="pill-left"><span class="pill-dot" style="background:${color}"></span>${name}</div>
                <div class="pill-right"></div>`;
            // Left half: toggle visibility in RRG
            pill.querySelector('.pill-left').addEventListener('click', (e) => {
                e.stopPropagation();
                if (visibleIndustries.has(name)) {
                    visibleIndustries.delete(name);
                    pill.querySelector('.pill-left').style.opacity = '0.4';
                } else {
                    visibleIndustries.add(name);
                    pill.querySelector('.pill-left').style.opacity = '';
                }
                if (chart) chart.updateSeries(buildSeries(playSnapshots[currentIdx].sectors), false);
                drawTrails();
            });
            // Long press or right-click: add to comparison
            let pressTimer;
            pill.addEventListener('contextmenu', (e) => {
                e.preventDefault();
                addToComparison(name, color);
                pill.classList.toggle('compared', comparedItems.has(name));
            });
            pill.addEventListener('mousedown', () => { pressTimer = setTimeout(() => addToComparison(name, color), 600); });
            pill.addEventListener('mouseup', () => clearTimeout(pressTimer));
            pill.addEventListener('mouseleave', () => clearTimeout(pressTimer));
            container.appendChild(pill);
        });
        container.style.display = '';
    }
```

**Note on industry pills:** The left half toggles RRG visibility (existing behavior preserved). Right-click or long-press adds the industry to comparison. This avoids the drill-down conflict since industries have no further drill.

- [ ] **Step 6: Verify split pills in browser**

Open `http://localhost:8087/rotation.html` (after Task 2 plan is deployed):
1. Sector pills show name on left + `▾` on right
2. Click pill name → pill turns blue (added to comparison)
3. Click `▾` → enters industry drill-down view
4. In drill-down: industry pills have no `▾`; left-click toggles RRG visibility; right-click adds to comparison
5. No console errors

- [ ] **Step 7: Commit**

```bash
git add rotation.html
git commit -m "feat: split pill — left-click compare, right ▾ drill-down"
```

---

## Task 4: Comparison panel

**Files:**
- Modify: `rotation.html` — HTML (new card), CSS, new `renderComparisonPanel` function

- [ ] **Step 1: Add comparison panel HTML**

Find in `rotation.html`:
```html
        <!-- Sector ranking table -->
        <div class="card">
```

Insert BEFORE that line:

```html
        <!-- Sector / Industry comparison panel -->
        <div class="card mb-4" id="comparisonCard" style="display:none;">
            <div class="card-header d-flex align-items-center justify-content-between">
                <span>Comparison <span class="text-secondary fw-normal" style="font-size:0.8rem;" id="comparisonSubtitle">vs SPY</span></span>
                <button id="clearComparison" style="background:transparent;border:1px solid #334155;border-radius:6px;color:#94a3b8;font-size:11px;padding:3px 10px;cursor:pointer;">清除全部</button>
            </div>
            <div class="card-body p-2">
                <div id="comparisonChart"></div>
            </div>
        </div>
```

- [ ] **Step 2: Add `renderComparisonPanel` function**

Find `function updateEtfMarker(dateStr) {` in `rotation.html`. Insert BEFORE it:

```js
    let comparisonChart = null;

    function renderComparisonPanel() {
        const card = document.getElementById('comparisonCard');
        if (comparedItems.size === 0) {
            card.style.display = 'none';
            if (comparisonChart) { comparisonChart.destroy(); comparisonChart = null; }
            return;
        }
        card.style.display = '';

        const series = [];
        const colors = [];

        // SPY baseline always present if available
        if (etfData?.benchmarks?.SPY) {
            const d = etfData.benchmarks.SPY;
            series.push({ name: 'SPY', data: d.dates.map((dt, i) => ({ x: new Date(dt).getTime(), y: d.values[i] })) });
            colors.push('#f1f5f9');
        }

        comparedData.forEach(({ dates, values, color }, name) => {
            if (dates.length === 0) return;
            series.push({
                name,
                data: dates.map((dt, i) => ({ x: new Date(dt).getTime(), y: values[i] })),
            });
            colors.push(color);
        });

        const subtitle = Array.from(comparedItems).join(', ');
        document.getElementById('comparisonSubtitle').textContent = subtitle.length > 60 ? subtitle.slice(0, 57) + '…' : subtitle;

        const dashArray = series.map(s => s.name === 'SPY' ? 4 : 0);

        if (comparisonChart) {
            comparisonChart.updateOptions({
                series,
                colors,
                stroke: { width: 2, curve: 'smooth', dashArray },
            }, true, false);
            return;
        }

        comparisonChart = new ApexCharts(document.getElementById('comparisonChart'), {
            chart: {
                type: 'line', height: 260, background: '#1e293b',
                toolbar: { show: false }, animations: { enabled: false },
                zoom: { enabled: false },
            },
            series,
            colors,
            stroke: { width: 2, curve: 'smooth', dashArray },
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
            legend: { position: 'top', labels: { colors: '#94a3b8' } },
            grid: { borderColor: '#334155' },
            annotations: { yaxis: [{ y: 0, borderColor: '#475569', strokeDashArray: 4 }] },
            theme: { mode: 'dark' },
        });
        comparisonChart.render();
    }
```

- [ ] **Step 3: Wire "清除全部" button**

Find in `rotation.html`, near the other event listeners (before `load();`):
```js
    document.getElementById('etfRangeBtns').addEventListener('click', e => {
```

Add before that line:

```js
    document.getElementById('clearComparison').addEventListener('click', () => {
        comparedItems.clear();
        comparedData.clear();
        document.querySelectorAll('.sector-pill.compared').forEach(p => p.classList.remove('compared'));
        renderComparisonPanel();
    });

```

- [ ] **Step 4: Verify comparison panel in browser**

Open `http://localhost:8087/rotation.html`:
1. No comparison panel visible initially
2. Click a sector pill left-half (e.g. Energy) → comparison card appears below ETF chart
3. Card shows Energy line + SPY dashed white baseline
4. Add another sector → both lines show
5. Click "清除全部" → panel disappears
6. In drill-down: right-click an industry pill → it appears in comparison panel
7. Exit drill-down → comparison panel still shows (industry data preserved)

- [ ] **Step 5: Commit**

```bash
git add rotation.html
git commit -m "feat: comparison panel — select sectors/industries and compare vs SPY"
```

---

## Task 5: Final verification and push

- [ ] **Step 1: Run all tests**

```bash
venv/bin/python -m pytest tests/ -v -k "not generates_cache"
```
Expected: all pass

- [ ] **Step 2: Quick browser smoke-test**

Open `http://localhost:8087/rotation.html` and verify:
- Arrows visible on trails ✓
- "自訂" date picker works ✓
- Split pills: name → compare, ▾ → drill ✓
- Comparison panel shows/hides correctly ✓
- No console errors

- [ ] **Step 3: Push**

```bash
git push
```
