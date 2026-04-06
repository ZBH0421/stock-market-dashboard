# Industry Drill-down RRG — Design Spec

**Date:** 2026-04-06
**Feature:** rotation.html 加入 industry 級別的 RRG 鑽取功能

---

## 目標

讓使用者從 11 個 GICS sector 的 RRG 鑽取進入單一 sector 底下的 sub-industry RRG，支援兩種 benchmark（vs 全市場 / vs 所屬 sector），使用相同的動畫播放控制。

---

## 使用者決策

| 項目 | 選擇 |
|------|------|
| drill-down 觸發 | 單擊 sector pill |
| benchmark 切換位置 | 控制列右側 |
| benchmark 選項 | vs 全市場 + vs 所屬 sector（兩者皆有，可切換） |
| ETF 圖在 drill-down 時 | 隱藏（industry 無對應 ETF） |

---

## 使用者流程

```
Sector 視圖（預設）
  → 單擊 Energy pill
  → breadcrumb：「Sector Rotation › Energy  ← 返回」
  → pill 列換成 Energy 底下的 industry（例如：Oil & Gas E&P、Coal、Uranium...）
  → RRG 顯示 industry 圓點與軌跡
  → 控制列右側：[vs 全市場] [vs Energy] 切換
  → ETF 圖隱藏
  → 點「← 返回」回到 sector 視圖
```

---

## 資料範圍

- DB 有 145 個 industry，分屬 11 個 GICS sector
- 每個 sector 底下 3–12 個 industry
- 過濾掉「Shell Companies」（無意義）
- Industry 股票數 < 3 支者標記警示，但仍顯示
- 歷史資料：2020-01-01 ~ 今日（和 sector RRG 相同）

---

## 架構

### 新增：`generate_industry_rotation_history.py`

接受 CLI 參數 `--sector <sector_name>`，計算該 sector 底下所有 industry 的週快照。

**流程：**
1. 從 DB 查詢該 sector 底下所有 industry 的個股收盤價與市值
2. 使用和 `generate_rotation_history.py` 相同的向量化計算方式
3. 每個快照計算兩組 RS 值：
   - `vs_market`：industry return / 全市場 return（z-score 跨 industry 標準化至 100）
   - `vs_sector`：industry return / 所屬 sector return（z-score 跨 industry 標準化至 100）
4. Lock file 防並發：`/tmp/industry_rotation_{sector}_{date}.lock`
5. Atomic write cache：`/tmp/industry_rotation_history_{sector}_{date}.json`

**Cache 格式：**
```json
{
  "generated_at": "2026-04-06",
  "sector": "Energy",
  "total_snapshots": 250,
  "snapshots": [
    {
      "date": "2020-06-12",
      "week_index": 0,
      "industries": [
        {
          "industry": "Oil, Gas & Consumable Fuels",
          "rs_ratio_market": 103.5,
          "rs_momentum_market": 101.2,
          "rs_ratio_sector": 105.1,
          "rs_momentum_sector": 102.8,
          "quadrant_market": "Leading",
          "quadrant_sector": "Leading",
          "return_13w": 4.2,
          "return_4w": 1.1,
          "stock_count": 42
        }
      ]
    }
  ]
}
```

**Cache 過期：** 每日（檔名含日期），不寫 error cache（允許 retry）。

---

### 修改：`api.py`

新增端點：`GET /api/industry-rotation-history?sector=Energy`

- sector 參數必填，需在 11 個 GICS sector 之中，否則 400
- 讀取 cache，若存在且為今日 → 直接回傳
- 若不存在 → 背景執行 `generate_industry_rotation_history.py --sector Energy`，回傳 202
- Sector name 含空格需 URL encode（e.g., `Information%20Technology`）

---

### 修改：`rotation.html`

**狀態變數：**
```js
let drillSector = null;          // null = sector 視圖，string = industry 視圖
let drillBenchmark = 'market';   // 'market' | 'sector'
let drillSnapshots = [];         // industry 的快照資料
```

**Sector pill 行為改變：**
- 單擊 → `enterDrill(sectorName)`
- 原本的 show/hide 功能移除（drill-down 視圖中 industry pill 才有 toggle）

**drill-down 進入流程（`enterDrill(sector)`）：**
1. 停止播放
2. 顯示 breadcrumb（含 ← 返回按鈕）
3. 控制列右側顯示 benchmark toggle
4. 隱藏 ETF 圖、sector pill 列
5. 呼叫 `/api/industry-rotation-history?sector=Energy`
6. 202 → spinner + polling（每 10 秒，最多 30 次）
7. 資料就緒 → 顯示 industry pill 列，初始化 RRG（預設 1Y 範圍，最後一幀）

**Benchmark 切換（`setBenchmark(mode)`）：**
- 切換 `drillBenchmark`
- 重建 `drillSnapshots`（用對應欄位的 rs_ratio/rs_momentum）
- 更新圖表與軌跡

**返回 sector 視圖（`exitDrill()`）：**
1. 清除 `drillSector`、`drillSnapshots`
2. 隱藏 breadcrumb、benchmark toggle、industry pills
3. 顯示 sector pill 列、ETF 圖
4. 重新載入 sector 快照（allSnapshots 已在記憶體中）
5. 顯示最後一幀

**Industry pill：**
- 每個 industry 一個 pill，顏色用 hash 函數從名稱生成（固定顏色）
- 單擊 toggle 顯示/隱藏（同 sector 視圖的 pill 行為）

**Loading 畫面（drill-down 計算中）：**
```
「Energy industry 資料計算中，約需 30 秒…」+ spinner
```

**Smoothing：** 同 sector RRG，使用 3-week trailing moving average。

---

## 不做的事

- 不做 industry 的 ETF 圖（無對應 ticker）
- 不做 industry 內的個股展開（超出範圍）
- 不做超過一層的鑽取（industry 不能再往下）
- 不做 industry 的動態軌跡顏色隨象限改變（統一用固定顏色）

---

## 測試重點

- `GET /api/industry-rotation-history?sector=Energy` → 202 → polling → 200
- `GET /api/industry-rotation-history?sector=Invalid` → 400
- benchmark 切換：圖表正確更新為對應欄位
- 返回 sector 視圖：狀態完整還原
- industry 數 < 3 的 sector 不崩潰

---

## 檔案異動

| 檔案 | 動作 |
|------|------|
| `generate_industry_rotation_history.py` | 新增 |
| `api.py` | 新增 `/api/industry-rotation-history` 端點 |
| `rotation.html` | 修改：drill-down 邏輯、industry pills、benchmark toggle |
| `tests/test_industry_rotation.py` | 新增 |
