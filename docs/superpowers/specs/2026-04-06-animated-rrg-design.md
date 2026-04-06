# Animated RRG — Design Spec

**Date:** 2026-04-06  
**Feature:** Sector Rotation 頁面加入動態 RRG 動畫（時間軸播放 + 軌跡線）

---

## 目標

讓使用者能看到 11 個 GICS 板塊在過去 ~100 週（2024-01-02 至今）如何在 RRG 圖上移動，透過播放動畫、拖曳時間軸、調整速度與軌跡長度，直觀理解板塊輪動歷史。

---

## 使用者決策（brainstorming 確認）

| 項目 | 選擇 |
|------|------|
| 控制列版面 | A — 單行橫排（播放鍵 + 時間軸 + 速度 + 時間長度） |
| 軌跡樣式 | A — 漸淡細線（愈舊愈透明，最新位置為實心大圓點） |
| 拖尾長度 | C — 可自由調整（slider 控制週數） |

---

## 資料範圍

- DB 有 564 個交易日（2024-01-02 ~ 2026-04-02）
- 每週快照（每 5 個交易日一個點）：共 ~99 個快照
- 每個快照需 13W（65 個交易日）回看窗口
- 可用快照：從第 66 個交易日開始，共 ~99 個

---

## 架構

### 新增：`generate_rotation_history.py`

獨立 script，預計算所有週快照並寫入 `/tmp/sector_rotation_history.json`。

**流程：**
1. 查詢全部 564 天的收盤價 + tickers.market_cap
2. 取得所有交易日列表，每 5 天取一個快照日期（從第 66 天起）
3. 對每個快照日期 T，計算：
   - `date_13w = all_dates[T_idx - 65]`
   - `date_4w  = all_dates[T_idx - 20]`
   - 11 個板塊各自的 market-cap weighted return（13W 與 4W）
   - 全市場 return（13W 與 4W）
   - RS-Ratio = (1 + sector_13w) / (1 + market_13w)
   - RS-Momentum = (1 + sector_4w) / (1 + market_4w)
   - 跨板塊 z-score normalize，中心化至 100
4. 組成 snapshots 陣列，atomic write 至 cache
5. Lock file 防止並發

**快取：** `/tmp/sector_rotation_history_{date}.json`，每日過期（隔天自動重算）

**預計計算時間：** 99 快照 × ~13 次 merge 操作 ≈ 15–30 秒（首次請求時背景執行）

---

### 修改：`api.py`

新增端點：`GET /api/sector-rotation-history`

- 讀取 cache，若存在且為今日 → 直接回傳
- 若不存在 → 以 `subprocess.Popen` 背景執行 `generate_rotation_history.py`，回傳 202 `{status: "generating"}`
- 回傳格式：

```json
{
  "generated_at": "2026-04-06",
  "total_snapshots": 99,
  "snapshots": [
    {
      "date": "2024-04-12",
      "week_index": 0,
      "sectors": [
        {
          "sector": "Energy",
          "rs_ratio": 103.5,
          "rs_momentum": 101.2,
          "quadrant": "Leading",
          "return_13w": 4.2,
          "return_4w": 1.1
        }
      ]
    }
  ]
}
```

---

### 修改：`rotation.html`

**控制列（單行橫排，緊貼圖表上方）：**

```
[⏮] [▶ 播放] [⏭]  ████░░░░░░░░░░  2025-01-15  速度 ──○── 1.0s  拖尾 ──○── 8週  [3M][6M][1Y][全部]
```

**元素說明：**

| 元素 | 說明 |
|------|------|
| ⏮ / ⏭ | 跳到第一/最後一個快照 |
| ▶ 播放 / ⏸ 暫停 | 切換自動播放 |
| 時間軸 scrubber | 可拖曳，顯示目前週次/日期 |
| 速度 slider | 0.2s ~ 3.0s / 幀，預設 1.0s |
| 拖尾 slider | 4週 ~ 26週，預設 8週 |
| 時間長度按鈕 | 3M / 6M / 1Y / 全部 — 控制播放範圍 |

**圖表邏輯：**

- 使用 ApexCharts scatter chart（延續現有設計）
- 每幀呼叫 `chart.updateSeries()` 更新當前位置（11 個實心大圓點）
- 軌跡線：用 SVG overlay 疊加在 ApexCharts 上，繪製過去 N 週的漸淡折線
  - 透明度：最舊的點 opacity 0.05，線性遞增至最新點前 opacity 0.8
  - 線寬：1.5px
  - 顏色：沿用各象限顏色（Leading=綠、Weakening=橘、Lagging=紅、Improving=藍）
- 圖表大小不變，保留現有四象限標籤與 crosshair

**載入流程：**

1. 頁面載入 → 呼叫 `/api/sector-rotation-history`
2. 若 202 → 顯示「資料計算中，約需 30 秒…」spinner，每 10 秒 poll 一次
3. 資料就緒 → 初始化圖表，顯示最後一個快照（最新），播放停止
4. 使用者按播放 → 從時間長度選擇的起點開始往最新播放

**時間長度按鈕：**

- 3M → 從最新快照往前 13 個快照
- 6M → 26 個快照
- 1Y → 52 個快照
- 全部 → 所有 99 個快照

**靜態模式（預設）：**

不播放時，圖表顯示最新快照位置 + 選定拖尾週數的靜態軌跡，等同現有 rotation.html 的增強版。現有的 `/api/sector-rotation` endpoint 和 ranking table 保持不變。

---

## 不做的事

- 不做個別 sector 顯示/隱藏（畫面已夠複雜）
- 不做 sector 點擊展開詳情（超出範圍）
- 不做自訂日期範圍選擇器（3M/6M/1Y/全部已足夠）
- ranking table 在動畫播放時不即時更新（效能考量，只在暫停時更新）

---

## 檔案異動

| 檔案 | 動作 |
|------|------|
| `generate_rotation_history.py` | 新增 |
| `api.py` | 新增 `/api/sector-rotation-history` 端點 |
| `rotation.html` | 修改：加入控制列 + SVG 軌跡疊層 + 動畫邏輯 |

---

## 測試重點

- 首次請求 202 → polling → 資料就緒流程
- 拖曳時間軸到任意位置 → 圖表正確更新
- 時間長度切換 → 播放範圍正確
- 速度 slider 邊界值（0.2s 最快，3s 最慢）
- 拖尾 slider 邊界值（4週最短，26週最長）
- 播放到最後一幀 → 自動停止
