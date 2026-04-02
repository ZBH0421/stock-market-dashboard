# Stock Market Dashboard — Claude Instructions

## Dev Workflow（必須遵守）

**每次開始任何開發任務前，先判斷任務類型，然後遵照對應流程。**

### 任務路由

- 新功能 / 大改動 → Large Feature Flow
- Bug fix → Bug Fix Flow
- 小修改 / 重構 → Small Change Flow

---

### Large Feature Flow

1. **`/office-hours`** — 產品驗證（gstack）
2. **`superpowers:brainstorming`** — 技術方案探索
3. **`superpowers:writing-plans`** — 產生詳細計劃
4. **`/plan-eng-review`** — 審查計劃架構（gstack）
5. **`superpowers:subagent-driven-development`** — 派 subagent 平行執行，含 TDD + 兩階段 review
6. **`superpowers:verification-before-completion`** — 確認完成前跑測試
7. **`/review`** — 靜態 diff 審查（gstack）
8. **`/qa http://localhost:8087`** — 瀏覽器測試（gstack）
9. **`git push`** — 推送到 GitHub

### Bug Fix Flow

1. **`superpowers:systematic-debugging`** — 先診斷
2. **`superpowers:test-driven-development`** — 寫失敗測試再修
3. **`superpowers:verification-before-completion`** — 確認修好
4. **`/qa http://localhost:8087`** — 瀏覽器驗證
5. **`git push`**

### Small Change Flow

1. 直接實作（必要時用 `superpowers:subagent-driven-development`）
2. **`superpowers:verification-before-completion`**
3. **`git push`**

---

## 專案設定

- **服務重啟**：`sudo systemctl restart stock-api`
- **前端**：`http://localhost:8087`
- **API**：`http://localhost:8000`
- **測試**：`venv/bin/python -m pytest tests/ -v`
- **更新 log**：`tail -f /tmp/daily_update.log`
- **DB**：PostgreSQL 17，`stock_db`，user: `stock_user`

## 自動排程

- 每日 23:00 UTC（台灣早上 7:00）自動跑 `daily_update.py` 更新股價
- Log：`/tmp/daily_update.log`
