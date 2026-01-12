---
description: 使用 Git Worktree 建立、測試、清理前端頁面的完整流程
---
1. **檢查/清理舊的 worktree**  
   ```bash
   cd e:\Antigravity ticker catch
   git worktree prune
   ```

2. **建立新 worktree**  
   ```bash
   git worktree add .worktrees/stock-detail -b feature/stock-detail
   ```

3. **安裝依賴**（在 worktree 內）  
   ```bash
   cd .worktrees/stock-detail
   python -m venv venv
   .\\venv\\Scripts\\activate
   pip install -r requirements.txt   # 或手動安裝 fastapi uvicorn sqlalchemy psycopg2-binary yfinance pandas
   ```

4. **啟動後端 API**（同一目錄）  
   ```bash
   uvicorn api:app --reload
   ```

5. **驗證 API**（另開 terminal）  
   ```bash
   curl -s http://127.0.0.1:8000/api/stock/AAPL | python -m json.tool
   ```

6. **啟動前端伺服器**（同 worktree 根目錄）  
   ```bash
   python -m http.server 8080
   ```

7. **在瀏覽器測試**  
   - 打開 `http://127.0.0.1:8080/index.html`  
   - 點擊或直接開 `stock.html?ticker=AAPL`  
   - 確認圖表、統計資料、新聞區塊正常顯示

8. **（可選）執行自動化測試**  
   ```bash
   pip install pytest
   pytest -q test_stock_api.py
   ```

9. **測試完成後清理**  
   ```bash
   cd e:\Antigravity ticker catch
   git worktree remove .worktrees/stock-detail
   git worktree prune
   ```
---
**備註**  
- `config.js` 已加入 `window.location.protocol === "file:"` 判斷，若直接以 `file://` 開啟 `stock.html` 仍會指向本機 API。
- 若你在 Render 上部署，請確保環境變數（DB_HOST、DB_USER、DB_PASSWORD）已在 Render Dashboard 正確設定。
