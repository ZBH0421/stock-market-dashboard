# Stock Market Dashboard Local Deployment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deploy stock-market-dashboard on this Ubuntu server with local PostgreSQL 17, FastAPI via Gunicorn, and Nginx as reverse proxy/static file server.

**Architecture:** Nginx on port 80 serves static HTML and proxies `/api/*` to Gunicorn on port 8000. Gunicorn runs FastAPI workers that connect to local PostgreSQL 17. The Supabase backup (40MB, already at `/home/ubuntu/supabase_backup.dump`) is restored into a local `stock_db` database.

**Tech Stack:** PostgreSQL 17, Python 3.10, FastAPI, Gunicorn + uvicorn workers, Nginx, systemd

---

## File Map

| File | Action | Purpose |
|---|---|---|
| `/etc/systemd/system/stock-api.service` | Create | Systemd unit to run Gunicorn |
| `/etc/nginx/sites-available/stock-dashboard` | Create | Nginx config: static files + API proxy |
| `/etc/nginx/sites-enabled/stock-dashboard` | Symlink | Enable Nginx site |
| `/home/ubuntu/stock-market-dashboard/.env` | Create | Local DB credentials |
| `/home/ubuntu/stock-market-dashboard/config.js` | Modify | Point API to same-host (relative) |
| `/home/ubuntu/stock-market-dashboard/venv/` | Create | Python virtualenv |

---

## Task 1: Install PostgreSQL 17 Server

**Files:** none (system package)

- [ ] **Step 1: Install the server package**

```bash
sudo apt-get install -y postgresql-17
```

Expected: installs without error, service starts automatically.

- [ ] **Step 2: Verify PostgreSQL is running**

```bash
sudo systemctl status postgresql
```

Expected: `active (running)` in output.

- [ ] **Step 3: Commit checkpoint note**

```bash
cd /home/ubuntu/stock-market-dashboard
git commit --allow-empty -m "chore: PostgreSQL 17 server installed"
```

---

## Task 2: Create Database and User

**Files:** none (DB setup)

- [ ] **Step 1: Create the database user**

```bash
sudo -u postgres psql -c "CREATE USER stock_user WITH PASSWORD 'stock_pass_2026';"
```

Expected: `CREATE ROLE`

- [ ] **Step 2: Create the database**

```bash
sudo -u postgres psql -c "CREATE DATABASE stock_db OWNER stock_user;"
```

Expected: `CREATE DATABASE`

- [ ] **Step 3: Grant privileges**

```bash
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE stock_db TO stock_user;"
```

Expected: `GRANT`

- [ ] **Step 4: Verify connection**

```bash
PGPASSWORD=stock_pass_2026 psql -h localhost -U stock_user -d stock_db -c "SELECT version();"
```

Expected: PostgreSQL 17.x version string printed.

---

## Task 3: Restore Supabase Backup

**Files:** uses `/home/ubuntu/supabase_backup.dump` (already exists, 40MB)

- [ ] **Step 1: Restore the dump**

```bash
PGPASSWORD=stock_pass_2026 pg_restore \
  --no-owner --no-acl \
  -h localhost -U stock_user -d stock_db \
  /home/ubuntu/supabase_backup.dump 2>&1
```

Expected: completes with no fatal errors (warnings about extensions like `pg_stat_statements` are OK to ignore).

- [ ] **Step 2: Verify tables exist and have data**

```bash
PGPASSWORD=stock_pass_2026 psql -h localhost -U stock_user -d stock_db -c "
SELECT
  (SELECT COUNT(*) FROM industries) AS industries,
  (SELECT COUNT(*) FROM tickers) AS tickers,
  (SELECT COUNT(*) FROM us_daily_prices) AS prices;
"
```

Expected: all counts are non-zero (prices should be in the thousands to millions).

---

## Task 4: Create .env File

**Files:**
- Create: `/home/ubuntu/stock-market-dashboard/.env`

- [ ] **Step 1: Write the .env file**

```bash
cat > /home/ubuntu/stock-market-dashboard/.env << 'EOF'
DB_HOST=localhost
DB_PORT=5432
DB_NAME=stock_db
DB_USER=stock_user
DB_PASSWORD=stock_pass_2026
ALLOWED_ORIGINS=*
EOF
```

- [ ] **Step 2: Verify contents**

```bash
cat /home/ubuntu/stock-market-dashboard/.env
```

Expected: shows all 6 variables with correct values.

---

## Task 5: Set Up Python Virtualenv

**Files:**
- Create: `/home/ubuntu/stock-market-dashboard/venv/`

- [ ] **Step 1: Create virtualenv**

```bash
cd /home/ubuntu/stock-market-dashboard
python3 -m venv venv
```

- [ ] **Step 2: Install dependencies**

```bash
cd /home/ubuntu/stock-market-dashboard
venv/bin/pip install --upgrade pip
venv/bin/pip install -r requirements.txt
```

Expected: all packages install without error. `psycopg2-binary` and `gunicorn` must be present.

- [ ] **Step 3: Verify FastAPI can import**

```bash
cd /home/ubuntu/stock-market-dashboard
venv/bin/python -c "from api import app; print('OK')"
```

Expected: prints `OK` (may print DB init logs too — that's fine as long as no exception).

---

## Task 6: Smoke-Test the API

**Files:** none

- [ ] **Step 1: Start Gunicorn in the foreground**

```bash
cd /home/ubuntu/stock-market-dashboard
venv/bin/gunicorn -k uvicorn.workers.UvicornWorker api:app --bind 0.0.0.0:8000 &
sleep 3
```

- [ ] **Step 2: Hit the health endpoint**

```bash
curl -s http://localhost:8000/health
```

Expected: `{"status":"healthy"}`

- [ ] **Step 3: Hit the industries endpoint**

```bash
curl -s http://localhost:8000/api/industries | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'{len(d[\"industries\"])} industries')"
```

Expected: `N industries` where N > 0.

- [ ] **Step 4: Stop the test Gunicorn**

```bash
kill %1 2>/dev/null; sleep 1
```

---

## Task 7: Create Systemd Service

**Files:**
- Create: `/etc/systemd/system/stock-api.service`

- [ ] **Step 1: Write the service file**

```bash
sudo tee /etc/systemd/system/stock-api.service > /dev/null << 'EOF'
[Unit]
Description=Stock Market Dashboard API
After=network.target postgresql.service
Requires=postgresql.service

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/stock-market-dashboard
EnvironmentFile=/home/ubuntu/stock-market-dashboard/.env
ExecStart=/home/ubuntu/stock-market-dashboard/venv/bin/gunicorn \
    -k uvicorn.workers.UvicornWorker \
    api:app \
    --bind 0.0.0.0:8000 \
    --workers 2 \
    --timeout 120
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF
```

- [ ] **Step 2: Reload systemd and enable service**

```bash
sudo systemctl daemon-reload
sudo systemctl enable stock-api
sudo systemctl start stock-api
```

- [ ] **Step 3: Verify service is running**

```bash
sudo systemctl status stock-api
```

Expected: `active (running)`. If not, run `sudo journalctl -u stock-api -n 50` to see errors.

- [ ] **Step 4: Verify API still responds**

```bash
curl -s http://localhost:8000/health
```

Expected: `{"status":"healthy"}`

---

## Task 8: Update config.js for Any-Host Access

**Files:**
- Modify: `/home/ubuntu/stock-market-dashboard/config.js`

The current `config.js` only routes to local API when hostname is `localhost` or `127.0.0.1`. When accessed via the server's IP, it falls back to the old Render backend. Fix by using a relative URL (empty string) so all API calls go to the same host — Nginx will proxy them.

- [ ] **Step 1: Update config.js**

Replace the entire file content:

```bash
cat > /home/ubuntu/stock-market-dashboard/config.js << 'EOF'
// ============================================
// Frontend Configuration
// ============================================

// Empty string = relative URL, so API calls go to same host/port.
// Nginx proxies /api/* to the local Gunicorn backend.
const API_BASE_URL = "";
EOF
```

- [ ] **Step 2: Commit the change**

```bash
cd /home/ubuntu/stock-market-dashboard
git add config.js
git commit -m "fix: use relative API URL so frontend works from any host"
```

---

## Task 9: Install and Configure Nginx

**Files:**
- Create: `/etc/nginx/sites-available/stock-dashboard`
- Symlink: `/etc/nginx/sites-enabled/stock-dashboard`

- [ ] **Step 1: Install Nginx**

```bash
sudo apt-get install -y nginx
```

Expected: installs and starts automatically.

- [ ] **Step 2: Write Nginx site config**

```bash
sudo tee /etc/nginx/sites-available/stock-dashboard > /dev/null << 'EOF'
server {
    listen 80 default_server;
    listen [::]:80 default_server;

    root /home/ubuntu/stock-market-dashboard;
    index home.html;

    # Serve static HTML files
    location / {
        try_files $uri $uri/ /home.html;
    }

    # Proxy API calls to Gunicorn
    location /api/ {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_read_timeout 120s;
    }

    # Proxy FastAPI root and health check
    location ~ ^/(health|docs|openapi.json)$ {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
    }
}
EOF
```

- [ ] **Step 3: Enable the site and disable default**

```bash
sudo ln -sf /etc/nginx/sites-available/stock-dashboard /etc/nginx/sites-enabled/stock-dashboard
sudo rm -f /etc/nginx/sites-enabled/default
```

- [ ] **Step 4: Set correct permissions so Nginx can read files**

```bash
sudo chmod o+x /home/ubuntu
sudo chmod o+x /home/ubuntu/stock-market-dashboard
```

- [ ] **Step 5: Test and reload Nginx**

```bash
sudo nginx -t && sudo systemctl reload nginx
```

Expected: `nginx: configuration file /etc/nginx/nginx.conf test is successful`

---

## Task 10: End-to-End Verification

**Files:** none

- [ ] **Step 1: Check both services are up**

```bash
sudo systemctl is-active stock-api nginx
```

Expected: two lines, both `active`.

- [ ] **Step 2: Verify Nginx serves the frontend**

```bash
curl -s http://localhost/ | grep -o "<title>.*</title>"
```

Expected: the page title tag from `home.html`.

- [ ] **Step 3: Verify Nginx proxies the API**

```bash
curl -s http://localhost/api/industries | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'{len(d[\"industries\"])} industries loaded')"
```

Expected: `N industries loaded` where N > 0.

- [ ] **Step 4: Get the server's public IP and test from outside**

```bash
curl -s ifconfig.me
```

Open `http://<that-ip>/` in your browser. The dashboard should load with real industry/stock data.

- [ ] **Step 5: Final commit**

```bash
cd /home/ubuntu/stock-market-dashboard
git add docs/
git commit -m "docs: add local deployment implementation plan"
```
