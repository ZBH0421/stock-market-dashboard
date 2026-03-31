# Stock Market Dashboard — Local Deployment Design

**Date:** 2026-03-31  
**Status:** Approved

## Overview

Deploy the stock-market-dashboard project on a local Ubuntu server with PostgreSQL 17, FastAPI (Gunicorn), and Nginx. Migrate the existing Supabase PostgreSQL database to the local instance.

## Architecture

```
[Browser] → [Nginx :80] → [Gunicorn/FastAPI :8000]
                                    ↓
                         [PostgreSQL 17 local]
```

## Components

### 1. PostgreSQL 17 Server
- Install: `postgresql-17` (server package, already have client)
- Database: `stock_db`
- User: `stock_user` with password
- Restore from: `/home/ubuntu/supabase_backup.dump` (40MB pg_dump custom format)
- The dump contains tables: `industries`, `tickers`, `us_daily_prices`

### 2. Python Environment
- Virtualenv at: `/home/ubuntu/stock-market-dashboard/venv`
- Install from: `requirements.txt`
- Packages: fastapi, uvicorn, gunicorn, sqlalchemy, psycopg2-binary, pandas, yfinance, etc.

### 3. Environment Configuration
File: `/home/ubuntu/stock-market-dashboard/.env`
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=stock_db
DB_USER=stock_user
DB_PASSWORD=<generated>
ALLOWED_ORIGINS=*
```

### 4. Gunicorn systemd Service
- Service file: `/etc/systemd/system/stock-api.service`
- Runs as: `ubuntu` user
- Command: `gunicorn -k uvicorn.workers.UvicornWorker api:app --bind 0.0.0.0:8000`
- Working directory: `/home/ubuntu/stock-market-dashboard`
- Auto-restart on failure, starts on boot

### 5. Nginx
- Install: `nginx`
- Serves `home.html` and `stock.html` as static files on port 80
- Proxies `/api/*` requests to `http://localhost:8000`
- Config: `/etc/nginx/sites-available/stock-dashboard`

## Data Flow

1. Browser requests `http://<server-ip>/` → Nginx serves `home.html`
2. Browser requests `http://<server-ip>/stock.html` → Nginx serves `stock.html`
3. JS in HTML calls `http://<server-ip>/api/...` → Nginx proxies to Gunicorn → FastAPI queries PostgreSQL

## Migration Steps (high level)

1. Install PostgreSQL 17 server
2. Create `stock_db` database and `stock_user`
3. Restore `supabase_backup.dump` with `pg_restore`
4. Set up Python venv and install dependencies
5. Write `.env` with local DB credentials
6. Create and enable `stock-api.service`
7. Install and configure Nginx
8. Verify end-to-end: API health check and frontend load

## Out of Scope

- HTTPS/TLS (can be added later with Let's Encrypt)
- GitHub Actions daily update (uses Supabase credentials — needs separate update after migration)
- Authentication layer
