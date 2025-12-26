# Interactive Market Intelligence Dashboard

A professional-grade, dynamic web application for stock market analysis and industry-specific financial reporting. This project integrates a high-performance **FastAPI** backend with a modern, glassmorphic **HTML5/JS** frontend to provide real-time insights into market trends, performance metrics, and valuation distributions.

## 🚀 Features

### 1. Dynamic Performance Analysis
- **Vectorized Calculations**: Backend uses Pandas vectorization to instantly calculate 1D, 1M, 2M, 3M, 6M, 12M, and YTD performance for hundreds of tickers.
- **Precision Calendar Logic**: Accurate percentage change calculations using robust calendar-based slicing (mirroring professional trading platforms).

### 2. Interactive Visualizations
- **Hover Sparklines**: Premium tooltips featuring high-resolution price trend charts.
- **Dynamic Chart Scaling**: Charts automatically zoom into the relevant price range for the selected timeframe.
- **Market Cap Distribution**: Interactive donut charts with professional unit scaling (Billions of USD).

### 3. Industrial-Strength Backend
- **FastAPI Integration**: Asynchronous, high-throughput API serving industry-specific financial data.
- **PostgreSQL Power**: Efficient SQL queries with optimized lookbacks and history buffers.
- **Incremental ETL**: `daily_update.py` script performs efficient incremental updates, fetching only missing data (T-5 window) with Upsert protection.

## 🛠️ Tech Stack

- **Frontend**: HTML5, Vanilla CSS, jQuery, DataTables, ApexCharts.
- **Backend**: Python 3.9+, FastAPI, SQLAlchemy, Pandas, Uvicorn.
- **Database**: PostgreSQL (Relational Market Data).
- **Design**: Premium Glassmorphic aesthetic with Inter & Noto Sans TC typography.

## 📂 Project Structure

```text
├── home.html               # Main Entry Point (Industry Selection)
├── index.html              # Dynamic Dashboard & Report View
├── api.py                  # Main FastAPI backend
├── daily_update.py         # Automated Daily ETL Script
├── market_data_db.py       # Database Manager & Schema
├── market_data_fetcher.py  # yfinance Data Ingestion
├── config.js               # Frontend API Configuration
└── schema.sql              # Database Schema Definition
```

## ⚙️ Setup & Installation

### 1. Configure Environment
Create a `.env` file in the root directory:
```env
DB_HOST=your_host
DB_NAME=your_db_name
DB_USER=your_user
DB_PASSWORD=your_password
DB_PORT=5432
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Initialize Database
Ensure your PostgreSQL database is running. The `market_data_db.py` module will automatically create tables (`industries`, `tickers`, `us_daily_prices`) on first run.

## 🏃 Usage

### 1. Start the API Server
Start the backend server to serve data to the frontend:
```bash
python api.py
```
*Server runs on http://127.0.0.1:8000*

### 2. Open the Dashboard
Open `home.html` in your browser.
- Select an industry to view the detailed report (`index.html`).
- Use the dropdowns to switch industries or performance timeframes.

### 3. Daily Updates
To keep data fresh, run the update script (CRON recommended):
```bash
python daily_update.py
```

## 🔍 Data Integrity
This project prioritizes accuracy. The backend logic handles:
- **Missing Data**: Graceful handling of null values or suspended tickers.
- **Orphan Checks**: Scripts like `check_orphans.py` ensure synchronization between tickers and price history.
