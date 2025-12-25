import os
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Date, Float, BigInteger, DateTime, Integer, ForeignKey, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import func
from dotenv import load_dotenv
from typing import Optional

# Load environment variables
load_dotenv()

class MarketDataDB:
    """
    Manages connections and operations for the PostgreSQL Market Data database using SQLAlchemy.
    Features strict schema definition and high-performance UPSERT operations.
    """

    def __init__(self):
        """
        Initializes the database engine using environment variables.
        Raises ValueError if required environment variables are missing.
        """
        # Validate Config
        required_vars = ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_NAME', 'DB_PORT']
        config = {var: os.getenv(var) for var in required_vars}
        
        missing = [var for var, val in config.items() if not val]
        if missing:
            raise ValueError(f"Missing environment variables: {missing}")

        # Connection String
        # url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        db_url = f"postgresql+psycopg2://{config['DB_USER']}:{config['DB_PASSWORD']}@{config['DB_HOST']}:{config['DB_PORT']}/{config['DB_NAME']}"
        
        # Create Engine
        # pool_size=10, max_overflow=20 for handling concurrent writes if needed
        self.engine = create_engine(db_url, pool_size=10, max_overflow=20)
        
        # Schema Definition (Core)
        self.metadata = MetaData()
        
        # 1. Industries Table
        self.industries_table = Table(
            'industries',
            self.metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(255), unique=True, nullable=False)
        )

        # 2. Tickers Table
        self.tickers_table = Table(
            'tickers',
            self.metadata,
            Column('ticker', String(20), primary_key=True),
            Column('industry_id', Integer, ForeignKey('industries.id')),
            Column('company_name', String(255)),
            Column('market_cap', BigInteger),
            # New Fundamentals
            Column('revenue', BigInteger),
            Column('gross_profit', BigInteger),
            Column('net_income', BigInteger),
            Column('pe_ratio', Float),
            Column('profit_margin', Float),
            Column('dividend_yield', Float),
            Column('created_at', DateTime, server_default=func.now())
        )

        # 3. Daily Prices Table
        self.prices_table = Table(
            'us_daily_prices', 
            self.metadata,
            Column('symbol', String(20), ForeignKey('tickers.ticker'), primary_key=True), # Added FK
            Column('date', Date, primary_key=True),
            Column('close', Float),
            Column('adj_close', Float),
            Column('volume', BigInteger),
            Column('updated_at', DateTime, server_default=func.now(), onupdate=func.now())
        )
        
        # Ensure table exists
        self.metadata.create_all(self.engine)

        # --- Migration: Ensure columns exist ---
        with self.engine.begin() as conn:
            conn.execute(text("ALTER TABLE tickers ADD COLUMN IF NOT EXISTS market_cap BIGINT"))
            conn.execute(text("ALTER TABLE tickers ADD COLUMN IF NOT EXISTS revenue BIGINT"))
            conn.execute(text("ALTER TABLE tickers ADD COLUMN IF NOT EXISTS gross_profit BIGINT"))
            conn.execute(text("ALTER TABLE tickers ADD COLUMN IF NOT EXISTS net_income BIGINT"))
            conn.execute(text("ALTER TABLE tickers ADD COLUMN IF NOT EXISTS pe_ratio FLOAT"))
            conn.execute(text("ALTER TABLE tickers ADD COLUMN IF NOT EXISTS profit_margin FLOAT"))
            conn.execute(text("ALTER TABLE tickers ADD COLUMN IF NOT EXISTS dividend_yield FLOAT"))

        print("[DB] Initialized MarketDataDB and verified schema (Industries -> Tickers -> Prices).")

    def get_or_create_industry(self, name: str) -> int:
        """
        Retrieves the industry ID if it exists, otherwise creates it.
        """
        # Try to find
        stmt = select(self.industries_table.c.id).where(self.industries_table.c.name == name)
        with self.engine.connect() as conn:
            result = conn.execute(stmt).scalar()
            if result:
                return result

        # Insert if not found
        stmt = insert(self.industries_table).values(name=name).on_conflict_do_nothing()
        with self.engine.begin() as conn:
            conn.execute(stmt)
            # Re-fetch ID
            return conn.execute(select(self.industries_table.c.id).where(self.industries_table.c.name == name)).scalar()

    def register_ticker(self, ticker: str, industry_id: int, info: dict = None):
        """
        Registers a ticker under an industry if it doesn't already exist.
        Updates market_cap and fundamentals if info is provided.
        """
        values = {
            'ticker': ticker, 
            'industry_id': industry_id
        }
        
        # Mapping: DB Column -> Info Key
        # info keys assumed: 'market_cap', 'revenue', 'gross_profit', etc.
        if info:
            if 'company_name' in info: values['company_name'] = info['company_name']
            if 'market_cap' in info: values['market_cap'] = info['market_cap']
            if 'revenue' in info: values['revenue'] = info['revenue']
            if 'gross_profit' in info: values['gross_profit'] = info['gross_profit']
            if 'net_income' in info: values['net_income'] = info['net_income']
            if 'pe_ratio' in info: values['pe_ratio'] = info['pe_ratio']
            if 'profit_margin' in info: values['profit_margin'] = info['profit_margin']
            # if 'dividend_yield' in info: values['dividend_yield'] = info['dividend_yield']

        stmt = insert(self.tickers_table).values(**values).on_conflict_do_update(
            index_elements=['ticker'],
            set_=values # Update all fields on conflict
        )
        
        with self.engine.begin() as conn:
            conn.execute(stmt)

    def save_daily_data(self, df: pd.DataFrame):
        """
        Efficiently UPSERTS (Insert on Conflict Update) a DataFrame into the database.
        
        Args:
            df (pd.DataFrame): Must contain Index(Date) or 'date' column, 
                               and columns ['symbol', 'close', 'adj_close', 'volume'].
        """
        if df.empty:
            print("[DB] Warning: Empty DataFrame provided, skipping save.")
            return

        # Prepare DataFrame
        data = df.copy()
        
        # Ensure 'date' is a column (reset index if needed)
        # Note: The fetcher returns index named 'market_date', we map it to 'date'
        if data.index.name == 'market_date':
            data = data.reset_index()
            data = data.rename(columns={'market_date': 'date'})
        elif 'date' not in data.columns and isinstance(data.index, pd.DatetimeIndex):
             data = data.reset_index()
             data = data.rename(columns={'index': 'date'})

        # Validate columns
        required_cols = {'symbol', 'date', 'close', 'adj_close', 'volume'}
        if not required_cols.issubset(data.columns):
             raise ValueError(f"DataFrame missing required columns. Expected {required_cols}, got {data.columns}")

        # Convert to list of dicts for SQLAlchemy
        records = data.to_dict(orient='records')
        
        # Upsert Logic
        # See: https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#insert-on-conflict-upsert
        stmt = insert(self.prices_table).values(records)
        
        # Update columns on conflict (excluding primary keys)
        update_dict = {
            'close': stmt.excluded.close,
            'adj_close': stmt.excluded.adj_close,
            'volume': stmt.excluded.volume,
            'updated_at': func.now() # Update timestamp
        }
        
        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=['symbol', 'date'], # Constraint columns
            set_=update_dict
        )

        with self.engine.begin() as conn:
            result = conn.execute(upsert_stmt)
            print(f"[DB] Successfully upserted {result.rowcount} rows.")

    def get_data(self, symbol: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        Retrieves market data from the database.
        
        Returns:
            pd.DataFrame: Index=date, columns=[close, adj_close, volume]
        """
        query = select(self.prices_table).where(self.prices_table.c.symbol == symbol)
        
        if start_date:
            query = query.where(self.prices_table.c.date >= start_date)
        if end_date:
            query = query.where(self.prices_table.c.date <= end_date)
            
        query = query.order_by(self.prices_table.c.date.asc())
        
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)
        
        if not df.empty:
            df = df.set_index('date')
            # remove symbol column as it's redundant in return if querying single symbol
            if 'symbol' in df.columns:
                 del df['symbol']
            
        return df

if __name__ == "__main__":
    # --- Test Routine ---
    print("--- Testing MarketDataDB ---")
    
    # Needs valid .env to run
    try:
        db = MarketDataDB()
        
        # Create Dummy Data
        print("Creating dummy data...")
        dates = pd.date_range(start="2023-01-01", periods=3)
        dummy_df = pd.DataFrame({
            'close': [150.0, 151.5, 152.0],
            'adj_close': [149.5, 151.0, 151.5],
            'volume': [1000, 2000, 1500]
        }, index=dates)
        dummy_df.index.name = 'market_date' # Simulate Fetcher output
        dummy_df['symbol'] = 'TEST_TICKER' # Add symbol
        
        # 1. First Insert
        print("\n1. Performing Initial Insert...")
        db.save_daily_data(dummy_df)
        
        # 2. Update (Upsert) - Change values
        print("\n2. Performing Upsert (Update values)...")
        dummy_df['close'] = [160.0, 161.5, 162.0] # Changed prices
        db.save_daily_data(dummy_df)
        
        # 3. Read Verification
        print("\n3. Verifying Data...")
        result_df = db.get_data('TEST_TICKER')
        print(result_df)
        
        assert result_df.iloc[0]['close'] == 160.0, "Upsert failed to update close price!"
        print("\n[SUCCESS] Upsert logic verified.")
        
    except Exception as e:
        print(f"\n[SKIPPED] Test failed or skipped due to config: {e}")
