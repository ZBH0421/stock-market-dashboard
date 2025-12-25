import psycopg2
from psycopg2 import pool, extras
import pandas as pd
from typing import List, Tuple, Optional
import os

class DatabaseManager:
    def __init__(self, db_config: dict):
        """
        Initialize the connection pool.
        db_config: dict containing 'dbname', 'user', 'password', 'host', 'port'
        """
        self.connection_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            **db_config
        )

    def get_connection(self):
        return self.connection_pool.getconn()

    def release_connection(self, conn):
        self.connection_pool.putconn(conn)

    def close_all_connections(self):
        self.connection_pool.closeall()

    def get_or_create_industry(self, industry_name: str) -> int:
        """
        Get existing industry ID or create a new one.
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                # Try to get existing ID
                cur.execute("SELECT id FROM industries WHERE name = %s", (industry_name,))
                result = cur.fetchone()
                if result:
                    return result[0]
                
                # Insert new industry
                cur.execute(
                    "INSERT INTO industries (name) VALUES (%s) RETURNING id",
                    (industry_name,)
                )
                conn.commit()
                return cur.fetchone()[0]
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            self.release_connection(conn)

    def upsert_tickers(self, tickers_data: List[Tuple[str, int]]):
        """
        Batch upsert tickers.
        tickers_data: List of (ticker, industry_id) tuples.
        """
        conn = self.get_connection()
        query = """
            INSERT INTO tickers (ticker, industry_id)
            VALUES %s
            ON CONFLICT (ticker) 
            DO UPDATE SET industry_id = EXCLUDED.industry_id;
        """
        try:
            with conn.cursor() as cur:
                extras.execute_values(cur, query, tickers_data)
            conn.commit()
            print(f"✅ Upserted {len(tickers_data)} tickers.")
        except Exception as e:
            conn.rollback()
            print(f"❌ Error upserting tickers: {e}")
            raise e
        finally:
            self.release_connection(conn)

    def upsert_prices(self, df: pd.DataFrame):
        """
        Batch upsert daily prices.
        df: DataFrame containing ['Date', 'Ticker', 'Close']
        """
        if df.empty:
            return

        conn = self.get_connection()
        
        # Prepare data for insertion (list of tuples)
        # Ensure 'Date' is formatted efficiently
        data_tuples = list(df[['Ticker', 'Date', 'Close']].itertuples(index=False, name=None))
        
        query = """
            INSERT INTO daily_prices (ticker, market_date, close_price)
            VALUES %s
            ON CONFLICT (ticker, market_date)
            DO UPDATE SET close_price = EXCLUDED.close_price;
        """
        
        try:
            with conn.cursor() as cur:
                # execute_values is highly optimized for batch inserts
                extras.execute_values(
                    cur, 
                    query, 
                    data_tuples,
                    page_size=1000
                )
            conn.commit()
            print(f"✅ Upserted {len(data_tuples)} price records.")
        except Exception as e:
            conn.rollback()
            print(f"❌ Error upserting prices: {e}")
            raise e
        finally:
            self.release_connection(conn)

# --- Example Usage ---
if __name__ == "__main__":
    # Configuration - Replace with your actual credentials
    DB_CONFIG = {
        "dbname": "stock_db",
        "user": "postgres",
        "password": "password",
        "host": "localhost",
        "port": "5432"
    }

    # Dummy Data for testing logic
    try:
        db = DatabaseManager(DB_CONFIG)
        
        # 1. Setup Industry
        print("Setting up industry...")
        ind_id = db.get_or_create_industry("Semiconductors")
        
        # 2. Setup Tickers
        print("Setting up tickers...")
        tickers = [('NVDA', ind_id), ('AMD', ind_id), ('TSM', ind_id)]
        db.upsert_tickers(tickers)
        
        # 3. Setup Prices (Batch Upsert)
        print("Upserting prices...")
        data = {
            'Date': ['2023-10-01', '2023-10-01', '2023-10-01', '2023-10-02'],
            'Ticker': ['NVDA', 'AMD', 'TSM', 'NVDA'],
            'Close': [450.05, 102.50, 89.00, 455.20]
        }
        df_prices = pd.DataFrame(data)
        db.upsert_prices(df_prices)
        
        print("Done!")
        
    except Exception as e:
        print(f"Setup skipped or failed (expected if DB not running): {e}")
