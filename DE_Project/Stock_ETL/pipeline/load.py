"""
Use DuckDB as Database

Table: stock_prices
  Primary key: (date, ticker)
"""

import logging
import duckdb
import pandas as pd
from config import DB_PATH

logger = logging.getLogger(__name__)

# Load data into database
def load_to_duckdb(df: pd.DataFrame):
    conn = duckdb.connect(DB_PATH)

    # Create table 
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_prices (
            date          DATE,
            ticker        VARCHAR,
            open          DOUBLE,
            high          DOUBLE,
            low           DOUBLE,
            close         DOUBLE,
            volume        BIGINT,
            daily_return  DOUBLE,
            ma_20         DOUBLE,
            loaded_at     TIMESTAMP,
            PRIMARY KEY (date, ticker)
        )
    """)

    # Delete rows (safe re-run)
    conn.execute("""
        DELETE FROM stock_prices
        WHERE (date, ticker) IN (SELECT date, ticker FROM df)
    """)

    # Insert new data
    conn.execute("INSERT INTO stock_prices SELECT * FROM df")

    # Confirm row count
    total = conn.execute("SELECT COUNT(*) FROM stock_prices").fetchone()[0]
    logger.info(f"  ✓  Loaded {len(df)} rows → stock_prices (total in DB: {total})")

    conn.close()
