"""
Extract stock price data from Yahoo Finance via yfinance.
Returns a single DataFrame with all tickers combined (long format).
"""

import logging
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from config import TICKERS, LOOKBACK_DAYS

logger = logging.getLogger(__name__)


def extract_stock_data() -> pd.DataFrame:
    
    # Set date range
    end_date   = datetime.today()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)

    logger.info(f"Extracting {TICKERS} from {start_date.date()} to {end_date.date()}")

    # Empty List
    frames = []

    # Run Loop for each TICKERS (MSFT,AAPL,NVDA)
    for ticker in TICKERS:
        raw = yf.download(ticker, start=start_date, end=end_date, progress=False)

        # In case of Empty Response
        if raw.empty:
            logger.warning(f"{ticker}: no data returned -> skipping")
            continue

        # Prevent MultiIndex columns
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = [col[0] for col in raw.columns]

        # Clean 
        df = raw.reset_index() # Stop wrong index
        df.columns = [str(c).lower() for c in df.columns]   # Lowercase all column names
        df["ticker"] = ticker # Add column

        frames.append(df)
        logger.info(f"  ✓  {ticker}: {len(df)} rows")

    if not frames:
        raise RuntimeError("No data extracted. Check your internet connection.")

    return pd.concat(frames, ignore_index=True)
