"""
Transform Summary
1. cleans raw data 
2. adds daily return% and 20 days MA

Cleaning steps:
  1. Keep neccessary column
  2. Drop rows with missing prices
  3. Remove duplicate (date, ticker) rows
  4. Ensure correct data types

New columns:
  daily_return = how much the price changed % vs previous day
  ma_20        = 20-day moving average of the closing price
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)

KEEP_COLUMNS = ["date", "ticker", "open", "high", "low", "close", "volume"]


def clean_and_transform(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transforming data...")

    df = df.copy()

    # Keep neccessary column
    available = [c for c in KEEP_COLUMNS if c in df.columns]
    df = df[available]

    # Fix data types
    df["date"]   = pd.to_datetime(df["date"])
    df["close"]  = pd.to_numeric(df["close"],  errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

    # Drop rows with no closing price
    before = len(df)
    df = df.dropna(subset=["close"])
    dropped = before - len(df)
    if dropped > 0:
        logger.warning(f"Dropped {dropped} rows with missing close price")

    # Remove duplicate rows
    dupes = df.duplicated(subset=["date", "ticker"]).sum()
    if dupes > 0:
        logger.warning(f"Removed {dupes} duplicate rows")
        df = df.drop_duplicates(subset=["date", "ticker"])

    # Sort
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    # Daily return % (per ticker)
    df["daily_return"] = (
        df.groupby("ticker")["close"]
          .pct_change()          # (today - yesterday) / yesterday
          .round(4)
    )

    # 20-day moving average (per ticker)
    df["ma_20"] = (
        df.groupby("ticker")["close"]
          .transform(lambda x: x.rolling(window=20, min_periods=1).mean())
          .round(2)
    )

    # Add a timestamp
    df["loaded_at"] = pd.Timestamp.now()

    logger.info(f"  ✓  {len(df)} rows ready | columns: {list(df.columns)}")
    return df
