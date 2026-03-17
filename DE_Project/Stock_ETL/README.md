# Stock Price ETL Pipeline

A simple ETL pipeline that pulls stock price data from Yahoo Finance, cleans it, adds basic indicators, and stores it in a local DuckDB database.

Built as a beginner data engineering project to practise pipeline structure, data cleaning, and analytical storage.

---

## What it does

```
Extract                  Transform                  Load
──────────────────────   ──────────────────────     ─────────────────
yfinance (Yahoo Finance) Drop nulls & duplicates → DuckDB (stocks.duckdb)
AAPL, MSFT, NVDA         Fix data types
90 days of OHLCV prices  Add daily return %
                         Add 20-day moving average
```

---

## Project structure

```
fin-pipeline/
├── main.py              # Runs the pipeline (start here)
├── config.py            # Tickers and settings
├── requirements.txt
├── pipeline/
│   ├── extract.py       # Pull data from Yahoo Finance
│   ├── transform.py     # Clean + calculate indicators
│   └── load.py          # Save to DuckDB
└── outputs/             # Created on first run
    ├── stocks.duckdb    # The database
    └── pipeline.log     # Run history
```