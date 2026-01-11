import yfinance as yf
import pandas as pd
from pathlib import Path

RAW__DATA_DIR = Path("data/raw")
RAW__DATA_DIR.mkdir(parents=True, exist_ok=True)

PROCESSED__DATA_DIR = Path("data/processed")
PROCESSED__DATA_DIR.mkdir(parents=True, exist_ok=True)

def download_data(ticker: str, start: str, end: str, interval= "1m"):
    """Download historical stock data from Yahoo Finance."""

    print(f"Downloading data for {ticker} from {start} to {end} with interval {interval}...")
    df = yf.download(ticker, start=start, end=end, interval=interval, progress=False, auto_adjust=False)

    if df.empty:
        raise ValueError(f"No data found for ticker {ticker} in the given date range.")
    
    df.to_csv(RAW__DATA_DIR / f"{ticker}_{start}_{end}_{interval}.csv") 
    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean OHLCV data:
    - Flatten multi-index columns
    - Remove missing bars
    - Sort by timestamp
    - Enforce numeric types
    """
    df = df.copy()

    # Flatten columns if multi-index
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join(col).strip() if isinstance(col, tuple) else col for col in df.columns]

    df = df.dropna()
    df = df.sort_index()

    for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.dropna()
    return df


def main():
    ticker = "AAPL"
    start = "2026-01-05"
    end = "2026-01-11"
    interval = "1m"

    raw_data = download_data(ticker, start, end, interval)
    clean_df = clean_data(raw_data)

    output_path = PROCESSED__DATA_DIR / f"{ticker}_{start}_{end}_{interval}_cleaned.csv"
    clean_df.to_csv(output_path)

    print(f"Saved cleaned data to {output_path}")
    print(clean_df.head())

if __name__ == "__main__":
    main()