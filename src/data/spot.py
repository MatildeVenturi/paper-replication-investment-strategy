from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _flatten_yf_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    yfinance can return MultiIndex columns when tickers are involved.
    This flattens them to plain strings.
    """
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join([str(x) for x in col if x is not None]).strip() for col in df.columns.values]
    else:
        df.columns = [str(c) for c in df.columns]
    return df


def _find_close_column(df: pd.DataFrame) -> str:
    """
    Prefer 'Close', otherwise something like 'Close_BTC-USD'.
    """
    if "Close" in df.columns:
        return "Close"
    close_cols = [c for c in df.columns if c.startswith("Close")]
    if not close_cols:
        raise ValueError(f"Could not find a Close column. Columns: {list(df.columns)}")
    # take the first close-like column
    return close_cols[0]


def build_spot_csv_yahoo(
    ticker: str,
    underlying: str,
    days_back: int = 30,
    out_rel_path: str = "data/raw/spot.csv",
) -> Path:
    """
    Downloads hourly BTC/ETH spot from Yahoo and takes the row at 08:00 UTC for each day.
    Saves CSV: date, underlying, spot (last 30 days by default).
    """

    root = _project_root()
    out_path = root / out_rel_path
    out_path.parent.mkdir(parents=True, exist_ok=True)

    end_day = datetime.now(timezone.utc).date()
    start_day = end_day - timedelta(days=days_back)

    # Request slightly more than needed (end boundary safety)
    start_dt = datetime.combine(start_day, datetime.min.time(), tzinfo=timezone.utc)
    end_dt = datetime.combine(end_day + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)

    df = yf.download(
        tickers=ticker,
        interval="1h",
        start=start_dt,
        end=end_dt,
        progress=False,
    )

    if df is None or df.empty:
        raise ValueError(f"No data downloaded for {ticker}. Try again or reduce date range.")

    df = _flatten_yf_columns(df)
    close_col = _find_close_column(df)

    df = df.reset_index()

    # Determine time column
    time_col = "Datetime" if "Datetime" in df.columns else ("Date" if "Date" in df.columns else None)
    if time_col is None:
        raise ValueError(f"Could not find time column. Columns: {list(df.columns)}")

    df[time_col] = pd.to_datetime(df[time_col], utc=True)
    df["date"] = df[time_col].dt.date
    df["hour"] = df[time_col].dt.hour

    df_0800 = df[df["hour"] == 8].copy()
    if df_0800.empty:
        raise ValueError("No 08:00 UTC rows found. Try interval='1d' as fallback.")

    # Build output safely (all series must be 1D)
    out = pd.DataFrame()
    out["date"] = df_0800["date"].astype(str).values
    out["underlying"] = underlying.upper()
    out["spot"] = df_0800[close_col].astype(float).values
    out["spot"] = out["spot"].round(2)

    # Keep only the last N days (in case extra got included)
    out = out[(pd.to_datetime(out["date"]).dt.date >= start_day) &
              (pd.to_datetime(out["date"]).dt.date <= end_day)].copy()

    out.to_csv(out_path, index=False)
    print(f"Saved {len(out)} rows -> {out_path}")
    return out_path


if __name__ == "__main__":
    build_spot_csv_yahoo(
        ticker="BTC-USD",
        underlying="BTC",
        days_back=30,
        out_rel_path="data/raw/spot.csv",
    )
