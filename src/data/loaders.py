# src/data/loaders.py
from __future__ import annotations

from pathlib import Path
import pandas as pd


# -------------------------
# Internals
# -------------------------
def _read(path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(path)

    # normalize keys to strings (scanner expects str keys)
    for c in ["date", "underlying", "expiry", "type"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    if "underlying" in df.columns:
        df["underlying"] = df["underlying"].astype(str).str.upper().str.strip()

    return df


def _require(df: pd.DataFrame, req: set[str], name: str) -> None:
    miss = req - set(df.columns)
    if miss:
        raise ValueError(f"{name} missing {miss}")


# -------------------------
# Public loaders
# -------------------------
def load_spot(path: str | Path) -> pd.DataFrame:
    """
    spot.csv columns:
      - date (str, YYYY-MM-DD)
      - underlying (BTC/ETH)
      - spot (float, USD per 1 underlying)

    Paper uses 08:00 UTC daily observation and VWAP around that time. :contentReference[oaicite:7]{index=7}
    """
    df = _read(path)
    _require(df, {"date", "underlying", "spot"}, "spot.csv")
    df["spot"] = df["spot"].astype(float)
    return df


def load_binary(path: str | Path) -> pd.DataFrame:
    """
    binary.csv columns:
      - date (str)
      - underlying (str)
      - expiry (str)
      - strike (float, USD)
      - price (float, USD in [0,1] typically)

    Paper notes prediction market binary options are settled/priced in dollars. :contentReference[oaicite:8]{index=8}
    """
    df = _read(path)
    _require(df, {"date", "underlying", "expiry", "strike", "price"}, "binary.csv")
    df["strike"] = df["strike"].astype(float)
    df["price"] = df["price"].astype(float)
    return df


def load_vanilla(path: str | Path) -> pd.DataFrame:
    """
    vanilla.csv columns:
      Required:
        - date (str)
        - underlying (str)
        - expiry (str)
        - strike (float, USD)
        - type (str: call/put)
      Price columns (at least one required):
        - price          (float) premium in underlying units (BTC/ETH), as commonly quoted on Deribit
        - price_usd      (float) premium in USD (if you already converted)

    If only 'price' is present, you can convert to USD later using spot:
      price_usd = price * spot

    (The paper evaluates payoffs in USD for consistency with binary options. :contentReference[oaicite:9]{index=9})
    """
    df = _read(path)
    _require(df, {"date", "underlying", "expiry", "strike", "type"}, "vanilla.csv")

    if "price" not in df.columns and "price_usd" not in df.columns:
        raise ValueError("vanilla.csv must have at least one of: 'price' (underlying) or 'price_usd' (USD)")

    df["strike"] = df["strike"].astype(float)

    if "price" in df.columns:
        df["price"] = df["price"].astype(float)

    if "price_usd" in df.columns:
        df["price_usd"] = df["price_usd"].astype(float)

    df["type"] = df["type"].astype(str).str.lower().str.strip()
    allowed = {"call", "put"}
    bad = set(df["type"].unique()) - allowed
    if bad:
        raise ValueError(f"vanilla.csv invalid option types {bad}. Allowed: {sorted(allowed)}")

    return df
