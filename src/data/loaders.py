#used in run_scan to convert csv to df 
from __future__ import annotations

from pathlib import Path
import pandas as pd


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



def load_spot(path: str | Path) -> pd.DataFrame:
 
    df = _read(path)
    _require(df, {"date", "underlying", "spot"}, "spot.csv")
    df["spot"] = df["spot"].astype(float)
    return df


def load_binary(path: str | Path) -> pd.DataFrame:
    df = _read(path)
    _require(df, {"date", "underlying", "expiry", "strike", "price"}, "binary.csv")
    df["strike"] = df["strike"].astype(float)
    df["price"] = df["price"].astype(float)
    return df


def load_vanilla(path: str | Path) -> pd.DataFrame:
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
