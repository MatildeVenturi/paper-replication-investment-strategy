# src/data/loaders.py
from __future__ import annotations

from pathlib import Path
import pandas as pd


def _read(path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    # normalize to strings (scanner expects str keys)
    for c in ["date", "underlying", "expiry", "type"]:
        if c in df.columns:
            df[c] = df[c].astype(str)
    return df


def load_spot(path: str | Path) -> pd.DataFrame:
    df = _read(path)
    req = {"date", "underlying", "spot"}
    miss = req - set(df.columns)
    if miss:
        raise ValueError(f"spot.csv missing {miss}")
    df["spot"] = df["spot"].astype(float)
    return df


def load_binary(path: str | Path) -> pd.DataFrame:
    df = _read(path)
    req = {"date", "underlying", "expiry", "strike", "price"}
    miss = req - set(df.columns)
    if miss:
        raise ValueError(f"binary.csv missing {miss}")
    df["strike"] = df["strike"].astype(float)
    df["price"] = df["price"].astype(float)
    return df


def load_vanilla(path: str | Path) -> pd.DataFrame:
    df = _read(path)
    req = {"date", "underlying", "expiry", "strike", "type"}
    if "price_usd" not in df.columns and "price" not in df.columns:
        raise ValueError("vanilla.csv must have 'price_usd' or 'price' column")

    miss = req - set(df.columns)
    if miss:
        raise ValueError(f"vanilla.csv missing {miss}")

    df["strike"] = df["strike"].astype(float)
    if "price_usd" in df.columns:
        df["price_usd"] = df["price_usd"].astype(float)
    if "price" in df.columns:
        df["price"] = df["price"].astype(float)

    df["type"] = df["type"].str.lower().str.strip()
    return df
