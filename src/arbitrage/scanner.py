# src/arbitrage/scanner.py
from __future__ import annotations

import pandas as pd

from src.arbitrage.strategy import check_and_build_candidate


def scan_opportunities(
    spot_df: pd.DataFrame,
    binary_df: pd.DataFrame,
    vanilla_df: pd.DataFrame,
    *,
    Qv: float = 1.0,
    fee_usd: float = 0.0,
    min_edge: float = 0.0,
) -> pd.DataFrame:
    """
    Scan for arbitrage opportunities using the unified condition.

    Expected input columns:
      spot_df:    date, underlying, spot
      binary_df:  date, underlying, expiry, strike, price
      vanilla_df: date, underlying, expiry, strike, type, price_usd (or price)

    Returns:
      DataFrame of TradeCandidates (one row per candidate), sorted by edge desc.
    """

    # copies + normalization
    spot = spot_df.copy()
    binary = binary_df.copy()
    vanilla = vanilla_df.copy()

    # vanilla: allow either 'price_usd' or 'price'
    if "price_usd" not in vanilla.columns:
        if "price" in vanilla.columns:
            vanilla = vanilla.rename(columns={"price": "price_usd"})
        else:
            raise ValueError("vanilla_df must contain 'price_usd' or 'price' column.")

    # normalize types
    for df in (spot, binary, vanilla):
        if "date" in df.columns:
            df["date"] = df["date"].astype(str)
        if "underlying" in df.columns:
            df["underlying"] = df["underlying"].astype(str)

    if "expiry" in binary.columns:
        binary["expiry"] = binary["expiry"].astype(str)
    if "expiry" in vanilla.columns:
        vanilla["expiry"] = vanilla["expiry"].astype(str)

    vanilla["type"] = vanilla["type"].astype(str).str.lower().str.strip()

    # build spot map for fast lookup
    spot_map = spot.set_index(["date", "underlying"])["spot"].astype(float).to_dict()

    rows = []

    # iterate binaries
    for _, b in binary.iterrows():
        date = str(b["date"])
        underlying = str(b["underlying"])
        expiry = str(b["expiry"])
        Kb = float(b["strike"])
        Pb = float(b["price"])

        s = spot_map.get((date, underlying))
        if s is None:
            continue
        spot_val = float(s)

        # vanilla slice: same date/underlying/expiry only
        v_slice = vanilla[
            (vanilla["date"] == date)
            & (vanilla["underlying"] == underlying)
            & (vanilla["expiry"] == expiry)
        ]
        if v_slice.empty:
            continue

        for _, v in v_slice.iterrows():
            Kv = float(v["strike"])
            vtype = str(v["type"])
            Pv_usd = float(v["price_usd"])

            cand = check_and_build_candidate(
                date=date,
                underlying=underlying,
                expiry=expiry,
                spot=spot_val,
                Kb=Kb,
                Pb=Pb,
                Kv=Kv,
                vanilla_type=vtype,   # "call" or "put"
                Pv_usd=Pv_usd,
                Qv=Qv,
                fee_usd=fee_usd,
            )

            if cand is None:
                continue
            if cand.edge < min_edge:
                continue

            rows.append({
                "date": cand.date,
                "underlying": cand.underlying,
                "expiry": cand.expiry,
                "spot": cand.spot,

                "binary_type": cand.binary_type,
                "Kb": cand.Kb,
                "Pb": cand.Pb,

                "vanilla_type": cand.vanilla_type,
                "Kv": cand.Kv,
                "Pv_usd": cand.Pv_usd,

                "Qv": cand.Qv,
                "Qb": cand.Qb,
                "fee_usd": cand.fee_usd,

                "kv_bound": cand.kv_bound,
                "edge": cand.edge,
            })

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    # best opportunities first
    out = out.sort_values(["date", "underlying", "expiry", "edge"], ascending=[True, True, True, False])
    out = out.reset_index(drop=True)
    return out
