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
    Paper-aligned scanner.

    Inputs (expected columns):
      spot_df:    date, underlying, spot
      binary_df:  date, underlying, expiry, strike, price        (Polymarket: price in USD in (0,1))
      vanilla_df: date, underlying, expiry, strike, type, price  (Deribit: price often in underlying units)
                 optionally vanilla_df may have price_usd already

    Key paper rules:
      - decision time is 08:00 UTC (your dataset should already be built that way) :contentReference[oaicite:4]{index=4}
      - direction:
          if Kb < spot:   binary is PUT, vanilla must be CALL
          else:           binary is CALL, vanilla must be PUT :contentReference[oaicite:5]{index=5}
      - unify condition uses vanilla premium in USD terms for consistency with binary payoff 

    Returns:
      DataFrame of candidates sorted by edge desc.
    """

    # Copies
    spot = spot_df.copy()
    binary = binary_df.copy()
    vanilla = vanilla_df.copy()

    # Normalize string keys
    for df in (spot, binary, vanilla):
        for c in ("date", "underlying"):
            if c in df.columns:
                df[c] = df[c].astype(str).str.strip()
        if "underlying" in df.columns:
            df["underlying"] = df["underlying"].str.upper()

    for df in (binary, vanilla):
        if "expiry" in df.columns:
            df["expiry"] = df["expiry"].astype(str).str.strip()

    # vanilla type normalization
    if "type" in vanilla.columns:
        vanilla["type"] = vanilla["type"].astype(str).str.lower().str.strip()

    # Build spot map
    spot_map = spot.set_index(["date", "underlying"])["spot"].astype(float).to_dict()

    rows: list[dict] = []

    # Iterate each binary option as reference (paper: binaries define the universe) :contentReference[oaicite:7]{index=7}
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

        # Determine trade direction (paper rule)
        if Kb < spot_val:
            required_vanilla_type = "call"
        else:
            required_vanilla_type = "put"

        # Candidate vanilla slice: same date/underlying/expiry + correct direction
        v_slice = vanilla[
            (vanilla["date"] == date)
            & (vanilla["underlying"] == underlying)
            & (vanilla["expiry"] == expiry)
            & (vanilla["type"] == required_vanilla_type)
        ]
        if v_slice.empty:
            continue

        for _, v in v_slice.iterrows():
            Kv = float(v["strike"])
            vtype = str(v["type"])

            # --- Convert vanilla premium to USD if needed ---
            # Deribit options are often quoted in underlying units; paper converts to USD using spot 
            if "price_usd" in v and pd.notna(v["price_usd"]):
                Pv_usd = float(v["price_usd"])
            elif "price" in v and pd.notna(v["price"]):
                Pv_underlying = float(v["price"])
                Pv_usd = Pv_underlying * spot_val
            else:
                raise ValueError("vanilla_df rows must have 'price_usd' or 'price'.")

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

            rows.append(
                {
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
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    out = out.sort_values(["date", "underlying", "expiry", "edge"], ascending=[True, True, True, False])
    out = out.reset_index(drop=True)
    return out
