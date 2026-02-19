#find combinations as in the paper 
from __future__ import annotations

import pandas as pd

from src.arbitrage.strategy import check_and_build_candidate


def scan_opportunities(
    spot_df: pd.DataFrame,
    binary_df: pd.DataFrame,
    vanilla_df: pd.DataFrame,
    *, 
    Qv: float = 1.0, #vanilla qt to buy
    fee_usd: float = 0.0,
    min_edge: float = 0.0,
) -> pd.DataFrame:
   
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
                vanilla_type=vtype,   
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
