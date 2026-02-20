# src/arbitrage/scanner.py
from __future__ import annotations

from datetime import datetime
from typing import Optional

import pandas as pd

from src.arbitrage.strategy import check_and_build_candidate


def _parse_iso_date(d: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(str(d))
    except Exception:
        return None


def scan_opportunities(
    spot_df: pd.DataFrame,
    binary_df: pd.DataFrame,
    vanilla_df: pd.DataFrame,
    *,
    Qv: float = 1.0,
    fee_usd: float = 0.0,
    min_edge: float = 0.0,
    # -------------------------
    # NEW knobs
    # -------------------------
    edge_epsilon: float = 100.0,     # match strategy slack
    pb_clip: float = 0.02,           # drop Pb extremes
    nearest_expiry_days: int = 2,    # allow +/- N days when exact expiry match is missing
) -> pd.DataFrame:
    """
    Scan for (relaxed) arbitrage / near-arbitrage candidates.

    Changes vs your original:
    - if exact expiry match is missing, uses nearest vanilla expiry within +/- nearest_expiry_days
    - passes edge_epsilon and pb_clip into strategy to be less strict
    """

    # Copies
    spot = spot_df.copy()
    binary = binary_df.copy()
    vanilla = vanilla_df.copy()

    # Normalize keys
    for df in (spot, binary, vanilla):
        for c in ("date", "underlying"):
            if c in df.columns:
                df[c] = df[c].astype(str).str.strip()
        if "underlying" in df.columns:
            df["underlying"] = df["underlying"].str.upper()

    for df in (binary, vanilla):
        if "expiry" in df.columns:
            df["expiry"] = df["expiry"].astype(str).str.strip()

    if "type" in vanilla.columns:
        vanilla["type"] = vanilla["type"].astype(str).str.lower().str.strip()

    # Spot map
    spot_map = spot.set_index(["date", "underlying"])["spot"].astype(float).to_dict()

    rows: list[dict] = []

    # Iterate binaries
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

        # Determine required vanilla type (same as your original rule)
        required_vanilla_type = "call" if Kb < spot_val else "put"

        # Exact expiry slice
        v_slice = vanilla[
            (vanilla["date"] == date)
            & (vanilla["underlying"] == underlying)
            & (vanilla["expiry"] == expiry)
            & (vanilla["type"] == required_vanilla_type)
        ]

        # If empty, try nearest expiry within +/- nearest_expiry_days
        if v_slice.empty and nearest_expiry_days > 0:
            base = vanilla[
                (vanilla["date"] == date)
                & (vanilla["underlying"] == underlying)
                & (vanilla["type"] == required_vanilla_type)
            ].copy()

            if not base.empty:
                b_exp = _parse_iso_date(expiry)
                if b_exp is not None:
                    def _abs_days(x) -> int:
                        xdt = _parse_iso_date(x)
                        if xdt is None:
                            return 10**9
                        return abs((xdt.date() - b_exp.date()).days)

                    base["__abs_days"] = base["expiry"].apply(_abs_days)
                    base = base[base["__abs_days"] <= nearest_expiry_days]
                    if not base.empty:
                        # Use the closest expiry group (smallest abs_days)
                        min_d = int(base["__abs_days"].min())
                        v_slice = base[base["__abs_days"] == min_d]

        if v_slice.empty:
            continue

        # Evaluate all candidate vanilla rows in the slice
        for _, v in v_slice.iterrows():
            Kv = float(v["strike"])
            vtype = str(v["type"])

            # price conversion
            if "price_usd" in v and pd.notna(v["price_usd"]):
                Pv_usd = float(v["price_usd"])
            elif "price" in v and pd.notna(v["price"]):
                Pv_underlying = float(v["price"])
                Pv_usd = Pv_underlying * spot_val
            else:
                continue

            cand = check_and_build_candidate(
                date=date,
                underlying=underlying,
                expiry=str(v.get("expiry", expiry)),  # keep the vanilla expiry actually used
                spot=spot_val,
                Kb=Kb,
                Pb=Pb,
                Kv=Kv,
                vanilla_type=vtype,
                Pv_usd=Pv_usd,
                Qv=Qv,
                fee_usd=fee_usd,
                edge_epsilon=edge_epsilon,
                pb_clip=pb_clip,
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