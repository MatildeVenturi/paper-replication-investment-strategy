# src/backtest/backtest.py
from __future__ import annotations

import pandas as pd

from src.arbitrage.payoffs import (
    payoff_long_call_binary_put,
    payoff_long_put_binary_call,
)


def _spot_map(spot_df: pd.DataFrame) -> dict[tuple[str, str], float]:
    s = spot_df.copy()
    s["date"] = s["date"].astype(str).str.strip()
    s["underlying"] = s["underlying"].astype(str).str.upper().str.strip()
    return s.set_index(["date", "underlying"])["spot"].astype(float).to_dict()


def _binary_outcome(binary_type: str, S_T: float, Kb: float) -> int:
    """
    Proxy outcome (since you don't have Polymarket resolved outcomes):
      - call: E=1 if S_T >= Kb else 0
      - put:  E=1 if S_T <  Kb else 0
    """
    b = str(binary_type).lower().strip()
    if b == "call":
        return 1 if S_T >= Kb else 0
    if b == "put":
        return 1 if S_T < Kb else 0
    raise ValueError(f"Unknown binary_type: {binary_type}")


def backtest_hold_to_expiry(
    opp_df: pd.DataFrame,
    spot_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Backtest "hold to expiry" for candidates in opportunities.csv.

    Requires:
      opp_df columns (from scanner):
        date, underlying, expiry, spot,
        binary_type, Kb, Pb,
        vanilla_type, Kv, Pv_usd,
        Qv, Qb, fee_usd, edge, ...

      spot_df:
        date, underlying, spot

    Since you don't have Polymarket resolved outcomes, we use a proxy:
      E determined by S_T and Kb.
    S_T is taken as spot at 'expiry' date (08:00 UTC), which must exist in spot_df.
    """
    opp = opp_df.copy()
    opp["date"] = opp["date"].astype(str).str.strip()
    opp["expiry"] = opp["expiry"].astype(str).str.strip()
    opp["underlying"] = opp["underlying"].astype(str).str.upper().str.strip()

    smap = _spot_map(spot_df)

    rows: list[dict] = []
    cum = 0.0

    for _, r in opp.iterrows():
        date = str(r["date"])
        expiry = str(r["expiry"])
        u = str(r["underlying"])

        # settlement proxy
        S_T = smap.get((expiry, u))
        if S_T is None:
            # can't evaluate trade; skip
            continue
        S_T = float(S_T)

        binary_type = str(r["binary_type"]).lower().strip()
        vanilla_type = str(r["vanilla_type"]).lower().strip()

        Kb = float(r["Kb"])
        Pb = float(r["Pb"])

        Kv = float(r["Kv"])
        Pv_usd = float(r["Pv_usd"])

        Qv = float(r.get("Qv", 1.0))
        Qb = float(r.get("Qb", 0.0))

        E = _binary_outcome(binary_type=binary_type, S_T=S_T, Kb=Kb)

        if vanilla_type == "call" and binary_type == "put":
            pnl = payoff_long_call_binary_put(
                S_T=S_T,
                K_v=Kv,
                P_v_usd=Pv_usd,
                Q_v=Qv,
                P_b=Pb,
                Q_b=Qb,
                E=E,
            )
        elif vanilla_type == "put" and binary_type == "call":
            pnl = payoff_long_put_binary_call(
                S_T=S_T,
                K_v=Kv,
                P_v_usd=Pv_usd,
                Q_v=Qv,
                P_b=Pb,
                Q_b=Qb,
                E=E,
            )
        else:
            # unexpected combo; skip
            continue

        cum += float(pnl)

        rows.append(
            {
                "date": date,
                "underlying": u,
                "expiry": expiry,
                "spot_t": float(r["spot"]),
                "S_T": S_T,
                "binary_type": binary_type,
                "Kb": Kb,
                "Pb": Pb,
                "E": E,
                "vanilla_type": vanilla_type,
                "Kv": Kv,
                "Pv_usd": Pv_usd,
                "Qv": Qv,
                "Qb": Qb,
                "pnl": float(pnl),
                "cum_pnl": float(cum),
                "edge": float(r.get("edge", float("nan"))),
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    out = out.sort_values(["date", "underlying", "expiry"]).reset_index(drop=True)
    return out
