# src/arbitrage/conditions.py
from __future__ import annotations


def binary_qty_to_cover_vanilla(Qv: float, Pv_usd: float, fee_usd: float, Pb: float) -> float:
    """
    Paper (Section 3.2): quantity of binary options required to cover the vanilla premium
    when vanilla expires OTM (binary ITM).
        Q_B = Q_V * (P_V + F) / (1 - P_B)

    Pv_usd: vanilla premium in USD (paper converts inverse premium to USD for consistency)
    Pb: binary price in USD in (0,1) interpreted as probability / share price
    fee_usd: total fees for forming the portfolio (USD)
    """
    if not (0.0 < Pb < 1.0):
        raise ValueError("Pb must be in (0,1).")
    if Qv <= 0:
        raise ValueError("Qv must be > 0.")
    if Pv_usd < 0:
        raise ValueError("Pv_usd must be >= 0.")
    if fee_usd < 0:
        raise ValueError("fee_usd must be >= 0.")

    return Qv * (Pv_usd + fee_usd) / (1.0 - Pb)


def kv_bound_for_call_case(Kb: float, Qv: float, Pv_usd: float, Pb: float) -> float:
    """
    Paper unified condition (Section 3.2), for KB < S_t (binary put + vanilla call):
        K_V <= K_B - (Q_V * P_V) / (1 - P_B)

    This returns the upper bound on K_V.
    """
    if not (0.0 < Pb < 1.0):
        raise ValueError("Pb must be in (0,1).")
    if Qv <= 0:
        raise ValueError("Qv must be > 0.")
    return Kb - (Qv * Pv_usd) / (1.0 - Pb)


def kv_bound_for_put_case(Kb: float, Qv: float, Pv_usd: float, Pb: float) -> float:
    """
    Paper unified condition (Section 3.2), otherwise (binary call + vanilla put):
        K_V >= K_B + (Q_V * P_V) / (1 - P_B)

    This returns the lower bound on K_V.
    """
    if not (0.0 < Pb < 1.0):
        raise ValueError("Pb must be in (0,1).")
    if Qv <= 0:
        raise ValueError("Qv must be > 0.")
    return Kb + (Qv * Pv_usd) / (1.0 - Pb)
