
from __future__ import annotations


def binary_qty_to_cover_vanilla(Qv: float, Pv_usd: float, fee_usd: float, Pb: float) -> float:
  
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
  
    if not (0.0 < Pb < 1.0):
        raise ValueError("Pb must be in (0,1).")
    if Qv <= 0:
        raise ValueError("Qv must be > 0.")
    return Kb - (Qv * Pv_usd) / (1.0 - Pb)


def kv_bound_for_put_case(Kb: float, Qv: float, Pv_usd: float, Pb: float) -> float:
  
    if not (0.0 < Pb < 1.0):
        raise ValueError("Pb must be in (0,1).")
    if Qv <= 0:
        raise ValueError("Qv must be > 0.")
    return Kb + (Qv * Pv_usd) / (1.0 - Pb)
