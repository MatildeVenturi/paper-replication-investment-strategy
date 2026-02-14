# src/arbitrage/conditions.py
from __future__ import annotations

def binary_qty_to_cover_vanilla(Qv: float, Pv_usd: float, fee_usd: float, Pb: float) -> float:
    """
    QB = QV * (PV + F) / (1 - PB)
    where PV is vanilla premium in USD terms, PB in (0,1), F in USD. (Paper Section 3.2)
    """
    if Pb <= 0 or Pb >= 1:
        raise ValueError("Pb must be in (0,1).")
    if Qv <= 0:
        raise ValueError("Qv must be > 0.")
    return Qv * (Pv_usd + fee_usd) / (1.0 - Pb)


def kv_bound_for_call_case(Kb: float, Qv: float, Pv_usd: float, Pb: float) -> float:
    """
    Vanilla CALL + Binary PUT case.
    Unified condition rearranged:
      KV <= KB - (QV * PV) / (1 - PB)
    """
    if Pb <= 0 or Pb >= 1:
        raise ValueError("Pb must be in (0,1).")
    return Kb - (Qv * Pv_usd) / (1.0 - Pb)


def kv_bound_for_put_case(Kb: float, Qv: float, Pv_usd: float, Pb: float) -> float:
    """
    Vanilla PUT + Binary CALL case.
    Unified condition rearranged:
      KV >= KB + (QV * PV) / (1 - PB)
    """
    if Pb <= 0 or Pb >= 1:
        raise ValueError("Pb must be in (0,1).")
    return Kb + (Qv * Pv_usd) / (1.0 - Pb)
