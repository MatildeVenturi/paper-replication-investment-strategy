# src/arbitrage/strategy.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional

from src.arbitrage.conditions import (
    binary_qty_to_cover_vanilla,
    kv_bound_for_call_case,
    kv_bound_for_put_case,
)

BinaryType = Literal["call", "put"]
VanillaType = Literal["call", "put"]


@dataclass(frozen=True)
class TradeCandidate:
    date: str
    underlying: str
    expiry: str

    spot: float

    binary_type: BinaryType
    Kb: float
    Pb: float

    vanilla_type: VanillaType
    Kv: float
    Pv_usd: float

    Qv: float
    Qb: float
    fee_usd: float

    kv_bound: float
    edge: float
    # edge = how far inside the constraint we are (positive = better margin)


def infer_direction(spot: float, Kb: float) -> tuple[BinaryType, VanillaType]:
    """
    Paper logic: direction depends on whether strike is above or below spot.
    If Kb < spot => binary PUT, pair with vanilla CALL.
    If Kb > spot => binary CALL, pair with vanilla PUT.
    """
    if Kb < spot:
        return "put", "call"
    elif Kb > spot:
        return "call", "put"
    else:
        # ATM: choose one convention (doesn't matter much for demo)
        return "call", "put"


def check_and_build_candidate(
    *,
    date: str,
    underlying: str,
    expiry: str,
    spot: float,
    Kb: float,
    Pb: float,
    Kv: float,
    vanilla_type: VanillaType,
    Pv_usd: float,
    Qv: float = 1.0,
    fee_usd: float = 0.0,
) -> Optional[TradeCandidate]:
    """
    Returns TradeCandidate if unified condition passes, else None.
    """
    if Pb <= 0 or Pb >= 1:
        return None
    if Pv_usd < 0 or spot <= 0:
        return None

    binary_type, vanilla_needed = infer_direction(spot, Kb)
    if vanilla_type != vanilla_needed:
        return None

    if vanilla_type == "call":
        kv_bound = kv_bound_for_call_case(Kb=Kb, Qv=Qv, Pv_usd=Pv_usd, Pb=Pb)
        ok = Kv <= kv_bound
        edge = kv_bound - Kv  # positive means inside constraint
    else:
        kv_bound = kv_bound_for_put_case(Kb=Kb, Qv=Qv, Pv_usd=Pv_usd, Pb=Pb)
        ok = Kv >= kv_bound
        edge = Kv - kv_bound

    if not ok:
        return None

    Qb = binary_qty_to_cover_vanilla(Qv=Qv, Pv_usd=Pv_usd, fee_usd=fee_usd, Pb=Pb)

    return TradeCandidate(
        date=date,
        underlying=underlying,
        expiry=expiry,
        spot=spot,
        binary_type=binary_type,
        Kb=Kb,
        Pb=Pb,
        vanilla_type=vanilla_type,
        Kv=Kv,
        Pv_usd=Pv_usd,
        Qv=Qv,
        Qb=Qb,
        fee_usd=fee_usd,
        kv_bound=kv_bound,
        edge=float(edge),
    )
