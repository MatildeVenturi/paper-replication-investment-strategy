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
    # keys
    date: str
    underlying: str
    expiry: str

    # state at decision time
    spot: float

    # binary leg (prediction market)
    binary_type: BinaryType
    Kb: float
    Pb: float

    # vanilla leg (Deribit)
    vanilla_type: VanillaType
    Kv: float
    Pv_usd: float

    # sizing & costs
    Qv: float
    Qb: float
    fee_usd: float

    # diagnostics
    kv_bound: float
    edge: float


def infer_direction(spot: float, Kb: float) -> tuple[BinaryType, VanillaType]:
    """
    Paper rule (as you had it):
    - if Kb < spot: binary is "put", vanilla should be call
    - else: binary is "call", vanilla should be put
    """
    if spot <= 0:
        raise ValueError("spot must be > 0.")
    if Kb < spot:
        return "put", "call"
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
    # -------------------------
    # NEW "less strict" knobs
    # -------------------------
    edge_epsilon: float = 100.0,   # allow near-arb within this slack (strike units)
    pb_clip: float = 0.02,         # ignore Pb too close to 0 or 1 (unstable bounds)
) -> Optional[TradeCandidate]:
    """
    Returns a TradeCandidate if it passes (relaxed) paper conditions.

    Changes vs your original:
    - drops extreme Pb (near 0/1) which makes bounds explode and kills matching
    - relaxes strict inequality edge>0 into edge>-edge_epsilon (near-arbitrage)
    """

    # basic sanity
    if spot <= 0:
        return None
    if Qv <= 0:
        return None
    if Pv_usd < 0:
        return None
    if fee_usd < 0:
        return None

    # drop numerically extreme binary probs
    if not (pb_clip < Pb < 1.0 - pb_clip):
        return None

    # direction constraint
    binary_type, required_vanilla_type = infer_direction(spot, Kb)
    if vanilla_type != required_vanilla_type:
        return None

    # bound + edge
    if vanilla_type == "call":
        kv_bound = kv_bound_for_call_case(Kb=Kb, Qv=Qv, Pv_usd=Pv_usd, Pb=Pb)
        raw_edge = kv_bound - Kv  # >0 good
    else:
        kv_bound = kv_bound_for_put_case(Kb=Kb, Qv=Qv, Pv_usd=Pv_usd, Pb=Pb)
        raw_edge = Kv - kv_bound  # >0 good

    # Relaxed condition: allow small violations
    if raw_edge < -edge_epsilon:
        return None

    # size binary to cover worst-case branch (same as your original)
    Qb = binary_qty_to_cover_vanilla(Qv=Qv, Pv_usd=Pv_usd, fee_usd=fee_usd, Pb=Pb)

    # Report "relaxed edge" (positive means inside after slack)
    edge = raw_edge + edge_epsilon

    return TradeCandidate(
        date=str(date),
        underlying=str(underlying),
        expiry=str(expiry),
        spot=float(spot),
        binary_type=binary_type,
        Kb=float(Kb),
        Pb=float(Pb),
        vanilla_type=vanilla_type,
        Kv=float(Kv),
        Pv_usd=float(Pv_usd),
        Qv=float(Qv),
        Qb=float(Qb),
        fee_usd=float(fee_usd),
        kv_bound=float(kv_bound),
        edge=float(edge),
    )