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
    """
    A paper-aligned trade candidate for the "vanilla + binary" portfolio.

    Conventions (paper):
      - decision time at 08:00 UTC (handled upstream when building datasets)
      - choose direction using K_B vs spot at decision time:
          if K_B < S_t -> long binary PUT + long vanilla CALL
          else         -> long binary CALL + long vanilla PUT
      - Pv_usd is the vanilla premium expressed in USD terms (even if the
        option is quoted in underlying units on Deribit, you convert using spot)
      - Qv is the number of vanilla contracts (Deribit: 1 contract = 1 unit underlying)
      - Qb is the number of binary shares/contracts (pay $1 if event happens)
    """

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

    # condition diagnostics
    kv_bound: float
    edge: float
    # edge > 0 means the vanilla strike is strictly inside the arbitrage region


def infer_direction(spot: float, Kb: float) -> tuple[BinaryType, VanillaType]:
    """
    Paper direction rule:
      - if Kb < spot:  binary PUT + vanilla CALL
      - else:          binary CALL + vanilla PUT

    We treat Kb == spot as the 'else' branch (CALL+PUT) to keep deterministic behavior.
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
) -> Optional[TradeCandidate]:
    """
    Paper-aligned candidate construction:

    1) infer direction from K_B vs spot -> required vanilla type
    2) compute strike bound on K_V (unified condition, Section 3.2)
    3) accept only if K_V is inside the valid region
    4) compute Q_B needed to cover the vanilla premium when vanilla finishes OTM
       (i.e., binary finishes ITM), per Section 3.2

    Returns:
      TradeCandidate if condition passes; otherwise None.
    """
    # Basic sanity checks
    if spot <= 0:
        return None
    if not (0.0 < Pb < 1.0):
        return None
    if Qv <= 0:
        return None
    if Pv_usd < 0:
        return None
    if fee_usd < 0:
        return None

    # 1) direction
    binary_type, required_vanilla_type = infer_direction(spot, Kb)
    if vanilla_type != required_vanilla_type:
        return None

    # 2) compute bound and "edge"
    if vanilla_type == "call":
        # Condition: K_V <= bound
        kv_bound = kv_bound_for_call_case(Kb=Kb, Qv=Qv, Pv_usd=Pv_usd, Pb=Pb)
        edge = kv_bound - Kv
        ok = edge > 0  # strict >0 gives a small buffer; change to >=0 if you want weak inequality
    else:
        # Condition: K_V >= bound
        kv_bound = kv_bound_for_put_case(Kb=Kb, Qv=Qv, Pv_usd=Pv_usd, Pb=Pb)
        edge = Kv - kv_bound
        ok = edge > 0

    if not ok:
        return None

    # 3) size the binary leg to cover vanilla premium + fees in the worst-case branch
    Qb = binary_qty_to_cover_vanilla(Qv=Qv, Pv_usd=Pv_usd, fee_usd=fee_usd, Pb=Pb)

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
