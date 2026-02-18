# src/arbitrage/payoffs.py
from __future__ import annotations


def payoff_long_call_binary_put(
    S_T: float,
    K_v: float,
    P_v_usd: float,
    Q_v: float,
    P_b: float,
    Q_b: float,
    E: int,
) -> float:
    """
    Paper (Section 5.1): net payoff in USD at expiration for:
      - long vanilla CALL (priced in USD terms for consistency)
      - long binary PUT (Polymarket, settles in USD)

    V_CP$ = QV( max(S_T - K_V, 0) - P_V ) + Q_B( E - P_B )

    E in {0,1} is the realized binary payoff ($1 if correct, else $0).
    """
    if S_T <= 0:
        raise ValueError("S_T must be > 0.")
    if E not in (0, 1):
        raise ValueError("E must be 0 or 1.")
    if Q_v <= 0 or Q_b <= 0:
        raise ValueError("Quantities must be > 0.")

    vanilla_leg = Q_v * (max(S_T - K_v, 0.0) - P_v_usd)
    binary_leg = Q_b * (float(E) - P_b)
    return vanilla_leg + binary_leg


def payoff_long_put_binary_call(
    S_T: float,
    K_v: float,
    P_v_usd: float,
    Q_v: float,
    P_b: float,
    Q_b: float,
    E: int,
) -> float:
    """
    Paper (Section 5.1): net payoff in USD at expiration for:
      - long vanilla PUT
      - long binary CALL

    V_PC$ = QV( max(K_V - S_T, 0) - P_V ) + Q_B( E - P_B )
    """
    if S_T <= 0:
        raise ValueError("S_T must be > 0.")
    if E not in (0, 1):
        raise ValueError("E must be 0 or 1.")
    if Q_v <= 0 or Q_b <= 0:
        raise ValueError("Quantities must be > 0.")

    vanilla_leg = Q_v * (max(K_v - S_T, 0.0) - P_v_usd)
    binary_leg = Q_b * (float(E) - P_b)
    return vanilla_leg + binary_leg
