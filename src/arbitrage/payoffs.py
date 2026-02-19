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
  
    if S_T <= 0:
        raise ValueError("S_T must be > 0.")
    if E not in (0, 1):
        raise ValueError("E must be 0 or 1.")
    if Q_v <= 0 or Q_b <= 0:
        raise ValueError("Quantities must be > 0.")

    vanilla_leg = Q_v * (max(K_v - S_T, 0.0) - P_v_usd)
    binary_leg = Q_b * (float(E) - P_b)
    return vanilla_leg + binary_leg
