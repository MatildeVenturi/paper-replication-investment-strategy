# src/arbitrage/payoffs.py

def payoff_long_call_binary_put(S_T: float, K_v: float, P_v: float, Q_v: float,
                                P_b: float, Q_b: float, E: int) -> float:
    """
    Net payoff in dollars at expiration for:
    long vanilla CALL + long binary PUT (direction used when KB < spot per paper logic).
    Paper formula (Section 5.1): V_CP$ = QV(max(ST-KV,0)-PV) + QB(E - PB) :contentReference[oaicite:10]{index=10}
    E in {0,1} is the binary resolution (1 if binary finishes ITM, else 0).
    """
    if E not in (0, 1):
        raise ValueError("E must be 0 or 1.")
    vanilla_leg = Q_v * (max(S_T - K_v, 0.0) - P_v)
    binary_leg = Q_b * (E - P_b)
    return vanilla_leg + binary_leg


def payoff_long_put_binary_call(S_T: float, K_v: float, P_v: float, Q_v: float,
                                P_b: float, Q_b: float, E: int) -> float:
    """
    Net payoff in dollars at expiration for:
    long vanilla PUT + long binary CALL.
    Paper formula (Section 5.1): V_PC$ = QV(max(KV-ST,0)-PV) + QB(E - PB) :contentReference[oaicite:11]{index=11}
    """
    if E not in (0, 1):
        raise ValueError("E must be 0 or 1.")
    vanilla_leg = Q_v * (max(K_v - S_T, 0.0) - P_v)
    binary_leg = Q_b * (E - P_b)
    return vanilla_leg + binary_leg
