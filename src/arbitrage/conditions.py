

def binary_qty_to_cover_vanilla(Qv: float, Pv: float, fee: float, Pb: float) -> float:
    """
    Quantity of binary contracts needed to cover the vanilla premium.
    From paper: QB = QV (PV + F) / (1 - PB)  (Section 3.2) :contentReference[oaicite:5]{index=5}
    """
    if Pb >= 1:
        raise ValueError("Pb must be < 1 (binary option price in (0,1)).")
    return Qv * (Pv + fee) / (1 - Pb)


def vanilla_strike_bound_call(Kb: float, Qv: float, Pv: float, Pb: float) -> float:
    """
    For direction: vanilla CALL + binary PUT.
    Unified condition rearranged gives upper bound for KV:
    KV <= KB - (QV*PV)/(1 - PB)   (Section 3.2) :contentReference[oaicite:6]{index=6}
    """
    if Pb >= 1:
        raise ValueError("Pb must be < 1.")
    return Kb - (Qv * Pv) / (1 - Pb)


def vanilla_strike_bound_put(Kb: float, Qv: float, Pv: float, Pb: float) -> float:
    """
    For direction: vanilla PUT + binary CALL.
    Unified condition rearranged gives lower bound for KV:
    KV >= KB + (QV*PV)/(1 - PB)   (Section 3.2) :contentReference[oaicite:7]{index=7}
    """
    if Pb >= 1:
        raise ValueError("Pb must be < 1.")
    return Kb + (Qv * Pv) / (1 - Pb)
