from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal

import requests

DERIBIT_BASE = "https://www.deribit.com/api/v2"

Currency = Literal["BTC", "ETH"]


def _get(path: str, params: dict) -> dict:
    r = requests.get(f"{DERIBIT_BASE}/{path}", params=params, timeout=30)
    r.raise_for_status()
    j = r.json()
    if j.get("error"):
        raise RuntimeError(f"Deribit error: {j['error']}")
    return j["result"]


def fetch_spot_index(currency: Currency) -> float:
    """
    Spot proxy (USD per coin) using Deribit index price.
    """
    index_name = f"{currency.lower()}_usd"
    res = _get("public/get_index_price", {"index_name": index_name})
    return float(res["index_price"])


def fetch_vanilla_snapshot(
    *,
    currency: Currency,
    expiry_iso: str,
    date_iso: str,
    max_strikes: int = 60,
) -> list[dict]:
    """
    Build vanilla rows for a single expiry:
      date, underlying, expiry, strike, type, price

    price is in underlying units (BTC/ETH), taken from Deribit ticker.
    We use:
      - mid of best bid/ask when available
      - else mark_price as fallback
    """
    instruments = _get(
    "public/get_instruments",
    {"currency": currency, "kind": "option", "expired": "false"},
)


    # filter exact expiry (Deribit provides expiration_timestamp in ms; also "expiration_timestamp" and "expiration_date" fields vary)
    # We'll match by date string contained in instrument_name as a simple robust approach:
    # instrument_name example: BTC-26JAN26-70000-C
    # We cannot directly match ISO date reliably without parsing, so we do a safer: match by expiration_timestamp converted to ISO date if present.
    chosen = []
    for ins in instruments:
        # prefer timestamp if available
        exp_ts = ins.get("expiration_timestamp")
        if exp_ts:
            exp_dt = datetime.fromtimestamp(int(exp_ts) / 1000, tz=timezone.utc).date().isoformat()
            if exp_dt == expiry_iso:
                chosen.append(ins)
        else:
            # fallback: if no timestamp, skip
            continue

    # reduce size (options chains can be huge)
    # keep near-the-money by sorting by |strike - spot| if strike exists
    spot = fetch_spot_index(currency)
    with_strike = [x for x in chosen if x.get("strike") is not None]
    with_strike.sort(key=lambda x: abs(float(x["strike"]) - spot))
    with_strike = with_strike[:max_strikes]

    rows: list[dict] = []
    for ins in with_strike:
        name = ins["instrument_name"]
        strike = float(ins["strike"])
        opt_type = str(ins.get("option_type", "")).lower().strip()  # "call"/"put"

        t = _get("public/ticker", {"instrument_name": name})
        bid = t.get("best_bid_price")
        ask = t.get("best_ask_price")
        mark = t.get("mark_price")

        price = None
        if bid is not None and ask is not None and float(bid) > 0 and float(ask) > 0:
            price = (float(bid) + float(ask)) / 2.0
        elif mark is not None:
            price = float(mark)

        if price is None:
            continue

        if opt_type not in ("call", "put"):
            continue

        rows.append(
            {
                "date": date_iso,
                "underlying": currency,
                "expiry": expiry_iso,
                "strike": strike,
                "type": opt_type,
                "price": float(price),  # underlying units (BTC/ETH)
            }
        )

    return rows

#aggiungo per trovare expiry 
def fetch_available_option_expiries(currency: Currency) -> list[str]:
    """
    Returns sorted unique expiry dates (YYYY-MM-DD) available on Deribit options chain.
    """
    instruments = _get(
        "public/get_instruments",
        {"currency": currency, "kind": "option", "expired": "false"},
    )
    expiries = set()
    for ins in instruments:
        exp_ts = ins.get("expiration_timestamp")
        if exp_ts:
            exp_dt = datetime.fromtimestamp(int(exp_ts) / 1000, tz=timezone.utc).date().isoformat()
            expiries.add(exp_dt)
    return sorted(expiries)


def pick_next_expiry(currency: Currency, min_expiry_iso: str) -> str:
    """
    Pick the earliest expiry >= min_expiry_iso that exists on Deribit.
    """
    expiries = fetch_available_option_expiries(currency)
    for e in expiries:
        if e >= min_expiry_iso:
            return e
    # fallback: last available
    if not expiries:
        raise RuntimeError("No option expiries returned by Deribit.")
    return expiries[-1]
