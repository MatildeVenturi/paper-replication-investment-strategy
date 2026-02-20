from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Literal, Optional

import requests

DERIBIT_BASE = "https://www.deribit.com/api/v2"
Currency = Literal["BTC", "ETH"]

# cache instruments (big list)
_INSTRUMENTS_CACHE: dict[tuple[str, str, str], list[dict[str, Any]]] = {}

# one shared session (keep-alive helps a LOT)
_SESSION: Optional[requests.Session] = None


def _session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        s = requests.Session()
        s.headers.update({"Accept": "application/json", "User-Agent": "deribit-client/1.0"})
        _SESSION = s
    return _SESSION


def _sleep_backoff(i: int, base: float = 1.5, cap: float = 20.0) -> float:
    return min(cap, base * (2**i))


def _get(path: str, params: dict, *, timeout: int = 30, retries: int = 5) -> Any:
    """
    Robust GET with retries/backoff.
    Retries network errors, timeouts, 429, 5xx.
    """
    url = f"{DERIBIT_BASE}/{path}"
    last_err: Exception | None = None

    for i in range(retries):
        try:
            r = _session().get(url, params=params, timeout=timeout)

            # retry on rate limit + server errors
            if r.status_code == 429 or (500 <= r.status_code <= 599):
                raise requests.HTTPError(f"HTTP {r.status_code}", response=r)

            r.raise_for_status()
            j = r.json()

            if isinstance(j, dict) and j.get("error"):
                raise RuntimeError(f"Deribit error: {j['error']}")

            return j["result"] if isinstance(j, dict) and "result" in j else j

        except (requests.Timeout, requests.ConnectionError, requests.HTTPError, ValueError, RuntimeError) as e:
            last_err = e
            sleep = _sleep_backoff(i)
            print(f"[WARN] Deribit GET failed ({i+1}/{retries}) {path}: {e} — retrying in {sleep:.1f}s")
            time.sleep(sleep)

    raise RuntimeError(f"Deribit request failed after {retries} attempts ({path}): {last_err}")


def _get_instruments_cached(currency: Currency, *, kind: str = "option", expired: str = "false") -> list[dict[str, Any]]:
    key = (currency, kind, expired)
    if key in _INSTRUMENTS_CACHE:
        return _INSTRUMENTS_CACHE[key]

    instruments = _get("public/get_instruments", {"currency": currency, "kind": kind, "expired": expired})
    if not isinstance(instruments, list):
        raise RuntimeError(f"Unexpected Deribit instruments response type: {type(instruments)}")

    _INSTRUMENTS_CACHE[key] = instruments
    return instruments


def fetch_spot_index(currency: Currency) -> float:
    index_name = f"{currency.lower()}_usd"
    res = _get("public/get_index_price", {"index_name": index_name})
    return float(res["index_price"])


def fetch_available_option_expiries(currency: Currency) -> list[str]:
    instruments = _get_instruments_cached(currency, kind="option", expired="false")
    expiries: set[str] = set()

    for ins in instruments:
        exp_ts = ins.get("expiration_timestamp")
        if exp_ts:
            exp_dt = datetime.fromtimestamp(int(exp_ts) / 1000, tz=timezone.utc).date().isoformat()
            expiries.add(exp_dt)

    return sorted(expiries)


def pick_expiries_in_window(
    currency: Currency,
    *,
    start_expiry_iso: Optional[str] = None,
    window_days: int = 365,
) -> list[str]:
    expiries = fetch_available_option_expiries(currency)
    if not expiries:
        return []

    start_date = datetime.now(timezone.utc).date() if start_expiry_iso is None else date.fromisoformat(start_expiry_iso)
    end_date = start_date + timedelta(days=window_days)
    return [e for e in expiries if start_date <= date.fromisoformat(e) <= end_date]


# keep old name for compatibility
def pick_expiries_in_next_two_weeks(
    currency: Currency,
    *,
    start_expiry_iso: Optional[str] = None,
    window_days: int = 14,
) -> list[str]:
    return pick_expiries_in_window(currency, start_expiry_iso=start_expiry_iso, window_days=window_days)


def fetch_vanilla_snapshot(
    *,
    currency: Currency,
    expiry_iso: str,
    date_iso: str,
    max_strikes: int = 60,
) -> list[dict]:
    instruments = _get_instruments_cached(currency, kind="option", expired="false")

    chosen: list[dict[str, Any]] = []
    for ins in instruments:
        exp_ts = ins.get("expiration_timestamp")
        if not exp_ts:
            continue
        exp_dt = datetime.fromtimestamp(int(exp_ts) / 1000, tz=timezone.utc).date().isoformat()
        if exp_dt == expiry_iso and ins.get("strike") is not None:
            chosen.append(ins)

    if not chosen:
        return []

    spot = fetch_spot_index(currency)

    chosen.sort(key=lambda x: abs(float(x["strike"]) - spot))
    chosen = chosen[:max_strikes]

    rows: list[dict] = []
    for ins in chosen:
        name = ins["instrument_name"]
        strike = float(ins["strike"])
        opt_type = str(ins.get("option_type", "")).lower().strip()

        if opt_type not in ("call", "put"):
            continue

        # ticker can fail sometimes; don't kill the whole run
        try:
            t = _get("public/ticker", {"instrument_name": name}, timeout=25, retries=4)
        except Exception as e:
            print(f"[WARN] ticker failed for {name}: {e} (skipping)")
            continue

        bid = t.get("best_bid_price")
        ask = t.get("best_ask_price")
        mark = t.get("mark_price")

        price: Optional[float] = None
        if bid is not None and ask is not None and float(bid) > 0 and float(ask) > 0:
            price = (float(bid) + float(ask)) / 2.0
        elif mark is not None:
            price = float(mark)

        if price is None:
            continue

        rows.append(
            {
                "date": date_iso,
                "underlying": currency,
                "expiry": expiry_iso,
                "strike": strike,
                "type": opt_type,
                "price": float(price),
            }
        )

    return rows