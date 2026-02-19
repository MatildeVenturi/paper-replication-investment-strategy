#transform binary option into "traditional" ones 
from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from typing import Literal, Optional

import requests

#mercati
GAMMA_BASE = "https://gamma-api.polymarket.com"
#prices (central limit order book)
CLOB_BASE = "https://clob.polymarket.com"

Currency = Literal["BTC", "ETH"]


_STRIKE_RE = re.compile(r"\$?(\d{1,3}(?:,\d{3})+|\d+)")



def _get_json(url: str, params: dict | None = None, *, timeout: int = 10, retries: int = 5) -> dict | list:
    """
    Robust GET with exponential backoff.
    Raises RuntimeError only after exhausting retries.
    """
    last_err: Exception | None = None
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            last_err = e
            sleep = min(20.0, 1.5 * (2 ** i))
            print(f"[WARN] Polymarket GET failed ({i+1}/{retries}) {url}: {e} — retrying in {sleep:.1f}s")
            time.sleep(sleep)
        except ValueError as e:
            # JSON decode error
            last_err = e
            sleep = min(20.0, 1.5 * (2 ** i))
            print(f"[WARN] Polymarket JSON decode failed ({i+1}/{retries}) {url}: {e} — retrying in {sleep:.1f}s")
            time.sleep(sleep)

    raise RuntimeError(f"Polymarket request failed after {retries} retries: {last_err}")


#binary price (prob)
def fetch_midpoint(token_id: str) -> Optional[float]:

    try:
        j = _get_json(f"{CLOB_BASE}/midpoint", {"token_id": token_id}, timeout=10, retries=3)
    except Exception as e:
        print(f"[WARN] CLOB midpoint failed for token_id={token_id}: {e}")
        return None

    if isinstance(j, dict):
        for k in ("mid", "midpoint", "price"):
            v = j.get(k)
            if v is not None:
                try:
                    return float(v)
                except Exception:
                    return None
    return None


#extract nuemric strike from market question
def _parse_strike(question: str) -> Optional[float]:

    m = _STRIKE_RE.search(question)
    if not m:
        return None
    s = m.group(1).replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


#iso date
def _parse_expiry_iso(end_date: str) -> Optional[str]:
    try:
        dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).date().isoformat()
    except Exception:
        return None


def _safe_json_loads(x) -> Optional[object]:
    if x is None:
        return None
    if isinstance(x, (list, dict)):
        return x
    if isinstance(x, str):
        try:
            return json.loads(x)
        except Exception:
            return None
    return None


def fetch_crypto_threshold_markets(
    *,
    currency: Currency,
    limit: int = 200,
    active: bool = True,
) -> list[dict]:
   
    cur = currency.upper()
    markets = _get_json(f"{GAMMA_BASE}/markets", {"limit": limit, "active": active}, timeout=15, retries=5)

    if not isinstance(markets, list):
        # Gamma normally returns a list
        raise RuntimeError(f"Unexpected Gamma response type: {type(markets)}")

    rows: list[dict] = []

    for m in markets:
        q = str(m.get("question", "")).strip()
        if not q:
            continue

        # currency filter
        ql = q.lower()
        if cur == "BTC" and ("bitcoin" not in ql and "btc" not in ql):
            continue
        if cur == "ETH" and ("ethereum" not in ql and "eth" not in ql):
            continue

        end = m.get("endDate") or m.get("endDateIso") or m.get("end_date")
        if not end:
            continue
        expiry = _parse_expiry_iso(str(end))
        if expiry is None:
            continue

        strike = _parse_strike(q)
        if strike is None:
            continue

        # Get YES token id
        ids = _safe_json_loads(m.get("clobTokenIds"))
        if not isinstance(ids, list) or len(ids) < 1:
            continue
        yes_token_id = str(ids[0])

        # Try midpoint first
        price = fetch_midpoint(yes_token_id)

        # Fallback: Gamma sometimes includes outcomePrices like ["0.43","0.57"]
        if price is None:
            op = _safe_json_loads(m.get("outcomePrices"))
            if isinstance(op, list) and len(op) >= 1:
                try:
                    price = float(op[0])
                except Exception:
                    price = None

        if price is None:
            continue

        # sanity clamp
        if price <= 0 or price >= 1:
            # keep but clamp slightly inside (optional). For now skip to avoid weirdness.
            continue

        rows.append(
            {
                "underlying": cur,
                "expiry": expiry,
                "strike": float(strike),
                "price": float(price),
                "question": q,
                "yes_token_id": yes_token_id,
            }
        )

    return rows

#look at vanilla expiry
def fetch_crypto_threshold_markets_for_expiry(
    *,
    currency: Currency,
    expiry_iso: str,
    limit: int = 400,
    active: bool = True,
) -> list[dict]:
    
    all_rows = fetch_crypto_threshold_markets(currency=currency, limit=limit, active=active)
    return [r for r in all_rows if r["expiry"] == expiry_iso]
