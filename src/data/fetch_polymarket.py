# src/data/fetch_polymarket.py
from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from typing import Literal, Optional

import requests

# Public endpoints (no wallet needed)
GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"

Currency = Literal["BTC", "ETH"]

# --- simple strike extractor for "above $70,000" style questions ---
_STRIKE_RE = re.compile(r"\$?(\d{1,3}(?:,\d{3})+|\d+)")


# ----------------------------
# Robust HTTP helpers
# ----------------------------
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


def fetch_midpoint(token_id: str) -> Optional[float]:
    """
    Try to fetch midpoint price for a token_id from the public CLOB endpoint.
    Returns None if unavailable.
    """
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


# ----------------------------
# Parsing utilities
# ----------------------------
def _parse_strike(question: str) -> Optional[float]:
    """
    Extract a numeric strike from a market question.
    Works for common forms like:
      "Will Bitcoin be above $70,000 on ...?"
    """
    m = _STRIKE_RE.search(question)
    if not m:
        return None
    s = m.group(1).replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


def _parse_expiry_iso(end_date: str) -> Optional[str]:
    """
    Gamma 'endDate' is usually ISO like '2026-02-27T08:00:00Z' (or similar).
    We convert it to UTC date 'YYYY-MM-DD'.
    """
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


# ----------------------------
# Main fetcher
# ----------------------------
def fetch_crypto_threshold_markets(
    *,
    currency: Currency,
    limit: int = 200,
    active: bool = True,
) -> list[dict]:
    """
    Fetch Polymarket markets and keep those that *look like* crypto threshold markets.

    Returns rows with:
      - underlying: 'BTC'/'ETH'
      - expiry: 'YYYY-MM-DD' (UTC date from market endDate)
      - strike: float
      - price: midpoint price for YES token (0..1) if available
      - question: original market question
      - yes_token_id: token id used for pricing (debug)

    Notes:
    - We do NOT force an expiry filter here. For backtesting you generally want to
      download everything and then intersect with Deribit expiries later.
    - This function is "best-effort": if midpoint isn't available, it falls back to
      outcomePrices (if present). If neither is available, it skips the market.
    """
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


def fetch_crypto_threshold_markets_for_expiry(
    *,
    currency: Currency,
    expiry_iso: str,
    limit: int = 400,
    active: bool = True,
) -> list[dict]:
    """
    Convenience wrapper: fetch and then filter to a specific expiry date.
    """
    all_rows = fetch_crypto_threshold_markets(currency=currency, limit=limit, active=active)
    return [r for r in all_rows if r["expiry"] == expiry_iso]
