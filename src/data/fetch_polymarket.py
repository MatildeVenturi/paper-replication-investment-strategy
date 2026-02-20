from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from typing import Any, Literal, Optional

import requests

# -----------------------------
# Endpoints
# -----------------------------
GAMMA_BASE = "https://gamma-api.polymarket.com"  # metadata (markets)
CLOB_BASE = "https://clob.polymarket.com"        # pricing endpoints

Currency = Literal["BTC", "ETH"]

# Grab dollar-like thresholds (e.g. "$100,000" or "100000")
_STRIKE_RE = re.compile(r"\$?(\d{1,3}(?:,\d{3})+|\d+)")


# -----------------------------
# Session + backoff helpers
# -----------------------------
_SESSION: Optional[requests.Session] = None


def _session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        s = requests.Session()
        s.headers.update(
            {
                "User-Agent": "polymarket-client/1.0 (+https://polymarket.com)",
                "Accept": "application/json",
            }
        )
        _SESSION = s
    return _SESSION


def _sleep_backoff(i: int, base: float = 1.5, cap: float = 20.0) -> float:
    return min(cap, base * (2**i))


def _get_json(
    url: str,
    params: dict[str, Any] | None = None,
    *,
    timeout: int = 20,
    retries: int = 5,
) -> Any:
    """
    Robust GET with exponential backoff.
    Retries: network errors, 429, 5xx, JSON decode errors.
    """
    last_err: Exception | None = None
    for i in range(retries):
        try:
            r = _session().get(url, params=params, timeout=timeout)

            # retry 429 + 5xx
            if r.status_code == 429 or (500 <= r.status_code <= 599):
                raise requests.HTTPError(f"HTTP {r.status_code}", response=r)

            r.raise_for_status()
            return r.json()

        except (requests.Timeout, requests.ConnectionError, requests.HTTPError, ValueError) as e:
            last_err = e
            sleep = _sleep_backoff(i)
            print(f"[WARN] GET failed ({i+1}/{retries}) {url}: {e} — retrying in {sleep:.1f}s")
            time.sleep(sleep)

    raise RuntimeError(f"Request failed after {retries} attempts: {last_err}")


def _get_json_allow_404(
    url: str,
    params: dict[str, Any] | None = None,
    *,
    timeout: int = 15,
    retries: int = 3,
) -> tuple[Optional[Any], Optional[int], Optional[str]]:
    """
    Like _get_json but returns (None, 404, ...) on 404 instead of raising.
    """
    last_err: Exception | None = None
    last_status: Optional[int] = None

    for i in range(retries):
        try:
            r = _session().get(url, params=params, timeout=timeout)
            last_status = r.status_code

            if r.status_code == 404:
                return None, 404, "Not Found"

            if r.status_code == 429 or (500 <= r.status_code <= 599):
                raise requests.HTTPError(f"HTTP {r.status_code}", response=r)

            r.raise_for_status()
            return r.json(), r.status_code, None

        except (requests.Timeout, requests.ConnectionError, requests.HTTPError, ValueError) as e:
            last_err = e
            sleep = _sleep_backoff(i)
            print(f"[WARN] GET failed ({i+1}/{retries}) {url}: {e} — retrying in {sleep:.1f}s")
            time.sleep(sleep)

    return None, last_status, str(last_err) if last_err else "unknown error"


# -----------------------------
# Parsing helpers
# -----------------------------
def _safe_json_loads(x: Any) -> Any:
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


def _parse_strike(question: str) -> Optional[float]:
    """
    Extract a numeric strike threshold from question.
    (Conservative: first numeric chunk; you can refine later if needed.)
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
    Converts Gamma endDate (ISO, often with Z) into UTC date YYYY-MM-DD.
    """
    try:
        dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).date().isoformat()
    except Exception:
        return None


def _utc_today_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _currency_in_question(currency: str, question: str) -> bool:
    ql = question.lower()
    if currency == "BTC":
        return ("bitcoin" in ql) or ("btc" in ql)
    if currency == "ETH":
        return ("ethereum" in ql) or ("eth" in ql)
    return False


def _pick_yes_token_id(market: dict[str, Any]) -> Optional[tuple[str, int]]:
    """
    Returns (yes_token_id, yes_index) by mapping outcomes <-> clobTokenIds.
    """
    outcomes = _safe_json_loads(market.get("outcomes"))
    token_ids = _safe_json_loads(market.get("clobTokenIds"))

    if not isinstance(outcomes, list) or not isinstance(token_ids, list):
        return None
    if len(outcomes) != len(token_ids) or not outcomes:
        return None

    # exact "Yes"
    if "Yes" in outcomes:
        idx = outcomes.index("Yes")
        return str(token_ids[idx]), idx

    # fallback case-insensitive
    for i, out in enumerate(outcomes):
        if isinstance(out, str) and out.strip().lower() == "yes":
            return str(token_ids[i]), i

    return None


def _price_from_gamma_outcome_prices(market: dict[str, Any], yes_index: int) -> Optional[float]:
    op = _safe_json_loads(market.get("outcomePrices"))
    if not isinstance(op, list) or len(op) <= yes_index:
        return None
    try:
        return float(op[yes_index])
    except Exception:
        return None


def _valid_prob(p: Optional[float]) -> bool:
    return p is not None and 0.0 < float(p) < 1.0


# -----------------------------
# CLOB pricing
# -----------------------------
def fetch_midpoint(token_id: str) -> Optional[float]:
    """
    CLOB midpoint. 404 => no orderbook => None.
    """
    j, status, err = _get_json_allow_404(
        f"{CLOB_BASE}/midpoint",
        {"token_id": token_id},
        timeout=10,
        retries=3,
    )
    if j is None:
        return None

    if isinstance(j, dict):
        for k in ("mid_price", "mid", "midpoint", "price"):
            v = j.get(k)
            if v is not None:
                try:
                    return float(v)
                except Exception:
                    return None
    return None


def fetch_last_trade_price(token_id: str) -> Optional[float]:
    """
    Last trade price fallback. 404 => None.
    """
    j, status, err = _get_json_allow_404(
        f"{CLOB_BASE}/last_trade_price",
        {"token_id": token_id},
        timeout=10,
        retries=3,
    )
    if j is None:
        return None

    if isinstance(j, dict):
        for k in ("last_trade_price", "price"):
            v = j.get(k)
            if v is not None:
                try:
                    return float(v)
                except Exception:
                    return None
    return None


# -----------------------------
# Main fetchers (with pagination)
# -----------------------------
def fetch_crypto_threshold_markets(
    *,
    currency: Currency,
    active: bool = True,
    closed: bool = False,
    page_limit: int = 200,      # page size for Gamma
    max_pages: int = 50,        # safety cap
    require_orderbook: bool = False,
    use_last_trade_fallback: bool = True,
    future_only: bool = True,
) -> list[dict]:
    """
    Fetch BTC/ETH threshold-style Polymarket markets and convert into "option-like" rows:
      (underlying, expiry, strike, price=YES probability)

    Key point: Gamma /markets is paginated via limit+offset.
    """
    cur = currency.upper()
    today = _utc_today_iso()

    rows: list[dict] = []
    offset = 0

    for _ in range(max_pages):
        markets = _get_json(
            f"{GAMMA_BASE}/markets",
            {
                "limit": page_limit,
                "offset": offset,
                "active": active,
                "closed": closed,   # IMPORTANT to avoid lots of stale stuff
            },
            timeout=20,
            retries=5,
        )

        if not isinstance(markets, list) or len(markets) == 0:
            break

        for m in markets:
            q = str(m.get("question", "")).strip()
            if not q or not _currency_in_question(cur, q):
                continue

            end = m.get("endDate") or m.get("endDateIso") or m.get("end_date")
            if not end:
                continue
            expiry = _parse_expiry_iso(str(end))
            if not expiry:
                continue

            if future_only and expiry < today:
                continue

            strike = _parse_strike(q)
            if strike is None:
                continue

            if require_orderbook and not bool(m.get("enableOrderBook", False)):
                continue

            picked = _pick_yes_token_id(m)
            if not picked:
                continue
            yes_token_id, yes_idx = picked

            price = fetch_midpoint(yes_token_id)
            if price is None and use_last_trade_fallback:
                price = fetch_last_trade_price(yes_token_id)
            if price is None:
                price = _price_from_gamma_outcome_prices(m, yes_idx)

            if not _valid_prob(price):
                continue

            rows.append(
                {
                    "underlying": cur,
                    "expiry": expiry,
                    "strike": float(strike),
                    "price": float(price),
                    "question": q,
                    "yes_token_id": yes_token_id,
                    "market_slug": m.get("slug"),
                }
            )

        offset += page_limit

    return rows


def fetch_crypto_threshold_markets_for_expiry(
    *,
    currency: Currency,
    expiry_iso: str,
    active: bool = True,
    closed: bool = False,
    page_limit: int = 200,
    max_pages: int = 50,
    require_orderbook: bool = False,
    use_last_trade_fallback: bool = True,
    future_only: bool = True,
) -> list[dict]:
    all_rows = fetch_crypto_threshold_markets(
        currency=currency,
        active=active,
        closed=closed,
        page_limit=page_limit,
        max_pages=max_pages,
        require_orderbook=require_orderbook,
        use_last_trade_fallback=use_last_trade_fallback,
        future_only=future_only,
    )
    return [r for r in all_rows if r["expiry"] == expiry_iso]