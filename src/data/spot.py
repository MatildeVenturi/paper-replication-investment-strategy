# scripts/build_spot_csv_deribit.py
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests


DERIBIT_URL = "https://www.deribit.com/api/v2"


def _project_root() -> Path:
    # adatta se la tua struttura è diversa
    return Path(__file__).resolve().parents[2]


def _ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        raise ValueError("Datetime must be timezone-aware")
    return int(dt.timestamp() * 1000)


def _deribit_get(method: str, params: dict) -> dict:
    r = requests.get(f"{DERIBIT_URL}/{method}", params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    if "error" in data and data["error"] is not None:
        raise RuntimeError(f"Deribit API error: {data['error']}")
    return data["result"]


def _get_trades(instrument_name: str, start_ms: int, end_ms: int, limit: int = 1000) -> list[dict]:
    """
    Pull trades for [start_ms, end_ms). Uses pagination via 'end_timestamp'
    by walking backwards until we cover the whole window.
    """
    all_trades: list[dict] = []
    # Deribit endpoint returns trades sorted by timestamp DESC by default.
    cursor_end = end_ms

    while True:
        res = _deribit_get(
            "public/get_last_trades_by_instrument",
            {
                "instrument_name": instrument_name,
                "start_timestamp": start_ms,
                "end_timestamp": cursor_end,
                "count": limit,
                "include_old": True,
                "sorting": "desc",
            },
        )
        trades = res.get("trades", []) if isinstance(res, dict) else res
        if not trades:
            break

        all_trades.extend(trades)

        # earliest trade in this batch (since desc)
        oldest_ts = trades[-1]["timestamp"]
        if oldest_ts <= start_ms:
            break

        # next query should end at the oldest_ts to avoid duplicates
        cursor_end = oldest_ts

        # safety break if API misbehaves
        if cursor_end <= start_ms:
            break

    # filter strictly to [start_ms, end_ms)
    return [t for t in all_trades if start_ms <= t["timestamp"] < end_ms]


def _vwap_from_trades(trades: list[dict]) -> float:
    """
    VWAP = sum(price * amount) / sum(amount)
    Deribit trades typically have:
      - price (float)
      - amount (float)  # base amount (BTC/ETH)
    """
    if not trades:
        raise ValueError("No trades to compute VWAP")
    num = 0.0
    den = 0.0
    for t in trades:
        p = float(t["price"])
        a = float(t["amount"])
        num += p * a
        den += a
    if den == 0:
        raise ValueError("Total traded amount is zero; cannot compute VWAP")
    return num / den


def build_spot_csv_deribit_vwap(
    underlying: str,
    days_back: int = 30,
    out_rel_path: str = "data/raw/spot.csv",
    window_minutes: int = 30,  # paper uses 07:45–08:15 => 30 minutes window around 08:00
) -> Path:
    """
    Builds spot.csv using Deribit spot pair and VWAP around 08:00 UTC.

    Paper method:
      - daily observation at 08:00 UTC
      - use minute-level data over 07:45–08:15 UTC and compute VWAP :contentReference[oaicite:1]{index=1}

    We implement VWAP using trades between:
      [08:00 - window/2, 08:00 + window/2)
    """
    root = _project_root()
    out_path = root / out_rel_path
    out_path.parent.mkdir(parents=True, exist_ok=True)

    u = underlying.upper().strip()
    if u not in {"BTC", "ETH"}:
        raise ValueError("underlying must be 'BTC' or 'ETH'")

    # Paper uses BTC/USDC on Deribit for spot reference :contentReference[oaicite:2]{index=2}
    instrument = f"{u}_USDC"

    end_day = datetime.now(timezone.utc).date()
    start_day = end_day - timedelta(days=days_back)

    half = timedelta(minutes=window_minutes / 2)

    rows: list[dict] = []
    for d in pd.date_range(start_day, end_day, freq="D"):
        center = datetime(d.year, d.month, d.day, 8, 0, 0, tzinfo=timezone.utc)
        start_dt = center - half
        end_dt = center + half

        trades = _get_trades(instrument, _ms(start_dt), _ms(end_dt))
        vwap = _vwap_from_trades(trades)

        rows.append(
            {
                "date": d.date().isoformat(),
                "underlying": u,
                "spot": round(float(vwap), 2),
            }
        )

    df = pd.DataFrame(rows)
    df.to_csv(out_path, index=False)
    print(f"Saved {len(df)} rows -> {out_path}")
    return out_path


if __name__ == "__main__":
    build_spot_csv_deribit_vwap(
        underlying="BTC",
        days_back=30,
        out_rel_path="data/raw/spot.csv",
    )
